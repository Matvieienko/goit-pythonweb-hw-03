"""HTTP and UDP socket servers for the message application."""

from __future__ import annotations

import json
import logging
import mimetypes
import os
import socket
import threading
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any
from urllib.parse import parse_qs, urlsplit

from jinja2 import Environment, FileSystemLoader, select_autoescape

BASE_DIR = Path(__file__).resolve().parent
STORAGE_PATH = Path(os.getenv("STORAGE_PATH", BASE_DIR / "storage" / "data.json"))
HTTP_HOST = os.getenv("HTTP_HOST", "0.0.0.0")
HTTP_PORT = int(os.getenv("HTTP_PORT", "3000"))
SOCKET_HOST = os.getenv("SOCKET_HOST", "0.0.0.0")
SOCKET_PORT = int(os.getenv("SOCKET_PORT", "5000"))
SOCKET_TARGET_HOST = os.getenv("SOCKET_TARGET_HOST", "127.0.0.1")
MAX_FORM_SIZE = 64 * 1024

LOGGER = logging.getLogger(__name__)


class MessageValidationError(ValueError):
    """Raised when submitted form data is invalid."""


class MessageStorage:
    """Thread-safe JSON storage for received messages."""

    def __init__(self, path: Path = STORAGE_PATH) -> None:
        self.path = path
        self._lock = threading.Lock()
        self._ensure_file()

    def _ensure_file(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self.path.write_text("{}\n", encoding="utf-8")

    @staticmethod
    def decode_form(raw_data: bytes) -> dict[str, str]:
        """Convert an URL-encoded byte string into a message dictionary."""
        try:
            decoded_data = raw_data.decode("utf-8")
        except UnicodeDecodeError as error:
            raise MessageValidationError("Form data must use UTF-8 encoding") from error

        parsed_data = parse_qs(decoded_data, keep_blank_values=True)
        username = parsed_data.get("username", [""])[0].strip()
        message = parsed_data.get("message", [""])[0].strip()

        if not username or not message:
            raise MessageValidationError("Username and message are required")

        return {"username": username, "message": message}

    def _read_unlocked(self) -> dict[str, dict[str, str]]:
        try:
            content = self.path.read_text(encoding="utf-8").strip()
            data: Any = json.loads(content or "{}")
        except (OSError, json.JSONDecodeError) as error:
            raise RuntimeError(f"Could not read {self.path}") from error

        if not isinstance(data, dict):
            raise RuntimeError(f"Expected a JSON object in {self.path}")
        return data

    def read_all(self) -> dict[str, dict[str, str]]:
        """Return all saved messages."""
        with self._lock:
            return self._read_unlocked()

    def save_bytes(self, raw_data: bytes) -> None:
        """Decode and append one timestamped message to the JSON file."""
        message = self.decode_form(raw_data)

        with self._lock:
            messages = self._read_unlocked()
            messages[str(datetime.now())] = message

            with NamedTemporaryFile(
                "w",
                encoding="utf-8",
                dir=self.path.parent,
                prefix="data-",
                suffix=".tmp",
                delete=False,
            ) as temporary_file:
                json.dump(
                    messages,
                    temporary_file,
                    ensure_ascii=False,
                    indent=2,
                )
                temporary_file.write("\n")
                temporary_path = Path(temporary_file.name)

            os.replace(temporary_path, self.path)


class SocketMessageServer(threading.Thread):
    """Receive raw form bodies through UDP and persist them."""

    def __init__(
        self,
        storage: MessageStorage,
        host: str = SOCKET_HOST,
        port: int = SOCKET_PORT,
    ) -> None:
        super().__init__(name="message-socket", daemon=True)
        self.storage = storage
        self.host = host
        self.port = port
        self.ready = threading.Event()
        self.stopped = threading.Event()
        self.bound_port: int | None = None

    def run(self) -> None:
        """Listen for UDP packets until the server is stopped."""
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as server_socket:
            server_socket.bind((self.host, self.port))
            server_socket.settimeout(0.5)
            self.bound_port = server_socket.getsockname()[1]
            self.ready.set()
            LOGGER.info(
                "Socket server started at udp://%s:%s",
                self.host,
                self.bound_port,
            )

            while not self.stopped.is_set():
                try:
                    raw_data, address = server_socket.recvfrom(MAX_FORM_SIZE)
                except socket.timeout:
                    continue
                except OSError:
                    break

                try:
                    self.storage.save_bytes(raw_data)
                    response = {"status": "ok"}
                except MessageValidationError as error:
                    response = {"status": "error", "message": str(error)}
                except Exception:
                    LOGGER.exception("Could not save a message")
                    response = {
                        "status": "error",
                        "message": "Could not save message",
                    }

                server_socket.sendto(
                    json.dumps(response).encode("utf-8"),
                    address,
                )

    def stop(self) -> None:
        """Request a graceful socket-server shutdown."""
        self.stopped.set()


class WebRequestHandler(BaseHTTPRequestHandler):
    """Handle pages, static resources, form posts, and errors."""

    storage = MessageStorage()
    socket_host = SOCKET_TARGET_HOST
    socket_port = SOCKET_PORT
    jinja = Environment(
        loader=FileSystemLoader(BASE_DIR / "templates"),
        autoescape=select_autoescape(("html", "xml")),
    )

    pages = {
        "/": BASE_DIR / "index.html",
        "/index.html": BASE_DIR / "index.html",
        "/message": BASE_DIR / "message.html",
        "/message.html": BASE_DIR / "message.html",
    }
    static_files = {
        "/style.css": BASE_DIR / "style.css",
        "/logo.png": BASE_DIR / "logo.png",
    }

    def _send_bytes(
        self,
        content: bytes,
        status: HTTPStatus = HTTPStatus.OK,
        content_type: str = "text/html; charset=utf-8",
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(content)

    def _send_file(
        self,
        path: Path,
        status: HTTPStatus = HTTPStatus.OK,
    ) -> None:
        try:
            content = path.read_bytes()
        except OSError:
            LOGGER.exception("Could not read file: %s", path)
            self._send_server_error()
            return

        content_type = mimetypes.guess_type(path.name)[0]
        content_type = content_type or "application/octet-stream"
        if content_type.startswith("text/"):
            content_type += "; charset=utf-8"
        self._send_bytes(content, status, content_type)

    def _send_not_found(self) -> None:
        self._send_file(BASE_DIR / "error.html", HTTPStatus.NOT_FOUND)

    def _send_server_error(self) -> None:
        self._send_bytes(
            b"Internal Server Error",
            HTTPStatus.INTERNAL_SERVER_ERROR,
            "text/plain; charset=utf-8",
        )

    def _render_messages(self) -> None:
        try:
            messages = self.storage.read_all()
            template = self.jinja.get_template("read.html")
            content = template.render(messages=messages).encode("utf-8")
        except Exception:
            LOGGER.exception("Could not render messages")
            self._send_server_error()
            return
        self._send_bytes(content)

    def do_GET(self) -> None:  # noqa: N802
        """Serve application routes."""
        path = urlsplit(self.path).path
        if path in self.pages:
            self._send_file(self.pages[path])
        elif path == "/read":
            self._render_messages()
        elif path in self.static_files:
            self._send_file(self.static_files[path])
        else:
            self._send_not_found()

    def do_HEAD(self) -> None:  # noqa: N802
        """Return the same headers as GET without a response body."""
        self.do_GET()

    def do_POST(self) -> None:  # noqa: N802
        """Forward raw form bytes to the UDP socket server."""
        if urlsplit(self.path).path != "/message":
            self._send_not_found()
            return

        try:
            content_length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            self._send_bytes(b"Bad Request", HTTPStatus.BAD_REQUEST)
            return

        if content_length <= 0 or content_length > MAX_FORM_SIZE:
            self._send_bytes(b"Bad Request", HTTPStatus.BAD_REQUEST)
            return

        raw_data = self.rfile.read(content_length)
        try:
            response = self._forward_to_socket(raw_data)
        except (OSError, TimeoutError, json.JSONDecodeError):
            LOGGER.exception("Socket server is unavailable")
            self._send_bytes(
                b"Service Unavailable",
                HTTPStatus.SERVICE_UNAVAILABLE,
            )
            return

        if response.get("status") != "ok":
            self._send_bytes(b"Bad Request", HTTPStatus.BAD_REQUEST)
            return

        self.send_response(HTTPStatus.SEE_OTHER)
        self.send_header("Location", "/read")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _forward_to_socket(self, raw_data: bytes) -> dict[str, Any]:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as client_socket:
            client_socket.settimeout(2.0)
            client_socket.sendto(
                raw_data,
                (self.socket_host, self.socket_port),
            )
            response, _ = client_socket.recvfrom(4096)
        return json.loads(response.decode("utf-8"))

    def log_message(self, format_string: str, *args: object) -> None:
        """Write HTTP requests through the logging module."""
        LOGGER.info("%s - %s", self.client_address[0], format_string % args)


def run() -> None:
    """Start the UDP socket server and HTTP server."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    storage = MessageStorage(STORAGE_PATH)
    socket_server = SocketMessageServer(storage)
    socket_server.start()
    if not socket_server.ready.wait(timeout=3):
        raise RuntimeError("Socket server failed to start")

    WebRequestHandler.storage = storage
    WebRequestHandler.socket_port = socket_server.bound_port or SOCKET_PORT
    http_server = ThreadingHTTPServer((HTTP_HOST, HTTP_PORT), WebRequestHandler)
    LOGGER.info("Web application started at http://localhost:%s", HTTP_PORT)

    try:
        http_server.serve_forever()
    except KeyboardInterrupt:
        LOGGER.info("Stopping servers")
    finally:
        http_server.server_close()
        socket_server.stop()
        socket_server.join(timeout=2)


if __name__ == "__main__":
    run()
