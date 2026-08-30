from __future__ import annotations

import json
import threading
import unittest
from http import HTTPStatus
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import urlencode

from main import MessageStorage, SocketMessageServer, WebRequestHandler


class ApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = TemporaryDirectory()
        self.data_path = Path(self.temporary_directory.name) / "storage" / "data.json"
        self.storage = MessageStorage(self.data_path)

        self.socket_server = SocketMessageServer(
            self.storage,
            host="127.0.0.1",
            port=0,
        )
        self.socket_server.start()
        self.assertTrue(self.socket_server.ready.wait(2))

        WebRequestHandler.storage = self.storage
        WebRequestHandler.socket_host = "127.0.0.1"
        WebRequestHandler.socket_port = self.socket_server.bound_port or 0
        self.http_server = ThreadingHTTPServer(
            ("127.0.0.1", 0),
            WebRequestHandler,
        )
        self.http_thread = threading.Thread(
            target=self.http_server.serve_forever,
            daemon=True,
        )
        self.http_thread.start()
        self.http_port = self.http_server.server_address[1]

    def tearDown(self) -> None:
        self.http_server.shutdown()
        self.http_server.server_close()
        self.http_thread.join(2)
        self.socket_server.stop()
        self.socket_server.join(2)
        self.temporary_directory.cleanup()

    def request(
        self,
        method: str,
        path: str,
        body: bytes | None = None,
    ) -> tuple[int, dict[str, str], bytes]:
        connection = HTTPConnection("127.0.0.1", self.http_port, timeout=3)
        headers = {"Content-Type": "application/x-www-form-urlencoded"} if body else {}
        connection.request(method, path, body=body, headers=headers)
        response = connection.getresponse()
        content = response.read()
        response_headers = dict(response.getheaders())
        connection.close()
        return response.status, response_headers, content

    def test_pages_and_static_files(self) -> None:
        for path in ("/", "/index.html", "/message.html", "/read"):
            status, _, content = self.request("GET", path)
            self.assertEqual(status, HTTPStatus.OK)
            self.assertTrue(content)

        status, headers, _ = self.request("GET", "/style.css")
        self.assertEqual(status, HTTPStatus.OK)
        self.assertIn("text/css", headers["Content-Type"])

        status, headers, _ = self.request("GET", "/logo.png")
        self.assertEqual(status, HTTPStatus.OK)
        self.assertEqual(headers["Content-Type"], "image/png")

    def test_unknown_route_returns_custom_404(self) -> None:
        status, _, content = self.request("GET", "/unknown")
        self.assertEqual(status, HTTPStatus.NOT_FOUND)
        self.assertIn(b"404", content)

    def test_form_is_saved_and_rendered(self) -> None:
        body = urlencode({"username": "Alex", "message": "First message"}).encode()
        status, headers, _ = self.request("POST", "/message", body)
        self.assertEqual(status, HTTPStatus.SEE_OTHER)
        self.assertEqual(headers["Location"], "/read")

        data = json.loads(self.data_path.read_text(encoding="utf-8"))
        self.assertEqual(len(data), 1)
        self.assertEqual(
            next(iter(data.values())),
            {"username": "Alex", "message": "First message"},
        )

        status, _, content = self.request("GET", "/read")
        self.assertEqual(status, HTTPStatus.OK)
        self.assertIn("Alex", content.decode("utf-8"))

    def test_jinja_escapes_html(self) -> None:
        body = urlencode(
            {"username": "<script>alert(1)</script>", "message": "Safe"}
        ).encode()
        status, _, _ = self.request("POST", "/message", body)
        self.assertEqual(status, HTTPStatus.SEE_OTHER)

        _, _, content = self.request("GET", "/read")
        rendered = content.decode("utf-8")
        self.assertNotIn("<script>alert(1)</script>", rendered)
        self.assertIn("&lt;script&gt;", rendered)


if __name__ == "__main__":
    unittest.main()
