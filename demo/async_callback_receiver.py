#!/usr/bin/env python3
"""Tiny callback receiver for the async jobs demo."""

from __future__ import annotations

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


class CallbackState:
    def __init__(self, payload_file: Path) -> None:
        self.payload_file = payload_file
        self.count = 0
        self.last_payload: dict[str, Any] | None = None

    def record(self, payload: dict[str, Any]) -> None:
        self.count += 1
        self.last_payload = payload
        self.payload_file.parent.mkdir(parents=True, exist_ok=True)
        self.payload_file.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


class Handler(BaseHTTPRequestHandler):
    server: "CallbackServer"

    def log_message(self, fmt: str, *args: object) -> None:
        return

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("content-length", "0"))
        if length == 0:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))

    def _send_json(self, status: int, body: dict[str, Any]) -> None:
        payload = json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self) -> None:
        if self.path == "/state":
            self._send_json(
                200,
                {
                    "count": self.server.state.count,
                    "last_payload": self.server.state.last_payload,
                },
            )
            return

        self._send_json(404, {"error": "not found"})

    def do_POST(self) -> None:
        if self.path == "/callback":
            payload = self._read_json()
            self.server.state.record(payload)
            self._send_json(200, {"ok": True, "count": self.server.state.count})
            return

        self._send_json(404, {"error": "not found"})


class CallbackServer(ThreadingHTTPServer):
    def __init__(self, addr: tuple[str, int], state: CallbackState) -> None:
        super().__init__(addr, Handler)
        self.state = state


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--payload-file", required=True, type=Path)
    args = parser.parse_args()

    server = CallbackServer(("127.0.0.1", args.port), CallbackState(args.payload_file))
    print(
        f"callback receiver listening on http://127.0.0.1:{args.port}/callback",
        flush=True,
    )
    server.serve_forever()


if __name__ == "__main__":
    main()
