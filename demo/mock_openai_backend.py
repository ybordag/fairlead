#!/usr/bin/env python3
"""Tiny OpenAI-compatible mock backend for the Bluewater demo."""

from __future__ import annotations

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


class BackendState:
    def __init__(self, node: str) -> None:
        self.node = node
        self.mode = "healthy"
        self.chat_requests = 0


def response_body(state: BackendState) -> dict[str, Any]:
    return {
        "id": f"chatcmpl-demo-{state.node}-{state.chat_requests}",
        "object": "chat.completion",
        "model": f"mock-{state.node}",
        "choices": [
            {
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": f"{state.node} handled request",
                },
                "finish_reason": "stop",
            }
        ],
        "fairlead_demo": {"source": state.node, "mode": state.mode},
    }


class Handler(BaseHTTPRequestHandler):
    server: "MockServer"

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
        if self.path == "/control/state":
            self._send_json(
                200,
                {
                    "node": self.server.state.node,
                    "mode": self.server.state.mode,
                    "chat_requests": self.server.state.chat_requests,
                },
            )
            return

        if self.path == "/v1/models":
            self._send_json(
                200,
                {
                    "object": "list",
                    "data": [
                        {
                            "id": f"mock-{self.server.state.node}",
                            "object": "model",
                            "owned_by": "fairlead-demo",
                        }
                    ],
                },
            )
            return

        self._send_json(404, {"error": "not found"})

    def do_POST(self) -> None:
        if self.path == "/control/mode":
            body = self._read_json()
            mode = body.get("mode")
            if mode not in {"healthy", "fail", "fail_once"}:
                self._send_json(400, {"error": "mode must be healthy, fail, or fail_once"})
                return
            self.server.state.mode = mode
            self._send_json(200, {"node": self.server.state.node, "mode": mode})
            return

        if self.path == "/v1/chat/completions":
            self.server.state.chat_requests += 1
            mode = self.server.state.mode
            if mode == "fail_once":
                self.server.state.mode = "healthy"
                self._send_json(500, {"error": f"{self.server.state.node} fail_once"})
                return
            if mode == "fail":
                self._send_json(500, {"error": f"{self.server.state.node} failing"})
                return
            self._send_json(200, response_body(self.server.state))
            return

        if self.path == "/v1/embeddings":
            self._send_json(
                200,
                {
                    "object": "list",
                    "data": [{"object": "embedding", "index": 0, "embedding": [0.1, 0.2]}],
                    "model": f"mock-{self.server.state.node}",
                },
            )
            return

        self._send_json(404, {"error": "not found"})


class MockServer(ThreadingHTTPServer):
    def __init__(self, addr: tuple[str, int], state: BackendState) -> None:
        super().__init__(addr, Handler)
        self.state = state


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", required=True)
    parser.add_argument("--port", required=True, type=int)
    args = parser.parse_args()

    server = MockServer(("127.0.0.1", args.port), BackendState(args.node))
    print(f"{args.node} mock backend listening on http://127.0.0.1:{args.port}/v1", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
