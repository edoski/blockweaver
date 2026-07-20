from __future__ import annotations

import json
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest


def block_hash(number: int) -> str:
    return f"0x{number + 1:064x}"


class ChainServer:
    def __init__(self, *, chain_id: int = 1, finalized: int = 30) -> None:
        self.chain_id = chain_id
        self.finalized = finalized
        self.requests: list[list[dict[str, Any]]] = []
        self.http_failures = 0
        self.omit_once: set[int] = set()
        self.null_once: set[int] = set()
        self.changes: dict[int, dict[str, Any]] = {}
        self.reject_batches_larger_than: int | None = None
        self.wrong_id_once = False
        state = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                length = int(self.headers["content-length"])
                payload = json.loads(self.rfile.read(length))
                calls = payload if isinstance(payload, list) else [payload]
                state.requests.append(calls)
                if state.http_failures:
                    state.http_failures -= 1
                    self.send_response(429)
                    self.send_header("Retry-After", "0")
                    self.end_headers()
                    return
                replies = []
                for call in calls:
                    params = call.get("params")
                    selector = params[0] if isinstance(params, list) and params else None
                    number = state.finalized if selector in {"finalized", "latest"} else int(selector, 16) if isinstance(selector, str) else -1
                    if number in state.omit_once:
                        state.omit_once.remove(number)
                        continue
                    if self.server.state.reject_batches_larger_than is not None and len(calls) > self.server.state.reject_batches_larger_than:  # type: ignore[attr-defined]
                        replies.append(
                            {
                                "jsonrpc": "2.0",
                                "id": call["id"],
                                "error": {"code": -32000, "message": "busy"},
                            }
                        )
                        continue
                    result: Any
                    if call.get("method") == "eth_chainId":
                        result = hex(state.chain_id)
                    elif number in state.null_once:
                        state.null_once.remove(number)
                        result = None
                    else:
                        result = state.block(number)
                    item_id = call["id"]
                    if state.wrong_id_once:
                        state.wrong_id_once = False
                        item_id += 10_000
                    replies.append({"jsonrpc": "2.0", "id": item_id, "result": result})
                body = json.dumps(list(reversed(replies))).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format: str, *args: object) -> None:
                del format, args

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._server.state = self  # type: ignore[attr-defined]
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self._server.server_port}"

    def block(self, number: int) -> dict[str, Any]:
        return {
            "number": hex(number),
            "hash": block_hash(number),
            "parentHash": block_hash(number - 1),
            "timestamp": hex(1_700_000_000 + number),
            "baseFeePerGas": hex(1_000_000_000 + number),
            "gasUsed": hex(15_000_000 + number),
            "gasLimit": hex(30_000_000),
            "transactions": [block_hash(number * 10 + offset) for offset in range(number % 3)],
            **self.changes.get(number, {}),
        }

    def __enter__(self) -> ChainServer:
        self._thread.start()
        return self

    def __exit__(self, *_args: object) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join()


@pytest.fixture
def chains() -> Iterator[tuple[ChainServer, ChainServer]]:
    with ChainServer() as primary, ChainServer() as verifier:
        yield primary, verifier
