from __future__ import annotations

import json
import threading
from collections.abc import Callable, Iterator
from contextlib import suppress
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest


def block_hash(number: int) -> str:
    return f"0x{number + 1:064x}"


class ChainServer:
    def __init__(self, *, chain_id: int = 1, finalized: int = 30, timestamp_base: int = 1_700_000_000) -> None:
        self.chain_id = chain_id
        self.finalized = finalized
        self.timestamp_base = timestamp_base
        self.requests: list[list[dict[str, Any]]] = []
        self.request_counts: dict[int, int] = {}
        self.http_failures = 0
        self.omit_counts: dict[int, int] = {}
        self.changes: dict[int, dict[str, Any]] = {}
        self.fee_history_changes: dict[str, Any] = {}
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
                    method, params = call.get("method"), call.get("params")
                    if method == "eth_chainId":
                        result: Any = hex(state.chain_id)
                    elif method == "eth_feeHistory":
                        count, newest = int(params[0], 16), int(params[1], 16)
                        oldest = newest - count + 1
                        percentiles = params[2]
                        result = {
                            "oldestBlock": hex(oldest),
                            "reward": [[hex(number * percentile) for percentile in percentiles] for number in range(oldest, newest + 1)],
                            **state.fee_history_changes,
                        }
                    else:
                        selector = params[0]
                        number = state.finalized if selector in {"finalized", "safe"} else int(selector, 16)
                        state.request_counts[number] = state.request_counts.get(number, 0) + 1
                        if state.omit_counts.get(number, 0):
                            state.omit_counts[number] -= 1
                            continue
                        result = state.block(number)
                    item_id = call["id"] + 10_000 if state.wrong_id_once else call["id"]
                    state.wrong_id_once = False
                    replies.append({"jsonrpc": "2.0", "id": item_id, "result": result})
                body = json.dumps(list(reversed(replies))).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                with suppress(BrokenPipeError, ConnectionResetError):
                    self.wfile.write(body)

            def log_message(self, format: str, *args: object) -> None:
                del format, args

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self._server.server_port}"

    def block(self, number: int) -> dict[str, Any]:
        return {
            "number": hex(number),
            "hash": block_hash(number),
            "parentHash": block_hash(number - 1),
            "timestamp": hex(self.timestamp_base + number),
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


@pytest.fixture
def make_config() -> Callable[..., Path]:
    def write(
        path: Path,
        primary: ChainServer,
        verifier: ChainServer,
        *,
        output_root: Path,
        features: tuple[str, ...] = ("timestamp", "block_hash"),
        output_format: str = "parquet",
    ) -> Path:
        quoted_features = ", ".join(json.dumps(feature) for feature in features)
        path.write_text(
            f"""[defaults]
chain = "test"
source = "rpc"
provider = "primary"
verifier = "verifier"
output_root = {json.dumps(str(output_root))}
format = {json.dumps(output_format)}
features = [{quoted_features}]

[chains.test]
chain_id = 1
finality_tag = "finalized"

[providers.primary]
url = {json.dumps(primary.url)}
batch_size = 3
concurrency = 2
timeout = 2

[providers.verifier]
url = {json.dumps(verifier.url)}
batch_size = 3
concurrency = 2
timeout = 2
""",
            encoding="utf-8",
        )
        return path

    return write
