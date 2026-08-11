from __future__ import annotations

import json
import threading
from collections.abc import Callable, Iterator
from contextlib import suppress
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, ClassVar

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
        self.retry_after = "0"
        self.omit_counts: dict[int, int] = {}
        self.changes: dict[int, dict[str, Any]] = {}
        self.tag_changes: dict[str, Any] = {}
        self.fee_history_changes: dict[str, Any] = {}
        self.wrong_id_once = False
        self.rpc_errors: list[int] = []
        self.item_errors: dict[int, list[int]] = {}
        self.limit_batches = False
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
                    self.send_header("Retry-After", state.retry_after)
                    self.end_headers()
                    return
                replies = []
                for call in calls:
                    method, params = call.get("method"), call.get("params")
                    error_code = state._error_code(calls, method, params)
                    if error_code is not None:
                        replies.append(
                            {
                                "jsonrpc": "2.0",
                                "id": call["id"],
                                "error": {"code": error_code, "message": "provider detail must stay private"},
                            }
                        )
                        continue
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
                        if selector in {"finalized", "safe"}:
                            result.update(state.tag_changes)
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

    def _error_code(self, calls: list[dict[str, Any]], method: str, params: list[Any]) -> int | None:
        if self.rpc_errors:
            return self.rpc_errors.pop(0)
        if self.limit_batches and len(calls) > 1:
            return -32005
        if method == "eth_getBlockByNumber" and params[0] not in {"finalized", "safe"}:
            errors = self.item_errors.get(int(params[0], 16), [])
            if errors:
                return errors.pop(0)
        return None

    def __enter__(self) -> ChainServer:
        self._thread.start()
        return self

    def __exit__(self, *_args: object) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join()


class FakeBigQuery:
    TABLES: ClassVar[dict[str, dict[str, tuple[str, str]]]] = {
        "blocks": {
            name: (dtype, "NULLABLE")
            for name, dtype in {"block_number": "INTEGER", "block_timestamp": "TIMESTAMP", "block_hash": "STRING", "parent_hash": "STRING"}.items()
        },
        "block_features": {name: ("INTEGER", "NULLABLE") for name in ("base_fee_per_gas", "gas_used", "gas_limit")},
        "transactions": {"block_hash": ("STRING", "NULLABLE"), "block_timestamp": ("TIMESTAMP", "NULLABLE")},
        "receipts": {
            name: (dtype, "NULLABLE") for name, dtype in {"block_hash": "STRING", "block_timestamp": "TIMESTAMP", "transaction_index": "INTEGER"}.items()
        },
        "receipt_features": {name: ("INTEGER", "NULLABLE") for name in ("gas_used", "effective_gas_price")},
    }

    def __init__(self, chain: ChainServer, *, bytes_processed: int = 100, wrong_hash_receipts: bool = False) -> None:
        self.chain = chain
        self.tables = {name: dict(fields) for name, fields in self.TABLES.items()}
        self.tables["blocks"].update(self.tables.pop("block_features"))
        self.tables["receipts"].update(self.tables.pop("receipt_features"))
        self.bytes_processed = bytes_processed
        self.wrong_hash_receipts = wrong_hash_receipts
        self.calls: list[str] = []
        self.sql = ""
        self.page_size = 0

    def table_schema(self, dataset: str, table: str) -> dict[str, tuple[str, str]]:
        self.calls.append(f"schema:{dataset}.{table}")
        return self.tables[table]

    def dry_run(self, sql: str, parameters: dict[str, int]) -> tuple[int, dict[str, tuple[str, str]]]:
        self.calls.append("dry_run")
        self.sql = sql
        del parameters
        return self.bytes_processed, _query_schema(sql)

    def pages(self, sql: str, parameters: dict[str, int], maximum_bytes_billed: int, page_size: int):
        self.calls.append("execute")
        self.sql, self.page_size = sql, page_size
        del maximum_bytes_billed
        columns = _query_schema(sql)
        rows = []
        receipt_join_is_fork_safe = "ON r.block_hash = b.block_hash" in sql
        for number in range(parameters["first_block"], parameters["last_block"] + 1):
            block = self.chain.block(number)
            values = {"block_number": number, "_proof_timestamp": self.chain.timestamp_base + number, "timestamp": self.chain.timestamp_base + number}
            values.update({"_proof_hash": block["hash"], "block_hash": block["hash"]})
            values.update({"_proof_parent_hash": block["parentHash"], "parent_hash": block["parentHash"]})
            values.update({"_proof_gas_used": 15_000_000 + number, "_receipt_gas_used": 15_000_000 + number, "gas_used": 15_000_000 + number})
            values["_proof_gas_limit"] = 30_000_000
            values.update({"base_fee_per_gas": 1_000_000_000 + number, "gas_limit": 30_000_000, "tx_count": number % 3})
            multiplier = 1 if receipt_join_is_fork_safe or not self.wrong_hash_receipts else 9
            values.update({"effective_priority_fee_per_gas_p50": number * 50 * multiplier, "effective_priority_fee_per_gas_p90": number * 90 * multiplier})
            rows.append({name: values[name] for name in columns})
        for offset in range(0, len(rows), page_size):
            yield iter(rows[offset : offset + page_size])


def _query_schema(sql: str) -> dict[str, tuple[str, str]]:
    strings = {"_proof_hash", "_proof_parent_hash", "block_hash", "parent_hash"}
    integers = (  # noqa: SIM905 - compact fake schema fixture
        "block_number _proof_timestamp _proof_gas_used _proof_gas_limit _receipt_gas_used timestamp base_fee_per_gas gas_used gas_limit tx_count "
        "effective_priority_fee_per_gas_p50 effective_priority_fee_per_gas_p90"
    ).split()
    return {name: ("STRING" if name in strings else "INTEGER", "NULLABLE") for name in [*strings, *integers] if f" AS {name}" in sql}


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
        source: str = "rpc",
        dataset: str | None = None,
        include_primary: bool = True,
    ) -> Path:
        quoted_features = ", ".join(json.dumps(feature) for feature in features)
        chain_dataset = f"bigquery_dataset = {json.dumps(dataset)}" if dataset else ""
        bigquery_config = '\n[bigquery]\nproject = "billing-project"\nmaximum_bytes_billed = 1000\n' if dataset else ""
        default_provider = 'provider = "primary"' if include_primary else ""
        primary_provider = (
            f"""[providers.primary]\nurl = {json.dumps(primary.url)}\nbatch_size = 3\nconcurrency = 2\ntimeout = 2\n""" if include_primary else ""
        )
        path.write_text(
            f"""[defaults]
chain = "test"
source = {json.dumps(source)}
{default_provider}
verifier = "verifier"
output_root = {json.dumps(str(output_root))}
format = {json.dumps(output_format)}
features = [{quoted_features}]

[chains.test]
chain_id = 1
finality_tag = "finalized"
{chain_dataset}

{primary_provider}
[providers.verifier]
url = {json.dumps(verifier.url)}
batch_size = 3
concurrency = 2
timeout = 2
{bigquery_config}
""",
            encoding="utf-8",
        )
        return path

    return write
