"""Local stand-ins for the external services, built from one deterministic fake chain.

`ChainServer` answers JSON-RPC over HTTP. `FakeBigQuery` runs the SQL Blockweaver compiles, transpiled to DuckDB, over
tables filled from the same chain. The two derive priority fees independently: the server applies the client rule in
Python, the warehouse executes the query.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Callable, Iterator
from contextlib import suppress
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import duckdb
import sqlglot
from typer.testing import CliRunner

from blockweaver.cli import app

DATASET_ID = "11111111-1111-4111-8111-111111111111"
BQ_DATASET = "bigquery-public-data.goog_blockchain_test_us"
GAS_LIMIT = 30_000_000
BASE_FEE = 1_000_000_000
ROUNDING_EDGE_BLOCK = 17


@dataclass(frozen=True, slots=True)
class Transaction:
    gas_used: int
    priority_fee: int


def default_transactions(number: int) -> list[Transaction]:
    """Transactions in index order. The shapes cover empty blocks, ties, exact thresholds and fees out of index order."""
    if number == ROUNDING_EDGE_BLOCK:
        return [Transaction(21_000, 1), Transaction(21_001, 5)]
    return [
        [],
        [Transaction(21_000, number)],
        [Transaction(21_000, number + 7), Transaction(21_000, number)],
        [Transaction(50_000, number + 9), Transaction(21_000, number + 2), Transaction(30_000, number + 5)],
        [Transaction(30_000, 4), Transaction(40_000, number), Transaction(30_000, 4)],
    ][number % 5]


def client_rewards(transactions: list[Transaction], percentiles: list[int]) -> list[int]:
    """The execution-client rule behind eth_feeHistory: the first transaction, by ascending fee, whose running gas reaches the floored threshold."""
    if not transactions:
        return [0 for _ in percentiles]
    ordered = [item for _index, item in sorted(enumerate(transactions), key=lambda pair: (pair[1].priority_fee, pair[0]))]
    gas_used = sum(item.gas_used for item in transactions)
    rewards = []
    for percentile in percentiles:
        threshold = gas_used * percentile // 100
        running = 0
        chosen = ordered[-1]
        for item in ordered:
            running += item.gas_used
            if running >= threshold:
                chosen = item
                break
        rewards.append(chosen.priority_fee)
    return rewards


def block_hash(number: int) -> str:
    return f"0x{number + 1:064x}"


class ChainServer:
    def __init__(
        self,
        *,
        chain_id: int = 1,
        finalized: int = 30,
        timestamp_base: int = 1_700_000_000,
        transactions: dict[int, list[Transaction]] | None = None,
    ) -> None:
        self.chain_id = chain_id
        self.finalized = finalized
        self.timestamp_base = timestamp_base
        self.transaction_overrides = transactions or {}
        self.requests: list[list[dict[str, Any]]] = []
        self.request_counts: dict[int, int] = {}
        self.http_failures = 0
        self.retry_after = "0"
        self.changes: dict[int, dict[str, Any]] = {}
        self.tag_changes: dict[str, Any] = {}
        self.fee_history_changes: dict[str, Any] = {}
        self.wrong_id_once = False
        self.rpc_errors: list[int] = []
        self.item_errors: dict[int, list[int]] = {}
        self.limit_batches = False
        self.when_block_requested: dict[int, Callable[[], None]] = {}
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
                        result = {
                            "oldestBlock": hex(oldest),
                            "reward": [
                                [hex(reward) for reward in client_rewards(state.transactions(number), params[2])] for number in range(oldest, newest + 1)
                            ],
                            **state.fee_history_changes,
                        }
                    else:
                        selector = params[0]
                        number = state.finalized if selector in {"finalized", "safe"} else int(selector, 16)
                        state.request_counts[number] = state.request_counts.get(number, 0) + 1
                        if selector not in {"finalized", "safe"} and number in state.when_block_requested:
                            state.when_block_requested.pop(number)()
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
        self._thread = threading.Thread(target=self._server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self._server.server_port}"

    def transactions(self, number: int) -> list[Transaction]:
        return self.transaction_overrides.get(number, default_transactions(number))

    def block(self, number: int) -> dict[str, Any]:
        transactions = self.transactions(number)
        return {
            "number": hex(number),
            "hash": block_hash(number),
            "parentHash": block_hash(number - 1),
            "timestamp": hex(self.timestamp_base + number),
            "baseFeePerGas": hex(BASE_FEE + number),
            "gasUsed": hex(sum(item.gas_used for item in transactions)),
            "gasLimit": hex(GAS_LIMIT),
            "transactions": [block_hash(number * 10 + offset) for offset in range(len(transactions))],
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


_BIGQUERY_TYPES = {"BIGINT": "INTEGER", "HUGEINT": "INTEGER", "VARCHAR": "STRING", "TIMESTAMP WITH TIME ZONE": "TIMESTAMP"}


def _bigquery_field(duckdb_type: str) -> tuple[str, str]:
    if duckdb_type.endswith("[]"):
        return _BIGQUERY_TYPES[duckdb_type.removesuffix("[]")], "REPEATED"
    return _BIGQUERY_TYPES[duckdb_type], "NULLABLE"


class FakeBigQuery:
    """The warehouse boundary Blockweaver calls, answered by executing its SQL in DuckDB."""

    def __init__(self, chain: ChainServer, *, bytes_processed: int = 100) -> None:
        self.bytes_processed = bytes_processed
        self.calls: list[str] = []
        self.page_size = 0
        project, dataset = BQ_DATASET.split(".")
        self._prefix = f'"{project}"."{dataset}"'
        self.database = duckdb.connect()
        self.database.execute("SET TimeZone = 'UTC'")
        self.database.execute(f"ATTACH ':memory:' AS \"{project}\"")
        self.database.execute(f'CREATE SCHEMA "{project}"."{dataset}"')
        self.execute(
            "CREATE TABLE {blocks} (block_number BIGINT, block_timestamp TIMESTAMPTZ, block_hash VARCHAR, parent_hash VARCHAR, "
            "base_fee_per_gas BIGINT, gas_used BIGINT, gas_limit BIGINT)"
        )
        self.execute("CREATE TABLE {transactions} (block_hash VARCHAR, block_timestamp TIMESTAMPTZ)")
        self.execute(
            "CREATE TABLE {receipts} (block_hash VARCHAR, block_timestamp TIMESTAMPTZ, transaction_index BIGINT, gas_used BIGINT, effective_gas_price BIGINT)"
        )
        blocks, transactions, receipts = [], [], []
        for number in range(chain.finalized + 1):
            block = chain.block(number)
            timestamp, base_fee = int(block["timestamp"], 16), int(block["baseFeePerGas"], 16)
            blocks.append((number, timestamp, block["hash"], block["parentHash"], base_fee, int(block["gasUsed"], 16), int(block["gasLimit"], 16)))
            orphan = block_hash(number + 1_000_000)
            transactions.append((orphan, timestamp))
            receipts.append((orphan, timestamp, 0, 21_000, base_fee + 1_000_000))
            for index, item in enumerate(chain.transactions(number)):
                transactions.append((block["hash"], timestamp))
                receipts.append((block["hash"], timestamp, index, item.gas_used, base_fee + item.priority_fee))
        self.database.executemany(self._sql("INSERT INTO {blocks} VALUES (?, to_timestamp(?), ?, ?, ?, ?, ?)"), blocks)
        self.database.executemany(self._sql("INSERT INTO {transactions} VALUES (?, to_timestamp(?))"), transactions)
        self.database.executemany(self._sql("INSERT INTO {receipts} VALUES (?, to_timestamp(?), ?, ?, ?)"), receipts)

    def _sql(self, statement: str) -> str:
        return statement.format(**{table: f'{self._prefix}."{table}"' for table in ("blocks", "transactions", "receipts")})

    def execute(self, statement: str) -> None:
        """Change the warehouse tables. Name them as {blocks}, {transactions} and {receipts}."""
        self.database.execute(self._sql(statement))

    def table_schema(self, dataset: str, table: str) -> dict[str, tuple[str, str]]:
        self.calls.append(f"schema:{dataset}.{table}")
        project, name = dataset.split(".")
        columns = self.database.execute(f'DESCRIBE "{project}"."{name}"."{table}"').fetchall()
        return {column[0]: _bigquery_field(column[1]) for column in columns}

    def dry_run(self, sql: str, parameters: dict[str, int]) -> tuple[int, dict[str, tuple[str, str]]]:
        self.calls.append("dry_run")
        described = self.database.execute(f"SELECT * FROM ({_transpile(sql)}) LIMIT 0", parameters).description
        return self.bytes_processed, {column[0]: _bigquery_field(str(column[1])) for column in described}

    def pages(self, sql: str, parameters: dict[str, int], maximum_bytes_billed: int, page_size: int) -> Iterator[Iterator[dict[str, object]]]:
        self.calls.append("execute")
        self.page_size = page_size
        del maximum_bytes_billed
        cursor = self.database.execute(_transpile(sql), parameters)
        names = [column[0] for column in cursor.description]
        rows = [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]
        for offset in range(0, len(rows), page_size):
            yield iter(rows[offset : offset + page_size])


def _transpile(sql: str) -> str:
    return sqlglot.transpile(sql, read="bigquery", write="duckdb")[0]


def invoke(arguments: list[str], env: dict[str, str] | None = None):
    return CliRunner().invoke(app, arguments, env=env)


def download_args(config: Path, *, first: int = 10, last: int = 14, dataset_id: str = DATASET_ID) -> list[str]:
    return ["download", "--config", str(config), "--id", dataset_id, "--from-block", str(first), "--to-block", str(last)]


def bigquery_config(make_config: Any, path: Path, primary: ChainServer, verifier: ChainServer, output_root: Path, **values: Any) -> Path:
    return make_config(path, primary, verifier, output_root=output_root, source="bigquery", dataset=BQ_DATASET, **values)


def artifact_from(result: Any) -> Path:
    assert result.exit_code == 0, result.output
    return Path(json.loads(result.stdout)["path"])


def error(result: Any) -> dict[str, Any]:
    assert result.exit_code != 0
    assert result.stdout == ""
    lines = result.stderr.splitlines()
    return json.loads(lines[-1])
