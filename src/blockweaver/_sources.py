"""Bounded external RPC and BigQuery source adapters."""

from __future__ import annotations

import asyncio
import random
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from importlib import import_module
from typing import Any

import aiohttp

from ._contract import BlockweaverError, Header, Plan, parse_header, quantity

_TRANSIENT_HTTP = {408, 425, 429, *range(500, 600)}
Validator = Callable[[Any], Any]
BigQuerySchema = dict[str, tuple[str, str]]


@dataclass(frozen=True, slots=True)
class BigQueryPlan:
    sql: str
    table_fields: dict[str, dict[str, str]]
    result_schema: dict[str, str]


class BigQueryClient:
    def __init__(self, module: Any, project: str) -> None:
        self._module = module
        self._client = module.Client(project=project)

    def table_schema(self, dataset: str, table: str) -> BigQuerySchema:
        value = self._client.get_table(f"{dataset}.{table}")
        return {field.name: (field.field_type, field.mode) for field in value.schema}

    def dry_run(self, sql: str, parameters: dict[str, int]) -> tuple[int, BigQuerySchema]:
        config = self._config(parameters, dry_run=True)
        job = self._client.query(sql, job_config=config)
        if type(job.total_bytes_processed) is not int or job.total_bytes_processed < 0:
            raise BlockweaverError("BIGQUERY_INVALID", "BigQuery dry run did not return a byte estimate")
        return job.total_bytes_processed, {field.name: (field.field_type, field.mode) for field in job.schema}

    def pages(
        self,
        sql: str,
        parameters: dict[str, int],
        maximum_bytes_billed: int,
        page_size: int,
    ) -> Iterator[Iterator[dict[str, object]]]:
        config = self._config(parameters, maximum_bytes_billed=maximum_bytes_billed)
        rows = self._client.query(sql, job_config=config).result(page_size=page_size)
        for page in rows.pages:
            yield (dict(row.items()) for row in page)

    def _config(self, parameters: dict[str, int], *, dry_run: bool = False, maximum_bytes_billed: int | None = None) -> Any:
        return self._module.QueryJobConfig(
            dry_run=dry_run,
            use_query_cache=False,
            maximum_bytes_billed=maximum_bytes_billed,
            query_parameters=[self._module.ScalarQueryParameter(name, "INT64", value) for name, value in parameters.items()],
        )


def open_bigquery(project: str) -> BigQueryClient:
    try:
        module = import_module("google.cloud.bigquery")
    except ModuleNotFoundError:
        raise BlockweaverError(
            "source_dependency_missing",
            "BigQuery source requires the optional blockweaver[bigquery] dependency",
        ) from None
    try:
        return BigQueryClient(module, project)
    except Exception as error:
        raise BlockweaverError("BIGQUERY_FAILED", f"Cannot initialize BigQuery client: {type(error).__name__}") from None


def compile_bigquery(dataset: str, plan: Plan) -> BigQueryPlan:
    block_fields = {
        "block_number": "INTEGER",
        "block_timestamp": "TIMESTAMP",
        "block_hash": "STRING",
        "parent_hash": "STRING",
    }
    result_schema = {
        "block_number": "INTEGER",
        "_proof_timestamp": "INTEGER",
        "_proof_hash": "STRING",
        "_proof_parent_hash": "STRING",
    }
    for feature in plan.features:
        if feature.bigquery_field is not None:
            block_fields[feature.bigquery_field] = (
                "TIMESTAMP" if feature.bigquery_field == "block_timestamp" else "STRING" if feature.dtype == "UTF-8" else "INTEGER"
            )
        block_fields.update({dependency: "INTEGER" for dependency in feature.bigquery_dependencies})
    if plan.percentiles:
        block_fields.update({"base_fee_per_gas": "INTEGER", "gas_used": "INTEGER"})
    gas_proof = any(feature.bigquery_dependencies for feature in plan.features)
    if gas_proof:
        block_fields.update({"gas_used": "INTEGER", "gas_limit": "INTEGER"})
    tables = {"blocks": block_fields}
    ctes = [
        "requested_blocks AS (\n"
        f"  SELECT {', '.join(sorted(block_fields))}\n"
        f"  FROM `{dataset}.blocks`\n"
        "  WHERE block_number BETWEEN @first_block AND @last_block\n"
        "    AND block_timestamp BETWEEN TIMESTAMP_SECONDS(@from_timestamp) AND TIMESTAMP_SECONDS(@to_timestamp)\n"
        ")"
    ]
    joins: list[str] = []
    projections = [
        "b.block_number AS block_number",
        "UNIX_SECONDS(b.block_timestamp) AS _proof_timestamp",
        "b.block_hash AS _proof_hash",
        "b.parent_hash AS _proof_parent_hash",
    ]
    if gas_proof:
        projections.extend(["b.gas_used AS _proof_gas_used", "b.gas_limit AS _proof_gas_limit"])
        result_schema.update({"_proof_gas_used": "INTEGER", "_proof_gas_limit": "INTEGER"})
    if any(feature.bigquery_family == "transactions" for feature in plan.features):
        tables["transactions"] = {"block_number": "INTEGER", "block_timestamp": "TIMESTAMP"}
        ctes.append(
            "tx_counts AS (\n"
            "  SELECT block_number, COUNT(*) AS tx_count\n"
            f"  FROM `{dataset}.transactions`\n"
            "  WHERE block_number BETWEEN @first_block AND @last_block\n"
            "    AND block_timestamp BETWEEN TIMESTAMP_SECONDS(@from_timestamp) AND TIMESTAMP_SECONDS(@to_timestamp)\n"
            "  GROUP BY block_number\n"
            ")"
        )
        joins.append("LEFT JOIN tx_counts AS t USING (block_number)")
    if plan.percentiles:
        tables["receipts"] = {
            "block_number": "INTEGER",
            "block_hash": "STRING",
            "block_timestamp": "TIMESTAMP",
            "transaction_index": "INTEGER",
            "gas_used": "INTEGER",
            "effective_gas_price": "INTEGER",
        }
        ctes.extend(
            [
                "requested_receipts AS (\n"
                "  SELECT block_number, block_hash, transaction_index, gas_used, effective_gas_price\n"
                f"  FROM `{dataset}.receipts`\n"
                "  WHERE block_number BETWEEN @first_block AND @last_block\n"
                "    AND block_timestamp BETWEEN TIMESTAMP_SECONDS(@from_timestamp) AND TIMESTAMP_SECONDS(@to_timestamp)\n"
                ")",
                "weighted_receipts AS (\n"
                "  SELECT r.block_number, r.transaction_index, r.gas_used, b.gas_used AS block_gas_used,\n"
                "    r.effective_gas_price - b.base_fee_per_gas AS priority_fee,\n"
                "    SUM(r.gas_used) OVER (PARTITION BY r.block_number "
                "ORDER BY r.effective_gas_price - b.base_fee_per_gas, r.transaction_index "
                "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_gas\n"
                "  FROM requested_receipts AS r JOIN requested_blocks AS b\n"
                "    ON r.block_number = b.block_number AND r.block_hash = b.block_hash\n"
                ")",
                "receipt_stats AS (\n"
                "  SELECT block_number, SUM(gas_used) AS receipt_gas_used,\n    "
                + ",\n    ".join(
                    f"MIN(IF(cumulative_gas >= CAST(CEIL(CAST(block_gas_used AS BIGNUMERIC) * {percentile} / 100) AS INT64), "
                    f"priority_fee, NULL)) AS effective_priority_fee_per_gas_p{percentile}"
                    for percentile in plan.percentiles
                )
                + "\n  FROM weighted_receipts GROUP BY block_number\n)",
            ]
        )
        joins.append("LEFT JOIN receipt_stats AS r USING (block_number)")
        if not gas_proof:
            projections.append("b.gas_used AS _proof_gas_used")
            result_schema["_proof_gas_used"] = "INTEGER"
        projections.append("COALESCE(r.receipt_gas_used, 0) AS _receipt_gas_used")
        result_schema["_receipt_gas_used"] = "INTEGER"
    for feature in plan.features:
        if feature.bigquery_family == "blocks":
            assert feature.bigquery_field is not None
            expression = "UNIX_SECONDS(b.block_timestamp)" if feature.bigquery_field == "block_timestamp" else f"b.{feature.bigquery_field}"
            projections.append(f"{expression} AS {feature.name}")
        elif feature.bigquery_family == "transactions":
            projections.append(f"COALESCE(t.tx_count, 0) AS {feature.name}")
        else:
            projections.append(f"COALESCE(r.{feature.name}, 0) AS {feature.name}")
        result_schema[feature.name] = "STRING" if feature.dtype == "UTF-8" else "INTEGER"
    sql = "WITH " + ",\n".join(ctes) + "\nSELECT\n  " + ",\n  ".join(projections) + "\nFROM requested_blocks AS b\n"
    if joins:
        sql += "\n".join(joins) + "\n"
    sql += "ORDER BY block_number"
    return BigQueryPlan(sql, tables, result_schema)


class Rpc:
    def __init__(self, url: str, *, batch_size: int, concurrency: int, timeout: float) -> None:
        self._url = url
        self._batch_size = batch_size
        self._semaphore = asyncio.Semaphore(concurrency)
        self._timeout = timeout
        self._session: aiohttp.ClientSession | None = None
        self._next_id = 1

    async def __aenter__(self) -> Rpc:
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self._timeout))
        return self

    async def __aexit__(self, *_args: object) -> None:
        assert self._session is not None
        await self._session.close()

    def _calls(self, method: str, parameters: Iterable[list[object]]) -> list[dict[str, Any]]:
        calls: list[dict[str, Any]] = []
        for params in parameters:
            calls.append({"jsonrpc": "2.0", "id": self._next_id, "method": method, "params": params})
            self._next_id += 1
        return calls

    async def chain_id(self) -> int:
        call = self._calls("eth_chainId", [[]])[0]
        item_id = int(call["id"])
        reply = await self._run([call], {item_id: lambda value: quantity(value, "chain id")})
        return reply[item_id]

    async def headers(self, numbers: Iterable[int], plan: Plan) -> list[Header]:
        ordered = list(numbers)
        calls = self._calls("eth_getBlockByNumber", [[hex(number), False] for number in ordered])
        validators = {
            int(call["id"]): lambda value, expected=number: parse_header(value, expected=expected, plan=plan)
            for call, number in zip(calls, ordered, strict=True)
        }
        groups = [calls[index : index + self._batch_size] for index in range(0, len(calls), self._batch_size)]
        replies: dict[int, Any] = {}
        for part in await self._run_groups(groups, validators):
            replies.update(part)
        return [replies[int(call["id"])] for call in calls]

    async def header(self, number: int, plan: Plan) -> Header:
        return (await self.headers([number], plan))[0]

    async def tagged_header(self, tag: str, plan: Plan) -> Header:
        call = self._calls("eth_getBlockByNumber", [[tag, False]])[0]
        item_id = int(call["id"])
        reply = await self._run([call], {item_id: lambda value: parse_header(value, expected=None, plan=plan)})
        return reply[item_id]

    async def fee_history(self, first_block: int, last_block: int, percentiles: tuple[int, ...]) -> list[dict[int, int]]:
        if not percentiles:
            return [{} for _ in range(last_block - first_block + 1)]
        count = last_block - first_block + 1
        call = self._calls("eth_feeHistory", [[hex(count), hex(last_block), list(percentiles)]])[0]
        item_id = int(call["id"])
        reply = await self._run(
            [call],
            {item_id: lambda value: _parse_fee_history(value, first_block, count, percentiles)},
        )
        return reply[item_id]

    async def rows(self, first_block: int, last_block: int, plan: Plan) -> tuple[list[Header], list[dict[str, int | str]]]:
        tasks = [
            asyncio.create_task(self.headers(range(first_block, last_block + 1), plan)),
            asyncio.create_task(self.fee_history(first_block, last_block, plan.percentiles)),
        ]
        try:
            headers, fees = await asyncio.gather(*tasks)
        except BaseException:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        return headers, [header.row(plan, fee) for header, fee in zip(headers, fees, strict=True)]

    async def _run(self, calls: list[dict[str, Any]], validators: dict[int, Validator], prior_attempts: int = 0) -> dict[int, Any]:
        pending = {int(call["id"]): call for call in calls}
        complete: dict[int, Any] = {}
        attempt = prior_attempts
        while pending and attempt < 12:
            attempt += 1
            status, payload, retry_after = await self._post(list(pending.values()))
            if status not in _TRANSIENT_HTTP:
                if status != 200:
                    raise RuntimeError(f"RPC returned non-retryable HTTP status {status}")
                accepted, retry = self._parse(payload, set(pending), validators)
                complete.update(accepted)
                pending = {item_id: pending[item_id] for item_id in retry}
            if not pending:
                return complete
            if attempt >= 3 and len(pending) > 1:
                items = list(pending.values())
                midpoint = len(items) // 2
                first, second = await self._run_groups([items[:midpoint], items[midpoint:]], validators, attempt)
                complete.update(first)
                complete.update(second)
                return complete
            delay = retry_after if retry_after is not None else random.uniform(0, min(2 ** (attempt - 4), 2.0))
            await asyncio.sleep(max(0.0, delay))
        raise RuntimeError("RPC request failed after 12 attempts")

    async def _run_groups(
        self,
        groups: Iterable[list[dict[str, Any]]],
        validators: dict[int, Validator],
        prior_attempts: int = 0,
    ) -> list[dict[int, Any]]:
        tasks = [asyncio.create_task(self._run(group, validators, prior_attempts)) for group in groups if group]
        try:
            return list(await asyncio.gather(*tasks))
        except BaseException:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

    async def _post(self, calls: list[dict[str, Any]]) -> tuple[int, Any, float | None]:
        assert self._session is not None
        try:
            async with self._semaphore, self._session.post(self._url, json=calls) as response:
                retry_after = _retry_after(response.headers.get("Retry-After"))
                if response.status in _TRANSIENT_HTTP:
                    await response.read()
                    return response.status, None, retry_after
                try:
                    return response.status, await response.json(content_type=None), retry_after
                except (ValueError, aiohttp.ClientError) as error:
                    raise RuntimeError("RPC returned invalid JSON") from error
        except (TimeoutError, aiohttp.ClientError):
            return 408, None, None

    @staticmethod
    def _parse(payload: Any, expected: set[int], validators: dict[int, Validator]) -> tuple[dict[int, Any], set[int]]:
        if not isinstance(payload, list):
            raise ValueError("Invalid JSON-RPC batch response shape")
        accepted: dict[int, Any] = {}
        retry = set(expected)
        seen: set[int] = set()
        for member in payload:
            if not isinstance(member, dict) or member.get("jsonrpc") != "2.0":
                raise ValueError("Invalid JSON-RPC response member")
            item_id = member.get("id")
            if type(item_id) is not int or item_id not in expected or item_id in seen:
                raise ValueError("JSON-RPC response ID mismatch")
            seen.add(item_id)
            has_result = "result" in member
            has_error = "error" in member
            if has_result == has_error:
                raise ValueError("Invalid JSON-RPC result shape")
            if has_error:
                error = member["error"]
                if not isinstance(error, dict) or type(error.get("code")) is not int or not isinstance(error.get("message"), str):
                    raise ValueError("Invalid JSON-RPC error shape")
                if error["code"] in {-32700, -32600, -32601, -32602}:
                    raise RuntimeError("RPC rejected a valid protocol request")
                continue
            if member["result"] is None:
                continue
            accepted[item_id] = validators[item_id](member["result"])
            retry.remove(item_id)
        return accepted, retry


def _retry_after(value: str | None) -> float | None:
    try:
        return min(float(value or ""), 60.0)
    except ValueError:
        try:
            when = parsedate_to_datetime(value or "")
            return min(max(0.0, (when - datetime.now(UTC)).total_seconds()), 60.0)
        except (TypeError, ValueError):
            return None


def _parse_fee_history(value: Any, first_block: int, count: int, percentiles: tuple[int, ...]) -> list[dict[int, int]]:
    if not isinstance(value, dict):
        raise ValueError("Invalid fee history response shape")
    if quantity(value.get("oldestBlock"), "fee history oldestBlock") != first_block:
        raise ValueError("fee history oldestBlock does not match the requested range")
    rewards = value.get("reward")
    if not isinstance(rewards, list) or len(rewards) != count:
        raise ValueError("fee history reward coverage does not match the requested range")
    if any(not isinstance(row, list) or len(row) != len(percentiles) for row in rewards):
        raise ValueError("fee history reward row does not match requested percentiles")
    return [{percentile: quantity(item, f"priority fee P{percentile}") for percentile, item in zip(percentiles, row, strict=True)} for row in rewards]
