"""Bounded external RPC and BigQuery source adapters."""

from __future__ import annotations

import asyncio
import hashlib
import random
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from importlib import import_module
from math import isfinite
from typing import Any, Protocol, TypeVar
from uuid import UUID

import aiohttp

from ._contract import (
    Anchor,
    BigQueryDownloadRequest,
    BlockweaverError,
    DownloadRequest,
    Header,
    Plan,
    Provider,
    RequestedRange,
    ResolvedRange,
    RpcDownloadRequest,
    Value,
    block_hash,
    parse_header,
    plan_features,
    quantity,
    validate_links,
)
from ._corpus import Dataset, FactReader, VerifiedProof

_TRANSIENT_HTTP = {408, 425, 429, *range(500, 600)}
_FATAL_RPC = {-32700, -32600, -32601, -32602, -32000, -32001, -32003, -32004, -32006}
_LIMIT_RPC = -32005
CHUNK_SIZE = 1024
Validator = Callable[[Any], Any]
BigQuerySchema = dict[str, tuple[str, str]]
_Left = TypeVar("_Left")
_Right = TypeVar("_Right")


@dataclass(frozen=True, slots=True)
class BigQueryPlan:
    sql: str
    table_fields: dict[str, dict[str, str]]
    result_schema: dict[str, str]


async def _paired(left: Awaitable[_Left], right: Awaitable[_Right]) -> tuple[_Left, _Right]:
    left_task = asyncio.ensure_future(left)
    right_task = asyncio.ensure_future(right)
    try:
        left_result, right_result = await asyncio.gather(left_task, right_task)
    except BaseException:
        left_task.cancel()
        right_task.cancel()
        await asyncio.gather(left_task, right_task, return_exceptions=True)
        raise
    return left_result, right_result


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
        if not plan.percentiles:
            headers = await self.headers(range(first_block, last_block + 1), plan)
            return headers, [header.row(plan) for header in headers]
        headers, fees = await _paired(
            self.headers(range(first_block, last_block + 1), plan),
            self.fee_history(first_block, last_block, plan.percentiles),
        )
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
                accepted, retry, limit = self._parse(payload, tuple(pending), validators)
                complete.update(accepted)
                pending = {item_id: pending[item_id] for item_id in retry}
                if limit:
                    if len(pending) == 1:
                        raise RuntimeError("RPC limit exceeded for a single request")
                    items = list(pending.values())
                    midpoint = len(items) // 2
                    first, second = await self._run_groups([items[:midpoint], items[midpoint:]], validators, attempt)
                    complete.update(first)
                    complete.update(second)
                    return complete
            if not pending:
                return complete
            if attempt >= 3 and len(pending) > 1:
                items = list(pending.values())
                midpoint = len(items) // 2
                first, second = await self._run_groups([items[:midpoint], items[midpoint:]], validators, attempt)
                complete.update(first)
                complete.update(second)
                return complete
            if attempt == 12:
                break
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
    def _parse(payload: Any, expected: tuple[int, ...], validators: dict[int, Validator]) -> tuple[dict[int, Any], tuple[int, ...], bool]:
        if not isinstance(payload, list):
            raise ValueError("Invalid JSON-RPC batch response shape")
        accepted: dict[int, Any] = {}
        expected_ids = set(expected)
        seen: set[int] = set()
        limit = False
        for member in payload:
            if not isinstance(member, dict) or member.get("jsonrpc") != "2.0":
                raise ValueError("Invalid JSON-RPC response member")
            item_id = member.get("id")
            if type(item_id) is not int or item_id not in expected_ids or item_id in seen:
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
                if error["code"] in _FATAL_RPC:
                    raise RuntimeError("RPC rejected a valid protocol request")
                limit = limit or error["code"] == _LIMIT_RPC
                continue
            if member["result"] is None:
                continue
            accepted[item_id] = validators[item_id](member["result"])
        return accepted, tuple(item_id for item_id in expected if item_id not in accepted), limit


def _retry_after(value: str | None) -> float | None:
    try:
        delay = float(value or "")
        return min(delay, 60.0) if isfinite(delay) and delay >= 0 else None
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


Progress = Callable[[dict[str, object]], None]
SourceOperation = Callable[["SourceAdapter"], Awaitable[dict[str, object]]]


class SourceAdapter(Protocol):
    resolved: ResolvedRange

    @property
    def chunk_size(self) -> int: ...

    def chunks(self, first: int, last: int) -> AsyncIterator[tuple[list[Header], list[dict[str, Value]]]]: ...

    async def prove(self, target: Header, read_facts: FactReader) -> VerifiedProof: ...

    async def revalidate(self, dataset: Dataset) -> None: ...


class RpcSource:
    def __init__(
        self,
        request: RpcDownloadRequest,
        primary: Rpc,
        verifier: Rpc,
        resolved: ResolvedRange,
        primary_chain_id: int,
        verifier_chain_id: int,
    ) -> None:
        self.request = request
        self.primary = primary
        self.verifier = verifier
        self.resolved = resolved
        self._verification = {"primary_chain_id": primary_chain_id, "verifier_chain_id": verifier_chain_id}

    @property
    def chunk_size(self) -> int:
        return CHUNK_SIZE

    async def chunks(self, first: int, last: int) -> AsyncIterator[tuple[list[Header], list[dict[str, Value]]]]:
        while first <= last:
            end = min(first + CHUNK_SIZE - 1, last)
            yield await self.primary.rows(first, end, self.request.plan)
            first = end + 1

    async def prove(self, target: Header, read_facts: FactReader) -> VerifiedProof:
        anchor, verifier_target = await _prove_finality(target, self.verifier, self.request)
        samples = sample_numbers(self.request.dataset_id, self.resolved.first_block, self.resolved.last_block)
        facts = read_facts(samples)
        await _check_rows(facts, samples, self.request.plan, self.verifier)
        return VerifiedProof(anchor, {**self._verification, "target_agreement": target == verifier_target, "sampled_blocks": samples}, facts)

    async def revalidate(self, dataset: Dataset) -> None:
        verifier_target = await _candidate_target(dataset, self.verifier, self.request)
        target = await self.primary.header(dataset.last_block, dataset._plan)
        if not _same_header(target, verifier_target):
            raise BlockweaverError("RPC_MISMATCH", "RPC endpoints disagree on the ready candidate target")
        await _finish_candidate_validation(dataset, verifier_target, self.verifier)


class BigQuerySource:
    def __init__(
        self,
        request: BigQueryDownloadRequest,
        verifier: Rpc,
        warehouse: BigQueryClient,
        resolved: ResolvedRange,
        verifier_chain_id: int,
        progress: Progress,
    ) -> None:
        self.request = request
        self.verifier = verifier
        self.warehouse = warehouse
        self.resolved = resolved
        self.verifier_chain_id = verifier_chain_id
        self.progress = progress
        self.dry_run_bytes = 0

    @property
    def chunk_size(self) -> int:
        return CHUNK_SIZE

    async def chunks(self, first: int, last: int) -> AsyncIterator[tuple[list[Header], list[dict[str, Value]]]]:
        if first > last:
            return
        query = compile_bigquery(self.request.dataset, self.request.plan)
        parameters = {
            "first_block": first,
            "last_block": last,
            "from_timestamp": self.resolved.first_timestamp,
            "to_timestamp": self.resolved.last_timestamp,
        }
        self.dry_run_bytes = _prepare_bigquery(
            self.warehouse,
            self.request.dataset,
            query,
            parameters,
            self.request.bigquery.maximum_bytes_billed,
        )
        self.progress({"event": "bigquery_dry_run", "bytes_processed": self.dry_run_bytes})
        for chunk in _bigquery_chunks(
            self.warehouse,
            query,
            parameters,
            self.request.bigquery.maximum_bytes_billed,
            first,
            last,
            self.request.plan,
        ):
            yield chunk

    async def prove(self, target: Header, read_facts: FactReader) -> VerifiedProof:
        anchor, verifier_target = await _prove_finality(target, self.verifier, self.request)
        samples = sample_numbers(self.request.dataset_id, self.resolved.first_block, self.resolved.last_block)
        facts = read_facts(samples)
        await _check_rows(facts, samples, self.request.plan, self.verifier)
        return VerifiedProof(
            anchor,
            {
                "verifier_chain_id": self.verifier_chain_id,
                "dry_run_bytes": self.dry_run_bytes,
                "target_agreement": target == verifier_target,
                "sampled_blocks": samples,
            },
            facts,
        )

    async def revalidate(self, dataset: Dataset) -> None:
        verifier_target = await _candidate_target(dataset, self.verifier, self.request)
        await _finish_candidate_validation(dataset, verifier_target, self.verifier)


async def acquire(request: DownloadRequest, progress: Progress, operation: SourceOperation) -> dict[str, object]:
    if isinstance(request, RpcDownloadRequest):
        return await _acquire_rpc(request, operation)
    return await _acquire_bigquery(request, progress, operation)


async def _acquire_rpc(request: RpcDownloadRequest, operation: SourceOperation) -> dict[str, object]:
    try:
        async with _rpc(request.primary) as primary, _rpc(request.verifier) as verifier:
            primary_chain_id, verifier_chain_id = await _paired(primary.chain_id(), verifier.chain_id())
            if primary_chain_id != request.chain.chain_id or verifier_chain_id != request.chain.chain_id:
                raise BlockweaverError("RPC_CHAIN_MISMATCH", "RPC chain ID does not match the configured chain")
            primary_tag, verifier_tag = await _paired(
                primary.tagged_header(request.chain.finality_tag, _INTEGRITY_PLAN),
                verifier.tagged_header(request.chain.finality_tag, _INTEGRITY_PLAN),
            )
            resolved = await _resolve_range(request.requested_range, primary, min(primary_tag.block_number, verifier_tag.block_number))
            if request.requested_range.kind == "time":
                await _verify_time_boundaries(request.requested_range, resolved, primary, verifier, verifier_tag.block_number)
            return await operation(RpcSource(request, primary, verifier, resolved, primary_chain_id, verifier_chain_id))
    except BlockweaverError:
        raise
    except ValueError as error:
        raise BlockweaverError("RPC_INVALID", str(error)) from None
    except RuntimeError as error:
        raise BlockweaverError("RPC_FAILED", str(error)) from None
    except OSError as error:
        raise BlockweaverError("IO_FAILED", str(error)) from None


async def _acquire_bigquery(request: BigQueryDownloadRequest, progress: Progress, operation: SourceOperation) -> dict[str, object]:
    try:
        warehouse = open_bigquery(request.bigquery.project)
        async with _rpc(request.verifier) as verifier:
            verifier_chain_id = await verifier.chain_id()
            if verifier_chain_id != request.chain.chain_id:
                raise BlockweaverError("RPC_CHAIN_MISMATCH", "Verifier RPC chain ID does not match the configured chain")
            tagged = await verifier.tagged_header(request.chain.finality_tag, _INTEGRITY_PLAN)
            resolved = await _resolve_range(request.requested_range, verifier, tagged.block_number)
            if request.requested_range.kind == "time":
                await _verify_time_boundaries(request.requested_range, resolved, verifier, verifier, tagged.block_number)
            return await operation(BigQuerySource(request, verifier, warehouse, resolved, verifier_chain_id, progress))
    except BlockweaverError:
        raise
    except ValueError as error:
        raise BlockweaverError("BIGQUERY_INVALID", str(error)) from None
    except RuntimeError as error:
        raise BlockweaverError("RPC_FAILED", str(error)) from None
    except OSError as error:
        raise BlockweaverError("IO_FAILED", str(error)) from None
    except Exception as error:
        raise BlockweaverError("BIGQUERY_FAILED", str(error) or type(error).__name__) from None


async def verify_rpc(dataset: Dataset, provider: Provider, full: bool) -> dict[str, object]:
    try:
        async with _rpc(provider) as rpc:
            chain_id = await rpc.chain_id()
            if chain_id != dataset.chain_id:
                raise BlockweaverError("RPC_CHAIN_MISMATCH", "RPC chain ID does not match the dataset")
            target = await rpc.header(dataset.last_block, dataset._plan)
            if target.block_hash != dataset._target_hash:
                raise BlockweaverError("RPC_MISMATCH", "Dataset target hash does not match RPC")
            fresh = await _refresh_finality(target, dataset._anchor, rpc)
            samples = sample_numbers(dataset.dataset_id, dataset.first_block, dataset.last_block)
            if full:
                await _check_full_dataset(dataset, rpc)
            else:
                await _check_rows(dataset._facts(samples), samples, dataset._plan, rpc)
            return {
                "mode": "full_rpc" if full else "sample_rpc",
                "provider": provider.name,
                "chain_id": chain_id,
                "sampled_blocks": samples,
                "finalized_anchor": fresh.document(),
            }
    except BlockweaverError:
        raise
    except ValueError as error:
        raise BlockweaverError("RPC_INVALID", str(error)) from None
    except RuntimeError as error:
        raise BlockweaverError("RPC_FAILED", str(error)) from None


def _rpc(provider: Provider) -> Rpc:
    return Rpc(provider.url, batch_size=provider.batch_size, concurrency=provider.concurrency, timeout=provider.timeout)


_INTEGRITY_PLAN = plan_features([])


async def _resolve_range(request: RequestedRange, rpc: Rpc, finalized: int) -> ResolvedRange:
    if request.kind == "block":
        if request.end > finalized:
            raise BlockweaverError("RANGE_UNFINALIZED", "Requested block range is not fully finalized")
        first, last = await rpc.headers([request.start, request.end], _INTEGRITY_PLAN)
        return ResolvedRange(request.start, request.end, first.timestamp, last.timestamp)
    cache: dict[int, Header] = {}

    async def header(number: int) -> Header:
        if number not in cache:
            cache[number] = await rpc.header(number, _INTEGRITY_PLAN)
        return cache[number]

    genesis, latest = await header(0), await header(finalized)
    if request.start < genesis.timestamp:
        raise BlockweaverError("RANGE_PRE_GENESIS", "Requested time range begins before chain genesis")
    if request.end > latest.timestamp:
        raise BlockweaverError("RANGE_UNFINALIZED", "Requested time range extends beyond the finalized chain")
    first = await _lower_bound_timestamp(request.start, finalized, header)
    after = await _lower_bound_timestamp(request.end + 1, finalized, header)
    last = finalized if after == finalized and (await header(finalized)).timestamp <= request.end else after - 1
    first_header, last_header = await header(first), await header(last)
    if first > last or first_header.timestamp > request.end:
        raise BlockweaverError("RANGE_EMPTY", "Requested time range contains no blocks")
    return ResolvedRange(first, last, first_header.timestamp, last_header.timestamp)


async def _lower_bound_timestamp(target: int, high: int, header: Callable[[int], Awaitable[Header]]) -> int:
    low = 0
    while low < high:
        middle = (low + high) // 2
        if (await header(middle)).timestamp < target:
            low = middle + 1
        else:
            high = middle
    return low


async def _verify_time_boundaries(
    request: RequestedRange,
    resolved: ResolvedRange,
    primary: Rpc,
    verifier: Rpc,
    verifier_finalized: int,
) -> None:
    boundary_numbers = sorted({resolved.first_block, resolved.last_block})
    primary_boundaries, verifier_boundaries = await _paired(
        primary.headers(boundary_numbers, _INTEGRITY_PLAN),
        verifier.headers(boundary_numbers, _INTEGRITY_PLAN),
    )
    if any(not _same_core(left, right) for left, right in zip(primary_boundaries, verifier_boundaries, strict=True)):
        raise BlockweaverError("RPC_MISMATCH", "RPC endpoints disagree on a resolved time boundary")
    adjacent_numbers = []
    if resolved.first_block > 0:
        adjacent_numbers.append(resolved.first_block - 1)
    if resolved.last_block < verifier_finalized:
        adjacent_numbers.append(resolved.last_block + 1)
    adjacent = {header.block_number: header for header in await verifier.headers(adjacent_numbers, _INTEGRITY_PLAN)}
    first, last = verifier_boundaries[0], verifier_boundaries[-1]
    if (
        first.timestamp != resolved.first_timestamp
        or last.timestamp != resolved.last_timestamp
        or first.timestamp < request.start
        or last.timestamp > request.end
        or (resolved.first_block > 0 and adjacent[resolved.first_block - 1].timestamp >= request.start)
        or (resolved.last_block < verifier_finalized and adjacent[resolved.last_block + 1].timestamp <= request.end)
    ):
        raise BlockweaverError("RPC_MISMATCH", "Verifier RPC does not prove the resolved time-range edges")


async def _prove_finality(target: Header, verifier: Rpc, request: DownloadRequest) -> tuple[Anchor, Header]:
    verifier_target = await verifier.header(target.block_number, plan_features(list(target.values)))
    if not _same_header(target, verifier_target):
        raise BlockweaverError("RPC_MISMATCH", "RPC endpoints disagree on the target block")
    tagged = await verifier.tagged_header(request.chain.finality_tag, _INTEGRITY_PLAN)
    if tagged.block_number < target.block_number:
        raise BlockweaverError("RANGE_UNFINALIZED", "Verifier finality head does not cover the target")
    await _connect_ancestry(verifier_target, tagged, verifier)
    return Anchor(tagged.block_number, tagged.block_hash, request.chain.finality_tag), verifier_target


def _same_header(left: Header, right: Header) -> bool:
    return (left.block_number, left.block_hash, left.parent_hash, left.timestamp, left.values) == (
        right.block_number,
        right.block_hash,
        right.parent_hash,
        right.timestamp,
        right.values,
    )


async def _refresh_finality(target: Header, anchor: Anchor, rpc: Rpc) -> Anchor:
    stored = await rpc.header(anchor.block_number, _INTEGRITY_PLAN)
    if stored.block_hash != anchor.block_hash:
        raise BlockweaverError("RPC_MISMATCH", "Stored finalized anchor no longer matches RPC")
    await _connect_ancestry(target, stored, rpc)
    fresh = await rpc.tagged_header(anchor.tag, _INTEGRITY_PLAN)
    if fresh.block_number < stored.block_number:
        raise BlockweaverError("RPC_MISMATCH", "RPC finality head regressed behind the stored anchor")
    await _connect_ancestry(stored, fresh, rpc)
    return Anchor(fresh.block_number, fresh.block_hash, anchor.tag)


async def _connect_ancestry(previous: Header, tagged: Header, rpc: Rpc) -> None:
    cursor = previous.block_number + 1
    while cursor <= tagged.block_number:
        last = min(cursor + CHUNK_SIZE - 1, tagged.block_number)
        segment = await rpc.headers(range(cursor, last + 1), _INTEGRITY_PLAN)
        try:
            validate_links(segment, previous)
        except ValueError as error:
            raise BlockweaverError("RPC_MISMATCH", str(error)) from None
        previous = segment[-1]
        cursor = last + 1
    reread = await rpc.header(tagged.block_number, _INTEGRITY_PLAN)
    if not _same_header(tagged, reread) or not _same_core(previous, tagged):
        raise BlockweaverError("RPC_MISMATCH", "Finality tag did not survive numbered reread")


def _same_core(left: Header, right: Header) -> bool:
    return (left.block_number, left.block_hash, left.parent_hash, left.timestamp) == (
        right.block_number,
        right.block_hash,
        right.parent_hash,
        right.timestamp,
    )


async def _check_rows(
    local: dict[int, dict[str, Value]],
    numbers: list[int],
    plan: Plan,
    rpc: Rpc,
    *,
    previous: Header | None = None,
) -> Header | None:
    contiguous = numbers == list(range(numbers[0], numbers[-1] + 1))
    if contiguous:
        headers, rows = await rpc.rows(numbers[0], numbers[-1], plan)
        try:
            validate_links(headers, previous)
        except ValueError as error:
            raise BlockweaverError("RPC_MISMATCH", str(error)) from None
    else:
        headers = await rpc.headers(numbers, plan)
        rows = []
        for header in headers:
            fees = (await rpc.fee_history(header.block_number, header.block_number, plan.percentiles))[0] if plan.percentiles else None
            rows.append(header.row(plan, fees))
    for number, row in zip(numbers, rows, strict=True):
        if local[number] != row:
            raise BlockweaverError("RPC_MISMATCH", f"Dataset row {number} does not match verifier RPC")
    return headers[-1] if contiguous else None


async def _check_full_dataset(dataset: Dataset, rpc: Rpc) -> None:
    previous: Header | None = None
    expected = dataset.first_block
    for local in dataset._fact_chunks(CHUNK_SIZE):
        numbers = list(local)
        if not numbers or numbers[0] != expected:
            raise BlockweaverError("ARTIFACT_INVALID", "Dataset streaming order changed during verification")
        previous = await _check_rows(local, numbers, dataset._plan, rpc, previous=previous)
        expected = numbers[-1] + 1
    if expected != dataset.last_block + 1:
        raise BlockweaverError("ARTIFACT_INVALID", "Dataset streaming coverage changed during verification")


async def _candidate_target(dataset: Dataset, verifier: Rpc, request: DownloadRequest) -> Header:
    if dataset.chain_id != request.chain.chain_id:
        raise BlockweaverError("RESUME_MISMATCH", "Ready candidate chain does not match the request")
    target = await verifier.header(dataset.last_block, dataset._plan)
    if target.block_hash != dataset._target_hash:
        raise BlockweaverError("RPC_MISMATCH", "Ready candidate target hash does not match verifier RPC")
    return target


async def _finish_candidate_validation(dataset: Dataset, target: Header, verifier: Rpc) -> None:
    await _refresh_finality(target, dataset._anchor, verifier)
    samples = sample_numbers(dataset.dataset_id, dataset.first_block, dataset.last_block)
    await _check_rows(dataset._facts(samples), samples, dataset._plan, verifier)


def sample_numbers(dataset_id: UUID, first_block: int, last_block: int) -> list[int]:
    selected = {first_block, last_block}
    available = last_block - first_block - 1
    if available > 0:
        seed = int.from_bytes(hashlib.sha256(dataset_id.bytes).digest()[:8], "big")
        for offset in range(min(3, available)):
            selected.add(first_block + 1 + (seed + offset) % available)
    return sorted(selected)


def _prepare_bigquery(
    warehouse: BigQueryClient,
    dataset: str,
    query: BigQueryPlan,
    parameters: dict[str, int],
    maximum_bytes_billed: int,
) -> int:
    for table, expected in query.table_fields.items():
        actual = warehouse.table_schema(dataset, table)
        if any(field not in actual or not _compatible_bigquery_type(actual[field], dtype) for field, dtype in expected.items()):
            raise BlockweaverError("SOURCE_FEATURE_UNAVAILABLE", f"Configured BigQuery dataset does not support the selected features in {table}")
    bytes_processed, schema = warehouse.dry_run(query.sql, parameters)
    if set(schema) != set(query.result_schema) or any(not _compatible_bigquery_type(schema[name], dtype) for name, dtype in query.result_schema.items()):
        raise BlockweaverError("BIGQUERY_SCHEMA_INVALID", "BigQuery dry-run result schema does not match the selected feature plan")
    if bytes_processed > maximum_bytes_billed:
        raise BlockweaverError("BIGQUERY_COST_LIMIT", "BigQuery dry run exceeds maximum_bytes_billed")
    return bytes_processed


def _compatible_bigquery_type(actual: tuple[str, str], expected: str) -> bool:
    dtype, mode = actual
    return mode in {"NULLABLE", "REQUIRED"} and (dtype == expected or {dtype, expected} <= {"INTEGER", "INT64"})


def _bigquery_chunks(
    warehouse: BigQueryClient,
    query: BigQueryPlan,
    parameters: dict[str, int],
    maximum_bytes_billed: int,
    first_block: int,
    last_block: int,
    plan: Plan,
) -> Iterator[tuple[list[Header], list[dict[str, Value]]]]:
    headers: list[Header] = []
    rows: list[dict[str, Value]] = []
    expected = first_block
    for page in warehouse.pages(query.sql, parameters, maximum_bytes_billed, CHUNK_SIZE):
        for value in page:
            header, row = _parse_bigquery_row(value, query.result_schema, plan, expected)
            headers.append(header)
            rows.append(row)
            expected += 1
            if len(headers) == CHUNK_SIZE:
                yield headers, rows
                headers, rows = [], []
    if headers:
        yield headers, rows
    if expected != last_block + 1:
        raise BlockweaverError("BIGQUERY_INVALID", "BigQuery did not return the exact contiguous requested range")


def _parse_bigquery_row(value: Mapping[str, object], schema: dict[str, str], plan: Plan, expected: int) -> tuple[Header, dict[str, Value]]:
    if set(value) != set(schema):
        raise BlockweaverError("BIGQUERY_INVALID", "BigQuery returned a noncanonical row shape")
    number = _bigquery_int(value["block_number"], "block_number")
    if number != expected:
        raise BlockweaverError("BIGQUERY_INVALID", "BigQuery did not return the exact contiguous requested range")
    timestamp = _bigquery_int(value["_proof_timestamp"], "timestamp")
    header = Header(number, block_hash(value["_proof_hash"], "block hash"), block_hash(value["_proof_parent_hash"], "parent hash"), timestamp, {})
    if "_proof_gas_limit" in schema:
        gas_used = _bigquery_int(value["_proof_gas_used"], "gas used")
        gas_limit = _bigquery_int(value["_proof_gas_limit"], "gas limit")
        if gas_limit == 0 or gas_used > gas_limit:
            raise BlockweaverError("BIGQUERY_INVALID", f"BigQuery returned an invalid gas domain for block {number}")
    if plan.percentiles and _bigquery_int(value["_receipt_gas_used"], "receipt gas used") != _bigquery_int(value["_proof_gas_used"], "block gas used"):
        raise BlockweaverError("BIGQUERY_INVALID", f"BigQuery receipts are incomplete for block {number}")
    row: dict[str, Value] = {"block_number": number}
    for feature in plan.features:
        raw = value[feature.name]
        row[feature.name] = block_hash(raw, feature.name) if feature.dtype == "UTF-8" else _bigquery_int(raw, feature.name)
    return header, row


def _bigquery_int(value: object, label: str) -> int:
    if type(value) is not int or value < 0 or value > 2**63 - 1:
        raise BlockweaverError("BIGQUERY_INVALID", f"BigQuery returned an invalid {label}")
    return value
