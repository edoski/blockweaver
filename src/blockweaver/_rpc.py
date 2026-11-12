"""Bounded JSON-RPC client for one provider."""

from __future__ import annotations

import asyncio
import random
import re
from collections.abc import Awaitable, Callable, Iterable, Sequence
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from math import isfinite
from typing import Any, TypeVar

import aiohttp

from ._config import Provider
from ._contract import (
    INT64_MAX,
    BlockweaverError,
    FeeHistory,
    Header,
    HeaderKey,
    Plan,
    Row,
    Value,
    plan_features,
)

_QUANTITY = re.compile(r"0x(?:0|[1-9a-f][0-9a-f]*)\Z")


_TRANSIENT_HTTP = {408, 425, 429, *range(500, 600)}
_REFUSED_HTTP = {400, 413}
_ATTEMPTS = 10
_UNREACHABLE_ATTEMPTS = 3
_BACKOFF_CAP = 30.0


_FATAL_RPC = {-32700, -32600, -32601, -32602, -32000, -32001, -32003, -32004, -32006}


_LIMIT_RPC = -32005


CHUNK_SIZE = 1024


Validator = Callable[[Any], Any]


_Left = TypeVar("_Left")


_Right = TypeVar("_Right")


_INTEGRITY_PLAN = plan_features([])


async def paired(left: Awaitable[_Left], right: Awaitable[_Right]) -> tuple[_Left, _Right]:
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


def quantity(value: Any, label: str) -> int:
    if not isinstance(value, str) or _QUANTITY.fullmatch(value) is None:
        raise ValueError(f"Invalid {label} quantity")
    parsed = int(value, 16)
    if parsed > INT64_MAX:
        raise ValueError(f"{label} quantity exceeds signed Int64")
    return parsed


def parse_block(value: Any, *, expected: int | None, plan: Plan) -> dict[str, Value]:
    """Decode the header keys a plan acquires from one eth_getBlockByNumber result. A key the block lacks is left out."""
    if not isinstance(value, dict):
        raise ValueError("Invalid block response shape")
    number = quantity(value.get("number"), "block number")
    if expected is not None and number != expected:
        raise ValueError(f"RPC returned block {number} when {expected} was requested")
    block: dict[str, Value] = {"block_number": number}
    for feature in plan.acquired:
        if not isinstance(feature.rpc, HeaderKey):
            continue
        key, kind = feature.rpc.key, feature.rpc.kind
        if key not in value:
            continue
        raw = value[key]
        if kind == "quantity":
            block[feature.name] = quantity(raw, f"block {number} {key}")
        elif kind == "count":
            if not isinstance(raw, list):
                raise ValueError(f"Block {number} has an invalid {key} list")
            block[feature.name] = len(raw)
        else:
            block[feature.name] = raw
    return block


class Rpc:
    def __init__(self, provider: Provider) -> None:
        self.provider = provider
        self._semaphore = asyncio.Semaphore(provider.concurrency)
        self._session: aiohttp.ClientSession | None = None
        self._next_id = 1
        self._batch_size = provider.batch_size
        self._answered = False

    async def __aenter__(self) -> Rpc:
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.provider.timeout))
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

    async def _blocks(self, numbers: Sequence[int], plan: Plan) -> list[dict[str, Value]]:
        calls = self._calls("eth_getBlockByNumber", [[hex(number), False] for number in numbers])
        validators: dict[int, Validator] = {}
        for call, number in zip(calls, numbers, strict=True):
            validators[int(call["id"])] = lambda value, expected=number: parse_block(value, expected=expected, plan=plan)
        groups = [calls[index : index + self._batch_size] for index in range(0, len(calls), self._batch_size)]
        replies: dict[int, Any] = {}
        for part in await self._run_groups(groups, validators):
            replies.update(part)
        return [replies[int(call["id"])] for call in calls]

    async def headers(self, numbers: Sequence[int]) -> list[Header]:
        return [Header.of(self._row(_INTEGRITY_PLAN, block)) for block in await self._blocks(numbers, _INTEGRITY_PLAN)]

    async def header(self, number: int) -> Header:
        return (await self.headers([number]))[0]

    async def tagged_header(self, tag: str) -> Header:
        call = self._calls("eth_getBlockByNumber", [[tag, False]])[0]
        item_id = int(call["id"])
        reply = await self._run([call], {item_id: lambda value: parse_block(value, expected=None, plan=_INTEGRITY_PLAN)})
        return Header.of(self._row(_INTEGRITY_PLAN, reply[item_id]))

    async def fee_history(self, first_block: int, last_block: int, percentiles: tuple[int, ...]) -> list[dict[int, int]]:
        count = last_block - first_block + 1
        call = self._calls("eth_feeHistory", [[hex(count), hex(last_block), list(percentiles)]])[0]
        item_id = int(call["id"])
        reply = await self._run(
            [call],
            {item_id: lambda value: _parse_fee_history(value, first_block, count, percentiles)},
        )
        return reply[item_id]

    async def rows(self, numbers: Sequence[int], plan: Plan) -> list[Row]:
        """Validated rows for ascending block numbers. A contiguous run shares one eth_feeHistory call."""
        if not plan.percentiles:
            blocks = await self._blocks(numbers, plan)
            fees: list[dict[int, int]] = [{} for _ in numbers]
        elif numbers[-1] - numbers[0] + 1 == len(numbers):
            blocks, fees = await paired(self._blocks(numbers, plan), self.fee_history(numbers[0], numbers[-1], plan.percentiles))
        else:
            blocks = await self._blocks(numbers, plan)
            fees = [(await self.fee_history(number, number, plan.percentiles))[0] for number in numbers]
        rewards = [(feature.name, feature.rpc.percentile) for feature in plan.acquired if isinstance(feature.rpc, FeeHistory)]
        return [self._row(plan, {**block, **{name: fee[percentile] for name, percentile in rewards}}) for block, fee in zip(blocks, fees, strict=True)]

    def _row(self, plan: Plan, values: dict[str, Value]) -> Row:
        name, number = self.provider.name, values["block_number"]
        for feature in plan.acquired:
            if feature.name not in values and isinstance(feature.rpc, HeaderKey):
                raise BlockweaverError("SOURCE_FEATURE_UNAVAILABLE", f"RPC provider {name}: block {number} has no header key {feature.rpc.key}")
        try:
            return plan.row(values)
        except ValueError as error:
            raise ValueError(f"RPC provider {self.provider.name}: {error}") from None

    async def _run(self, calls: list[dict[str, Any]], validators: dict[int, Validator]) -> dict[int, Any]:
        """Send one batch until every call has a validated result.

        A transient failure retries the same pending calls after a backoff and never splits them, so a rate limit does
        not multiply requests. Only a provider that refuses the batch as too large is answered with smaller batches.
        """
        pending = {int(call["id"]): call for call in calls}
        complete: dict[int, Any] = {}
        method = calls[0]["method"]
        name = self.provider.name
        attempt, failure = 0, ""
        while True:
            attempt += 1
            status, payload, retry_after = await self._post(list(pending.values()))
            refused = status in _REFUSED_HTTP
            if status == 200:
                try:
                    accepted, refused, failure = _parse_batch(payload, pending, validators)
                except _Rejected as error:
                    raise RuntimeError(f"RPC provider {name} rejected {method} with JSON-RPC error {error}") from None
                except ValueError as error:
                    raise ValueError(f"RPC provider {name} sent an invalid {method} reply: {error}") from None
                complete.update(accepted)
                pending = {item_id: call for item_id, call in pending.items() if item_id not in accepted}
                if not pending:
                    return complete
            elif status is None:
                failure = "no response"
            elif refused or status in _TRANSIENT_HTTP:
                failure = f"HTTP {status}"
            else:
                raise RuntimeError(f"RPC provider {name} answered {method} with non-retryable HTTP status {status}")
            if refused and len(pending) > 1:
                self._batch_size = min(self._batch_size, len(pending) // 2)
                items = list(pending.values())
                for part in await self._run_groups([items[: len(items) // 2], items[len(items) // 2 :]], validators):
                    complete.update(part)
                return complete
            if refused and status != 200:
                raise RuntimeError(f"RPC provider {name} answered a single {method} call with non-retryable HTTP status {status}")
            if attempt >= (_ATTEMPTS if self._answered else _UNREACHABLE_ATTEMPTS):
                raise RuntimeError(f"RPC provider {name} failed {method} after {attempt} attempts: {failure}")
            delay = retry_after if retry_after is not None else random.uniform(0, min(0.5 * 2 ** (attempt - 1), _BACKOFF_CAP))
            await asyncio.sleep(delay)

    async def _run_groups(self, groups: Iterable[list[dict[str, Any]]], validators: dict[int, Validator]) -> list[dict[int, Any]]:
        tasks = [asyncio.create_task(self._run(group, validators)) for group in groups if group]
        try:
            return list(await asyncio.gather(*tasks))
        except BaseException:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

    async def _post(self, calls: list[dict[str, Any]]) -> tuple[int | None, Any, float | None]:
        """One HTTP exchange: its status, its JSON payload when the status is 200, and any Retry-After. No response is status None."""
        assert self._session is not None
        try:
            async with self._semaphore, self._session.post(self.provider.url, json=calls) as response:
                self._answered = True
                if response.status != 200:
                    await response.read()
                    return response.status, None, _retry_after(response.headers.get("Retry-After"))
                try:
                    return 200, await response.json(content_type=None), None
                except (ValueError, aiohttp.ClientError) as error:
                    raise RuntimeError(f"RPC provider {self.provider.name} returned invalid JSON") from error
        except (TimeoutError, aiohttp.ClientError):
            return None, None, None


class _Rejected(Exception):
    """A JSON-RPC error code that retrying cannot cure."""


def _parse_batch(payload: Any, pending: dict[int, dict[str, Any]], validators: dict[int, Validator]) -> tuple[dict[int, Any], bool, str]:
    """Validated results by call ID, whether the provider refused the batch as too large, and why calls are still pending."""
    if isinstance(payload, dict) and payload.get("id") is None and "error" in payload:
        return {}, True, f"JSON-RPC error {_error_code(payload)} for the whole batch"
    if not isinstance(payload, list):
        raise ValueError("Invalid JSON-RPC batch response shape")
    accepted: dict[int, Any] = {}
    seen: set[int] = set()
    refused = False
    failure = "missing result"
    for member in payload:
        if not isinstance(member, dict) or member.get("jsonrpc") != "2.0":
            raise ValueError("Invalid JSON-RPC response member")
        item_id = member.get("id")
        if type(item_id) is not int or item_id not in pending or item_id in seen:
            raise ValueError("JSON-RPC response ID mismatch")
        seen.add(item_id)
        if ("result" in member) == ("error" in member):
            raise ValueError("Invalid JSON-RPC result shape")
        if "error" in member:
            code = _error_code(member)
            if code in _FATAL_RPC:
                raise _Rejected(code)
            refused = refused or code == _LIMIT_RPC
            failure = f"JSON-RPC error {code}"
        elif member["result"] is None:
            failure = "null result"
        else:
            accepted[item_id] = validators[item_id](member["result"])
    return accepted, refused, failure


def _error_code(member: dict[str, Any]) -> int:
    error = member["error"]
    if not isinstance(error, dict) or type(error.get("code")) is not int or not isinstance(error.get("message"), str):
        raise ValueError("Invalid JSON-RPC error shape")
    return error["code"]


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
