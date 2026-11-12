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
    """Decode the header keys a plan acquires from one eth_getBlockByNumber result."""
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
            raise ValueError(f"Block {number} response is missing {key}")
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
        groups = [calls[index : index + self.provider.batch_size] for index in range(0, len(calls), self.provider.batch_size)]
        replies: dict[int, Any] = {}
        for part in await self._run_groups(groups, validators):
            replies.update(part)
        return [replies[int(call["id"])] for call in calls]

    async def headers(self, numbers: Sequence[int]) -> list[Header]:
        return [Header.of(_INTEGRITY_PLAN.row(block)) for block in await self._blocks(numbers, _INTEGRITY_PLAN)]

    async def header(self, number: int) -> Header:
        return (await self.headers([number]))[0]

    async def tagged_header(self, tag: str) -> Header:
        call = self._calls("eth_getBlockByNumber", [[tag, False]])[0]
        item_id = int(call["id"])
        reply = await self._run([call], {item_id: lambda value: parse_block(value, expected=None, plan=_INTEGRITY_PLAN)})
        return Header.of(_INTEGRITY_PLAN.row(reply[item_id]))

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
        return [plan.row({**block, **{name: fee[percentile] for name, percentile in rewards}}) for block, fee in zip(blocks, fees, strict=True)]

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
            async with self._semaphore, self._session.post(self.provider.url, json=calls) as response:
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
