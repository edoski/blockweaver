"""Bounded asynchronous EVM JSON-RPC transport."""

from __future__ import annotations

import asyncio
import random
from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import aiohttp

from ._contract import Header, Plan, parse_header, quantity

_TRANSIENT_HTTP = {408, 425, 429, *range(500, 600)}
Validator = Callable[[Any], Any]


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
