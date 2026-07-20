"""Direct asynchronous JSON-RPC transport."""

from __future__ import annotations

import asyncio
import random
from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import aiohttp

from ._contract import Block, parse_block, quantity

_TRANSIENT_HTTP = {408, 425, 429, *range(500, 600)}
Validator = Callable[[Any], Any]


class Rpc:
    def __init__(self, url: str, *, batch_size: int, concurrency: int) -> None:
        self._url = url
        self._batch_size = batch_size
        self._semaphore = asyncio.Semaphore(concurrency)
        self._session: aiohttp.ClientSession | None = None
        self._next_id = 1

    async def __aenter__(self) -> Rpc:
        timeout = aiohttp.ClientTimeout(total=30)
        self._session = aiohttp.ClientSession(timeout=timeout)
        return self

    async def __aexit__(self, *_args: object) -> None:
        assert self._session is not None
        await self._session.close()

    def _calls(self, method: str, parameters: Iterable[list[object]]) -> list[dict[str, Any]]:
        calls = []
        for params in parameters:
            calls.append({"jsonrpc": "2.0", "id": self._next_id, "method": method, "params": params})
            self._next_id += 1
        return calls

    async def chain_id(self) -> int:
        call = self._calls("eth_chainId", [[]])[0]
        item_id = call["id"]
        result = await self._run([call], {item_id: lambda value: quantity(value, "chain id")})
        return result[item_id]

    async def blocks(self, numbers: Iterable[int], *, chain_id: int) -> list[Block]:
        ordered = list(numbers)
        calls = self._calls("eth_getBlockByNumber", [[hex(number), False] for number in ordered])
        validators = {
            call["id"]: lambda value, expected=number: parse_block(value, expected=expected, chain_id=chain_id)
            for call, number in zip(calls, ordered, strict=True)
        }
        groups = [calls[index : index + self._batch_size] for index in range(0, len(calls), self._batch_size)]
        replies: dict[int, Any] = {}
        for part in await self._run_groups(groups, validators):
            replies.update(part)
        return [replies[call["id"]] for call in calls]

    async def finalized_block(self, *, chain_id: int) -> Block:
        call = self._calls("eth_getBlockByNumber", [["finalized", False]])[0]
        item_id = call["id"]

        def validate(value: Any) -> Block:
            if not isinstance(value, dict):
                raise ValueError("Invalid tagged block response shape")
            expected = quantity(value.get("number"), "block number")
            return parse_block(value, expected=expected, chain_id=chain_id)

        reply = await self._run([call], {item_id: validate})
        return reply[item_id]

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
                first_half, second_half = await self._run_groups([items[:midpoint], items[midpoint:]], validators, attempt)
                complete.update(first_half)
                complete.update(second_half)
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
        tasks = [asyncio.create_task(self._run(group, validators, prior_attempts)) for group in groups]
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
