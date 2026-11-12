from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from support import DATASET_ID, ChainServer, download_args, error, invoke

from blockweaver import BlockweaverError, _sources
from blockweaver._contract import Chain, Provider, RpcDownloadRequest, plan_features, requested_range


@pytest.mark.parametrize(
    ("code", "retries"),
    [(-32601, False), (-32000, False), (-32603, True), (-32002, True), (-32042, True)],
)
def test_rpc_error_disposition_uses_numeric_codes_without_provider_messages(chains: tuple[ChainServer, ChainServer], code: int, retries: bool) -> None:
    primary, _verifier = chains
    primary.rpc_errors = [code]

    async def request() -> list[_sources.Header]:
        async with _sources.Rpc(primary.url, batch_size=3, concurrency=2, timeout=2) as rpc:
            return await rpc.headers([10], plan_features([]))

    if retries:
        assert [header.block_number for header in asyncio.run(request())] == [10]
        assert len(primary.requests) == 2
    else:
        with pytest.raises(RuntimeError, match="rejected") as failure:
            asyncio.run(request())
        assert "provider detail" not in str(failure.value)
        assert len(primary.requests) == 1


def test_rpc_limit_splits_immediately_and_retries_only_pending_calls(chains: tuple[ChainServer, ChainServer]) -> None:
    primary, _verifier = chains
    primary.limit_batches = True

    async def request() -> list[_sources.Header]:
        async with _sources.Rpc(primary.url, batch_size=3, concurrency=2, timeout=2) as rpc:
            return await rpc.headers([10, 11, 12], plan_features([]))

    assert [header.block_number for header in asyncio.run(request())] == [10, 11, 12]
    batch_sizes = [len(batch) for batch in primary.requests]
    assert batch_sizes[0] == 3 and all(size < 3 for size in batch_sizes[1:])
    primary.requests.clear()
    primary.limit_batches = False
    primary.item_errors[11] = [-32603]
    assert [header.block_number for header in asyncio.run(request())] == [10, 11, 12]
    assert [[call["params"][0] for call in batch] for batch in primary.requests] == [["0xa", "0xb", "0xc"], ["0xb"]]
    primary.requests.clear()
    primary.rpc_errors = [-32005]
    with pytest.raises(RuntimeError, match="limit exceeded"):
        asyncio.run(request())
    assert len(primary.requests) == 1


def test_rpc_retries_preserve_original_pending_order(chains: tuple[ChainServer, ChainServer]) -> None:
    primary, _verifier = chains
    for number in (11, 19, 27):
        primary.item_errors[number] = [-32603, -32603]

    async def request() -> list[_sources.Header]:
        async with _sources.Rpc(primary.url, batch_size=18, concurrency=1, timeout=2) as rpc:
            return await rpc.headers(range(10, 28), plan_features([]))

    assert [header.block_number for header in asyncio.run(request())] == list(range(10, 28))
    assert [[call["params"][0] for call in batch] for batch in primary.requests[1:]] == [
        ["0xb", "0x13", "0x1b"],
        ["0xb", "0x13", "0x1b"],
    ]


def test_rpc_retry_is_bounded_and_retry_after_is_validated(chains: tuple[ChainServer, ChainServer], monkeypatch: pytest.MonkeyPatch) -> None:
    primary, _verifier = chains
    delays: list[float] = []

    async def sleep(delay: float) -> None:
        delays.append(delay)

    async def request() -> None:
        async with _sources.Rpc(primary.url, batch_size=1, concurrency=1, timeout=2) as rpc:
            await rpc.headers([10], plan_features([]))

    monkeypatch.setattr(_sources.asyncio, "sleep", sleep)
    monkeypatch.setattr(_sources.random, "uniform", lambda _start, _end: 0.25)
    primary.http_failures, primary.retry_after = 1, "999"
    asyncio.run(request())
    assert delays == [60.0]
    delays.clear()
    primary.http_failures, primary.retry_after = 1, "-1"
    asyncio.run(request())
    assert delays == [0.25]
    delays.clear()
    primary.rpc_errors = [-32603] * 12
    with pytest.raises(RuntimeError, match="12 attempts") as failure:
        asyncio.run(request())
    assert "provider detail" not in str(failure.value)
    assert len(delays) == 11


def test_rpc_groups_are_concurrent_and_cancel_siblings(chains: tuple[ChainServer, ChainServer], monkeypatch: pytest.MonkeyPatch) -> None:
    primary, _verifier = chains

    async def exercise() -> tuple[int, bool]:
        rpc = _sources.Rpc("http://unused", batch_size=1, concurrency=3, timeout=2)
        active = maximum = started = 0
        release = asyncio.Event()

        async def concurrent(calls: list[dict[str, Any]]) -> tuple[int, Any, float | None]:
            nonlocal active, maximum, started
            active += 1
            started += 1
            maximum = max(maximum, active)
            if started == 3:
                release.set()
            await release.wait()
            call = calls[0]
            number = int(call["params"][0], 16)
            active -= 1
            return 200, [{"jsonrpc": "2.0", "id": call["id"], "result": primary.block(number)}], None

        monkeypatch.setattr(rpc, "_post", concurrent)
        assert [item.block_number for item in await rpc.headers([10, 11, 12], plan_features([]))] == [10, 11, 12]

        cancelled = False
        ready = asyncio.Event()
        waiting = 0

        async def fail_one(calls: list[dict[str, Any]]) -> tuple[int, Any, float | None]:
            nonlocal cancelled, waiting
            waiting += 1
            if waiting == 2:
                ready.set()
            await ready.wait()
            if calls[0]["params"][0] == "0xa":
                raise ValueError("bad sibling")
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled = True
                raise
            raise AssertionError("unreachable")

        monkeypatch.setattr(rpc, "_post", fail_one)
        with pytest.raises(ValueError, match="bad sibling"):
            await rpc.headers([10, 11], plan_features([]))
        return maximum, cancelled

    maximum, cancelled = asyncio.run(exercise())
    assert maximum == 3 and cancelled


def test_paired_provider_failure_cancels_and_awaits_sibling(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary, verifier = chains
    request = RpcDownloadRequest(
        dataset_id=UUID(DATASET_ID),
        chain=Chain("test", 1, "finalized", None, None, None),
        requested_range=requested_range(10, 14, None, None),
        plan=plan_features(["timestamp"]),
        output_root=tmp_path,
        output_format="parquet",
        primary=Provider("primary", primary.url, 3, 2, 2),
        verifier=Provider("verifier", verifier.url, 3, 2, 2),
    )
    ready: asyncio.Event | None = None
    started = 0
    sibling_cancelled = False

    async def chain_id(rpc: _sources.Rpc) -> int:
        nonlocal ready, sibling_cancelled, started
        if ready is None:
            ready = asyncio.Event()
        current = ready
        started += 1
        if started == 2:
            current.set()
        await current.wait()
        if rpc._url == primary.url:
            raise RuntimeError("primary failed")
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            sibling_cancelled = True
            raise
        raise AssertionError("unreachable")

    monkeypatch.setattr(_sources.Rpc, "chain_id", chain_id)

    async def exercise() -> tuple[bool, int]:
        with pytest.raises(BlockweaverError, match="primary failed"):
            await _sources.download(request, progress=lambda _event: None, publication=lambda _phase: None)
        current = asyncio.current_task()
        pending = [task for task in asyncio.all_tasks() if task is not current and not task.done()]
        cancelled_before_cleanup = sibling_cancelled
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        return cancelled_before_cleanup, len(pending)

    assert asyncio.run(exercise()) == (True, 0)


def test_rpc_protocol_failure_is_bounded_and_machine_readable(tmp_path: Path, chains: tuple[ChainServer, ChainServer], make_config: Any) -> None:
    primary, verifier = chains
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out")
    primary.wrong_id_once = True
    failure = error(invoke(download_args(config)))
    assert failure["code"] == "RPC_INVALID"
    assert "response ID mismatch" in failure["message"]
