"""Everything checked against RPC: range resolution, finality, row samples and the verify command."""

from __future__ import annotations

import hashlib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

from ._config import Provider
from ._contract import Anchor, BlockweaverError, Header, Plan, Progress, RequestedRange, ResolvedRange, Value, validate_links
from ._dataset import Artifact, read_artifact
from ._rpc import CHUNK_SIZE, Rpc


@dataclass(frozen=True, slots=True)
class RangeResolution:
    value: ResolvedRange
    headers: tuple[Header, ...]


async def resolve_range(request: RequestedRange, rpc: Rpc, finalized: int) -> RangeResolution:
    if request.kind == "block":
        if request.end > finalized:
            raise BlockweaverError("RANGE_UNFINALIZED", "Requested block range is not fully finalized")
        numbers = sorted({request.start, request.end})
        boundaries = await rpc.headers(numbers)
        return RangeResolution(
            ResolvedRange(request.start, request.end, boundaries[0].timestamp, boundaries[-1].timestamp),
            tuple(boundaries),
        )
    cache: dict[int, Header] = {}

    async def header(number: int) -> Header:
        if number not in cache:
            cache[number] = await rpc.header(number)
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
    return RangeResolution(ResolvedRange(first, last, first_header.timestamp, last_header.timestamp), tuple(cache.values()))


async def _lower_bound_timestamp(target: int, high: int, header: Callable[[int], Awaitable[Header]]) -> int:
    low = 0
    while low < high:
        middle = (low + high) // 2
        if (await header(middle)).timestamp < target:
            low = middle + 1
        else:
            high = middle
    return low


async def verify_time_boundaries(
    request: RequestedRange,
    resolution: RangeResolution,
    verifier: Rpc,
    verifier_finalized: int,
    *,
    independent: bool,
) -> None:
    resolved = resolution.value
    boundary_numbers = sorted({resolved.first_block, resolved.last_block})
    known = {header.block_number: header for header in resolution.headers}
    if independent:
        verifier_boundaries = await verifier.headers(boundary_numbers)
        primary_boundaries = [known[number] for number in boundary_numbers]
        if primary_boundaries != verifier_boundaries:
            raise BlockweaverError("RPC_MISMATCH", "RPC endpoints disagree on a resolved time boundary")
        known = {header.block_number: header for header in verifier_boundaries}
    adjacent_numbers = []
    if resolved.first_block > 0:
        adjacent_numbers.append(resolved.first_block - 1)
    if resolved.last_block < verifier_finalized:
        adjacent_numbers.append(resolved.last_block + 1)
    missing = [number for number in adjacent_numbers if number not in known]
    if missing:
        known.update((header.block_number, header) for header in await verifier.headers(missing))
    first, last = known[resolved.first_block], known[resolved.last_block]
    if (
        first.timestamp != resolved.first_timestamp
        or last.timestamp != resolved.last_timestamp
        or first.timestamp < request.start
        or last.timestamp > request.end
        or (resolved.first_block > 0 and known[resolved.first_block - 1].timestamp >= request.start)
        or (resolved.last_block < verifier_finalized and known[resolved.last_block + 1].timestamp <= request.end)
    ):
        raise BlockweaverError("RPC_MISMATCH", "Verifier RPC does not prove the resolved time-range edges")


async def prove_target(number: int, block_hash: str, verifier: Rpc, tag: str) -> Anchor:
    """The verifier holds the same target block and has finalized at or past it. Returns its finality head."""
    if (await verifier.header(number)).block_hash != block_hash:
        raise BlockweaverError("RPC_MISMATCH", "Verifier RPC disagrees on the target block hash")
    head = await verifier.tagged_header(tag)
    if head.block_number < number:
        raise BlockweaverError("RANGE_UNFINALIZED", "Verifier finality head does not cover the target")
    return Anchor(head.block_number, head.block_hash, tag)


async def check_artifact(artifact: Artifact, rpc: Rpc, *, full: bool = False) -> tuple[Anchor, list[int]]:
    """Check a validated artifact against one provider at a cost that does not depend on the chain head.

    The provider must hold the target hash, have finalized at or past the stored anchor, still hold the anchor block
    under its number, and return the same rows: the deterministic samples, or every row when `full` is set.
    """
    dataset, anchor = artifact.dataset, artifact.anchor
    fresh = await prove_target(dataset.last_block, artifact.target_hash, rpc, anchor.tag)
    if fresh.block_number < anchor.block_number:
        raise BlockweaverError("RPC_MISMATCH", "RPC finality head is behind the stored anchor")
    if (await rpc.header(anchor.block_number)).block_hash != anchor.block_hash:
        raise BlockweaverError("RPC_MISMATCH", "Stored finalized anchor no longer matches RPC")
    samples = sample_numbers(dataset.dataset_id, dataset.first_block, dataset.last_block)
    if full:
        await _check_full_dataset(artifact, rpc)
    else:
        await _check_rows(artifact.rows(samples), samples, artifact.plan, rpc)
    return fresh, samples


async def _check_rows(
    local: dict[int, dict[str, Value]],
    numbers: list[int],
    plan: Plan,
    rpc: Rpc,
    *,
    previous: Header | None = None,
) -> Header | None:
    rows = await rpc.rows(numbers, plan)
    contiguous = numbers[-1] - numbers[0] + 1 == len(numbers)
    if contiguous:
        try:
            validate_links([Header.of(row) for row in rows], previous)
        except ValueError as error:
            raise BlockweaverError("RPC_MISMATCH", str(error)) from None
    for number, row in zip(numbers, rows, strict=True):
        if local[number] != plan.export(row):
            raise BlockweaverError("RPC_MISMATCH", f"Dataset row {number} does not match verifier RPC")
    return Header.of(rows[-1]) if contiguous else None


async def _check_full_dataset(artifact: Artifact, rpc: Rpc) -> None:
    previous: Header | None = None
    expected = artifact.dataset.first_block
    for local in artifact.row_chunks(CHUNK_SIZE):
        numbers = list(local)
        if not numbers or numbers[0] != expected:
            raise BlockweaverError("ARTIFACT_INVALID", "Dataset streaming order changed during verification")
        previous = await _check_rows(local, numbers, artifact.plan, rpc, previous=previous)
        expected = numbers[-1] + 1
    if expected != artifact.dataset.last_block + 1:
        raise BlockweaverError("ARTIFACT_INVALID", "Dataset streaming coverage changed during verification")


def sample_numbers(dataset_id: UUID, first_block: int, last_block: int) -> list[int]:
    selected = {first_block, last_block}
    available = last_block - first_block - 1
    if available > 0:
        seed = int.from_bytes(hashlib.sha256(dataset_id.bytes).digest()[:8], "big")
        for offset in range(min(3, available)):
            selected.add(first_block + 1 + (seed + offset) % available)
    return sorted(selected)


async def verify_rpc(artifact: Artifact, provider: Provider, full: bool) -> dict[str, object]:
    dataset = artifact.dataset
    try:
        async with Rpc(provider) as rpc:
            chain_id = await rpc.chain_id()
            if chain_id != dataset.chain_id:
                raise BlockweaverError("RPC_CHAIN_MISMATCH", "RPC chain ID does not match the dataset")
            fresh, samples = await check_artifact(artifact, rpc, full=full)
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


async def verify_dataset(
    path: Path,
    *,
    provider: Provider | None,
    full_rpc: bool,
    progress: Progress,
) -> dict[str, object]:
    artifact = read_artifact(path, work=False)
    dataset = artifact.dataset
    progress({"event": "local_valid", "rows": dataset.row_count})
    verification: dict[str, object] = {"mode": "local"}
    if provider is not None:
        verification = await verify_rpc(artifact, provider, full_rpc)
        artifact.assert_unchanged()
    return {
        "operation": "verify",
        "dataset_id": str(dataset.dataset_id),
        "path": str(dataset.path),
        "rows": dataset.row_count,
        "artifact_sha256": artifact.fingerprint.hashes(),
        "verification": verification,
    }
