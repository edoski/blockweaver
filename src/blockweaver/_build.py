"""Download, verify, and atomically publish configured datasets."""

from __future__ import annotations

import hashlib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal
from uuid import UUID

from . import __version__
from ._contract import (
    Anchor,
    BlockweaverError,
    Chain,
    Header,
    OutputFormat,
    Plan,
    Provider,
    RequestedRange,
    ResolvedRange,
    Value,
    plan_features,
    validate_links,
    validate_uuid,
)
from ._corpus import (
    LoadedDataset,
    checkpoint_facts,
    checkpoint_paths,
    dataset_path,
    discard_work,
    load_dataset,
    locked_work,
    pair_hashes,
    prepare_work,
    publish,
    save_ready,
    write_candidate,
    write_checkpoint,
)
from ._rpc import Rpc

Progress = Callable[[dict[str, object]], None]
Publication = Callable[[Literal["publishing", "committed"]], None]
_CHUNK_SIZE = 1024
_INTEGRITY_PLAN = plan_features([])


@dataclass(frozen=True, slots=True)
class DownloadSpec:
    dataset_id: UUID
    chain: Chain
    requested_range: RequestedRange
    plan: Plan
    output_root: Path
    output_format: OutputFormat
    primary: Provider
    verifier: Provider


async def download(spec: DownloadSpec, *, progress: Progress, publication: Publication) -> dict[str, object]:
    validate_uuid(spec.dataset_id)
    if spec.primary.name == spec.verifier.name:
        raise BlockweaverError("PROVIDER_INVALID", "Primary and verifier must be distinct provider profiles")
    if spec.primary.url == spec.verifier.url:
        raise BlockweaverError("PROVIDER_INVALID", "Primary and verifier RPC endpoints must be independent")
    progress({"event": "request", "dataset_id": str(spec.dataset_id)})
    try:
        async with _rpc(spec.primary) as primary, _rpc(spec.verifier) as verifier:
            primary_chain_id, verifier_chain_id = await primary.chain_id(), await verifier.chain_id()
            if primary_chain_id != spec.chain.chain_id or verifier_chain_id != spec.chain.chain_id:
                raise BlockweaverError("RPC_CHAIN_MISMATCH", "RPC chain ID does not match the configured chain")
            primary_tag, verifier_tag = (
                await primary.tagged_header(spec.chain.finality_tag, _INTEGRITY_PLAN),
                await verifier.tagged_header(spec.chain.finality_tag, _INTEGRITY_PLAN),
            )
            resolved = await _resolve_range(spec.requested_range, primary, min(primary_tag.block_number, verifier_tag.block_number))
            destination = dataset_path(spec.output_root.resolve(), spec.chain.name, resolved, spec.dataset_id)
            binding = _binding(spec, resolved)
            with locked_work(spec.output_root.resolve(), spec.dataset_id, destination) as hidden:
                work = prepare_work(hidden, destination, binding)
                candidate_path, receipt = work.candidate, work.receipt
                recovered = candidate_path is not None
                paths: list[Path] = []
                next_block = resolved.first_block
                previous: Header | None = None
                reused = 0
                if candidate_path is None:
                    paths, next_block, previous = checkpoint_paths(
                        work.chunks,
                        first_block=resolved.first_block,
                        last_block=resolved.last_block,
                        size=_CHUNK_SIZE,
                        plan=spec.plan,
                    )
                    reused = next_block - resolved.first_block
                    progress({"event": "resume", "reused_rows": reused})
                if receipt is None:
                    if candidate_path is None:
                        while next_block <= resolved.last_block:
                            last = min(next_block + _CHUNK_SIZE - 1, resolved.last_block)
                            headers, rows = await primary.rows(next_block, last, spec.plan)
                            validate_links(headers, previous)
                            chunk_path = work.chunks / f"{next_block:020d}-{last:020d}.parquet"
                            write_checkpoint(chunk_path, spec.plan, headers, rows)
                            paths.append(chunk_path)
                            previous, next_block = headers[-1], last + 1
                            progress({"event": "checkpoint", "from_block": headers[0].block_number, "to_block": last})
                        assert previous is not None
                        anchor, verifier_target = await _prove_finality(previous, verifier, spec.chain)
                        sample_numbers = _sample_numbers(spec.dataset_id, resolved.first_block, resolved.last_block)
                        local_samples = checkpoint_facts(paths, spec.plan, sample_numbers)
                        await _check_rows(local_samples, sample_numbers, spec.plan, verifier)
                        verification = {
                            "primary_chain_id": primary_chain_id,
                            "verifier_chain_id": verifier_chain_id,
                            "target_agreement": previous == verifier_target,
                            "sampled_blocks": sample_numbers,
                        }
                        manifest = _manifest(spec, resolved, previous.block_hash, anchor, verification)
                        candidate_path = hidden / "ready.tmp"
                        candidate = write_candidate(
                            candidate_path,
                            plan=spec.plan,
                            output_format=spec.output_format,
                            sources=paths,
                            manifest=manifest,
                        )
                        acquired = candidate.rows - reused
                    else:
                        candidate = load_dataset(candidate_path, work=candidate_path.name in {"ready", "ready.tmp"})
                        await _validate_candidate(candidate, primary, verifier, spec.chain)
                        anchor = candidate.anchor
                        acquired, reused = 0, candidate.rows
                    receipt = _receipt("download", candidate, destination, reused, acquired, anchor)
                assert candidate_path is not None and receipt is not None
                if work.published:
                    publication("committed")
                else:
                    save_ready(hidden, candidate_path, receipt)
                    publication("publishing")
                    publish(hidden, destination)
                    publication("committed")
                discard_work(hidden)
                event: dict[str, object] = {"event": "published", "dataset_id": str(spec.dataset_id), "path": str(destination)}
                if recovered:
                    event["recovered"] = True
                progress(event)
                return receipt
    except BlockweaverError:
        raise
    except ValueError as error:
        raise BlockweaverError("RPC_INVALID", str(error)) from None
    except RuntimeError as error:
        raise BlockweaverError("RPC_FAILED", str(error)) from None
    except OSError as error:
        raise BlockweaverError("IO_FAILED", str(error)) from None


async def verify_dataset(
    path: Path,
    *,
    provider: Provider | None,
    full_rpc: bool,
    progress: Progress,
) -> dict[str, object]:
    dataset = load_dataset(path)
    progress({"event": "local_valid", "rows": dataset.rows})
    anchor = dataset.anchor
    verification: dict[str, object] = {"mode": "local"}
    if provider is None:
        if full_rpc:
            raise BlockweaverError("VERIFY_INVALID", "--full-rpc requires a configured provider or --rpc-url")
    else:
        try:
            async with _rpc(provider) as rpc:
                chain_id = await rpc.chain_id()
                if chain_id != dataset.chain_id:
                    raise BlockweaverError("RPC_CHAIN_MISMATCH", "RPC chain ID does not match the dataset")
                target = await rpc.header(dataset.last_block, dataset.plan)
                if target.block_hash != dataset.manifest["target_hash"]:
                    raise BlockweaverError("RPC_MISMATCH", "Dataset target hash does not match RPC")
                fresh = await _refresh_finality(target, anchor, rpc, dataset.chain_id)
                numbers = (
                    list(range(dataset.first_block, dataset.last_block + 1))
                    if full_rpc
                    else _sample_numbers(UUID(dataset.manifest["dataset_id"]), dataset.first_block, dataset.last_block)
                )
                await _check_rows(dataset.facts(numbers), numbers, dataset.plan, rpc, contiguous=full_rpc)
                verification = {
                    "mode": "full_rpc" if full_rpc else "sample_rpc",
                    "provider": provider.name,
                    "chain_id": chain_id,
                    "sampled_blocks": numbers
                    if not full_rpc
                    else _sample_numbers(UUID(dataset.manifest["dataset_id"]), dataset.first_block, dataset.last_block),
                    "finalized_anchor": fresh.document(),
                }
        except BlockweaverError:
            raise
        except ValueError as error:
            raise BlockweaverError("RPC_INVALID", str(error)) from None
        except RuntimeError as error:
            raise BlockweaverError("RPC_FAILED", str(error)) from None
    return {
        "version": 1,
        "operation": "verify",
        "dataset_id": dataset.manifest["dataset_id"],
        "path": str(dataset.path),
        "rows": dataset.rows,
        "artifact_sha256": pair_hashes(dataset.path),
        "verification": verification,
    }


def _rpc(provider: Provider) -> Rpc:
    return Rpc(provider.url, batch_size=provider.batch_size, concurrency=provider.concurrency, timeout=provider.timeout)


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
        value = await header(middle)
        if value.timestamp < target:
            low = middle + 1
        else:
            high = middle
    return low


async def _prove_finality(target: Header, verifier: Rpc, chain: Chain) -> tuple[Anchor, Header]:
    verifier_target = await verifier.header(target.block_number, plan_features_for_header(target))
    if not _same_header(target, verifier_target):
        raise BlockweaverError("RPC_MISMATCH", "RPC endpoints disagree on the target block")
    tagged = await verifier.tagged_header(chain.finality_tag, _INTEGRITY_PLAN)
    if tagged.block_number < target.block_number:
        raise BlockweaverError("RANGE_UNFINALIZED", "Verifier finality head does not cover the target")
    await _connect_ancestry(verifier_target, tagged, verifier)
    return Anchor(tagged.block_number, tagged.block_hash, chain.finality_tag), verifier_target


def plan_features_for_header(header: Header) -> Plan:
    return plan_features(list(header.values))


def _same_header(left: Header, right: Header) -> bool:
    return (
        left.block_number,
        left.block_hash,
        left.parent_hash,
        left.timestamp,
        left.values,
    ) == (
        right.block_number,
        right.block_hash,
        right.parent_hash,
        right.timestamp,
        right.values,
    )


async def _refresh_finality(target: Header, anchor: Anchor, rpc: Rpc, chain_id: int) -> Anchor:
    del chain_id
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
        last = min(cursor + _CHUNK_SIZE - 1, tagged.block_number)
        segment = await rpc.headers(range(cursor, last + 1), _INTEGRITY_PLAN)
        validate_links(segment, previous)
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
    contiguous: bool = False,
) -> None:
    if contiguous:
        headers, rows = await rpc.rows(numbers[0], numbers[-1], plan)
        validate_links(headers)
    else:
        headers = await rpc.headers(numbers, plan)
        rows = []
        for header in headers:
            fees = (await rpc.fee_history(header.block_number, header.block_number, plan.percentiles))[0]
            rows.append(header.row(plan, fees))
    for number, row in zip(numbers, rows, strict=True):
        if local[number] != row:
            raise BlockweaverError("RPC_MISMATCH", f"Dataset row {number} does not match verifier RPC")


async def _validate_candidate(dataset: LoadedDataset, primary: Rpc, verifier: Rpc, chain: Chain) -> None:
    if dataset.chain_id != chain.chain_id:
        raise BlockweaverError("RESUME_MISMATCH", "Ready candidate chain does not match the request")
    target = await primary.header(dataset.last_block, dataset.plan)
    if target.block_hash != dataset.manifest["target_hash"]:
        raise BlockweaverError("RPC_MISMATCH", "Ready candidate target hash does not match primary RPC")
    verifier_target = await verifier.header(dataset.last_block, dataset.plan)
    if not _same_header(target, verifier_target):
        raise BlockweaverError("RPC_MISMATCH", "RPC endpoints disagree on the ready candidate target")
    await _refresh_finality(verifier_target, dataset.anchor, verifier, chain.chain_id)
    numbers = _sample_numbers(UUID(dataset.manifest["dataset_id"]), dataset.first_block, dataset.last_block)
    await _check_rows(dataset.facts(numbers), numbers, dataset.plan, verifier)


def _binding(spec: DownloadSpec, resolved: ResolvedRange) -> dict[str, object]:
    return {
        "version": 1,
        "dataset_id": str(spec.dataset_id),
        "chain": {"name": spec.chain.name, "chain_id": spec.chain.chain_id, "finality_tag": spec.chain.finality_tag},
        "source": {"type": "rpc", "provider": spec.primary.name, "verifier": spec.verifier.name},
        "requested_range": spec.requested_range.document(),
        "resolved_range": resolved.document(),
        "features": list(spec.plan.columns[1:]),
        "format": spec.output_format,
    }


def _manifest(
    spec: DownloadSpec,
    resolved: ResolvedRange,
    target_hash: str,
    anchor: Anchor,
    verification: dict[str, object],
) -> dict[str, object]:
    return {
        "manifest_version": 1,
        "dataset_version": 1,
        "tool_version": __version__,
        "dataset_id": str(spec.dataset_id),
        "completed_at": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "chain": {"name": spec.chain.name, "chain_id": spec.chain.chain_id},
        "source": {"type": "rpc", "provider": spec.primary.name, "verifier": spec.verifier.name},
        "requested_range": spec.requested_range.document(),
        "resolved_range": resolved.document(),
        "schema": spec.plan.schema_document(),
        "acquisition_plan": spec.plan.document(),
        "row_count": resolved.last_block - resolved.first_block + 1,
        "target_hash": target_hash,
        "finalized_anchor": anchor.document(),
        "verification": verification,
    }


def _sample_numbers(dataset_id: UUID, first_block: int, last_block: int) -> list[int]:
    selected = {first_block, last_block}
    available = last_block - first_block - 1
    if available > 0:
        seed = int.from_bytes(hashlib.sha256(dataset_id.bytes).digest()[:8], "big")
        for offset in range(min(3, available)):
            selected.add(first_block + 1 + (seed + offset) % available)
    return sorted(selected)


def _receipt(
    operation: str,
    dataset: LoadedDataset,
    destination: Path,
    reused: int,
    acquired: int,
    anchor: Anchor,
) -> dict[str, object]:
    return {
        "version": 1,
        "operation": operation,
        "dataset_id": dataset.manifest["dataset_id"],
        "path": str(destination),
        "chain": dataset.manifest["chain"],
        "resolved_range": dataset.manifest["resolved_range"],
        "rows": dataset.rows,
        "reused_rows": reused,
        "acquired_rows": acquired,
        "finalized_anchor": anchor.document(),
        "artifact_sha256": pair_hashes(dataset.path),
    }
