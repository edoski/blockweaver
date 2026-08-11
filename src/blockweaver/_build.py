"""Download, verify, and atomically publish configured datasets."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Literal

from . import __version__, _sources
from ._contract import (
    Anchor,
    BlockweaverError,
    DownloadRequest,
    Header,
    Provider,
    ResolvedRange,
    validate_links,
)
from ._corpus import (
    Dataset,
    _open_dataset,
    checkpoint_paths,
    dataset_path,
    discard_work,
    locked_work,
    open_dataset,
    pair_hashes,
    prepare_work,
    publish,
    save_ready,
    write_candidate,
    write_checkpoint,
)
from ._sources import SourceAdapter

Progress = Callable[[dict[str, object]], None]
Publication = Callable[[Literal["publishing", "committed"]], None]


async def download(spec: DownloadRequest, *, progress: Progress, publication: Publication) -> dict[str, object]:
    progress({"event": "request", "dataset_id": str(spec.dataset_id)})
    return await _sources.acquire(
        spec,
        progress,
        lambda source: _materialize(spec, source, progress, publication),
    )


async def _materialize(
    spec: DownloadRequest,
    source: SourceAdapter,
    progress: Progress,
    publication: Publication,
) -> dict[str, object]:
    resolved = source.resolved
    root = spec.output_root.resolve()
    destination = dataset_path(root, spec.dataset_id)
    with locked_work(root, spec.dataset_id, destination) as hidden:
        work = prepare_work(hidden, destination, _binding(spec, resolved))
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
                size=_sources.CHUNK_SIZE,
                plan=spec.plan,
            )
            reused = next_block - resolved.first_block
            progress({"event": "resume", "reused_rows": reused})
        if receipt is None:
            if candidate_path is None:
                async for headers, rows in source.chunks(next_block, resolved.last_block):
                    expected_last = min(next_block + _sources.CHUNK_SIZE - 1, resolved.last_block)
                    if not headers or len(headers) != len(rows) or (headers[0].block_number, headers[-1].block_number) != (next_block, expected_last):
                        raise ValueError("Source did not return a deterministic complete chunk")
                    validate_links(headers, previous)
                    if next_block == resolved.first_block and headers[0].timestamp != resolved.first_timestamp:
                        raise ValueError("Source start timestamp does not match the resolved range")
                    paths.append(write_checkpoint(work.chunks, next_block, expected_last, spec.plan, headers, rows))
                    previous, next_block = headers[-1], expected_last + 1
                    progress({"event": "checkpoint", "from_block": headers[0].block_number, "to_block": expected_last})
                if previous is None or next_block != resolved.last_block + 1 or previous.timestamp != resolved.last_timestamp:
                    raise ValueError("Source did not return the exact resolved range")
                anchor, verification = await source.prove(previous, paths)
                candidate_path = hidden / "ready.tmp"
                candidate = write_candidate(
                    candidate_path,
                    plan=spec.plan,
                    output_format=spec.output_format,
                    sources=paths,
                    manifest=_manifest(spec, resolved, previous.block_hash, anchor, verification),
                )
                acquired = candidate.row_count - reused
            else:
                candidate = _open_dataset(candidate_path, work=candidate_path.name in {"ready", "ready.tmp"})
                await source.revalidate(candidate)
                anchor = candidate._anchor
                acquired, reused = 0, candidate.row_count
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


async def verify_dataset(
    path: Path,
    *,
    provider: Provider | None,
    full_rpc: bool,
    progress: Progress,
) -> dict[str, object]:
    dataset = open_dataset(path)
    progress({"event": "local_valid", "rows": dataset.row_count})
    verification: dict[str, object] = {"mode": "local"}
    if provider is None:
        if full_rpc:
            raise BlockweaverError("VERIFY_INVALID", "--full-rpc requires a configured provider or --rpc-url")
    else:
        verification = await _sources.verify_rpc(dataset, provider, full_rpc)
    return {
        "version": 1,
        "operation": "verify",
        "dataset_id": str(dataset.dataset_id),
        "path": str(dataset.path),
        "rows": dataset.row_count,
        "artifact_sha256": pair_hashes(dataset.path),
        "verification": verification,
    }


def _binding(spec: DownloadRequest, resolved: ResolvedRange) -> dict[str, object]:
    return {
        "version": 1,
        "dataset_id": str(spec.dataset_id),
        "chain": {"name": spec.chain.name, "chain_id": spec.chain.chain_id, "finality_tag": spec.chain.finality_tag},
        "source": spec.source_document(),
        "requested_range": spec.requested_range.document(),
        "resolved_range": resolved.document(),
        "features": list(spec.plan.columns[1:]),
        "format": spec.output_format,
    }


def _manifest(
    spec: DownloadRequest,
    resolved: ResolvedRange,
    target_hash: str,
    anchor: Anchor,
    verification: dict[str, object],
) -> dict[str, object]:
    return {
        "tool_version": __version__,
        "dataset_id": str(spec.dataset_id),
        "chain": {"name": spec.chain.name, "chain_id": spec.chain.chain_id},
        "source": spec.source_document(),
        "requested_range": spec.requested_range.document(),
        "resolved_range": resolved.document(),
        "schema": spec.plan.schema_document(),
        "acquisition_plan": spec.plan.document(spec.source),
        "row_count": resolved.last_block - resolved.first_block + 1,
        "target_hash": target_hash,
        "finalized_anchor": anchor.document(),
        "verification": verification,
    }


def _receipt(
    operation: str,
    dataset: Dataset,
    destination: Path,
    reused: int,
    acquired: int,
    anchor: Anchor,
) -> dict[str, object]:
    manifest = dataset._document()
    return {
        "version": 1,
        "operation": operation,
        "dataset_id": str(dataset.dataset_id),
        "path": str(destination),
        "chain": manifest["chain"],
        "resolved_range": manifest["resolved_range"],
        "rows": dataset.row_count,
        "reused_rows": reused,
        "acquired_rows": acquired,
        "finalized_anchor": anchor.document(),
        "artifact_sha256": pair_hashes(dataset.path),
    }
