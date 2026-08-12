"""Download, verify, and atomically publish configured datasets."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Literal

from . import __version__, _sources
from ._contract import (
    DownloadRequest,
    Provider,
)
from ._corpus import materialize_artifact, open_dataset

Progress = Callable[[dict[str, object]], None]
Publication = Callable[[Literal["publishing", "committed"]], None]


async def download(spec: DownloadRequest, *, progress: Progress, publication: Publication) -> dict[str, object]:
    progress({"event": "request", "dataset_id": str(spec.dataset_id)})
    return await _sources.acquire(
        spec,
        progress,
        lambda source: materialize_artifact(spec, source, tool_version=__version__, progress=progress, publication=publication),
    )


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
    if provider is not None:
        verification = await _sources.verify_rpc(dataset, provider, full_rpc)
        dataset._assert_unchanged()
    return {
        "operation": "verify",
        "dataset_id": str(dataset.dataset_id),
        "path": str(dataset.path),
        "rows": dataset.row_count,
        "artifact_sha256": dataset._pair_hashes,
        "verification": verification,
    }
