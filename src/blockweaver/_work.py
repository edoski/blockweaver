"""Resumable work state: checkpoints, the work lock, candidate assembly and atomic publication."""

from __future__ import annotations

import ctypes
import errno
import fcntl
import os
import re
import shutil
import stat
import sys
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Protocol
from uuid import UUID

import polars as pl

from ._config import DownloadRequest
from ._contract import Anchor, BlockweaverError, Header, OutputFormat, Plan, Progress, ResolvedRange, Row, validate_links
from ._dataset import Artifact, canonical_json, decode_json, file_hash, polars_schema, read_artifact, validate_manifest_source, validate_rows

_CHUNK = re.compile(r"(\d{20})-(\d{20})-([0-9a-f]{64})\.parquet\Z")
Publication = Callable[[Literal["publishing", "committed"]], None]


@dataclass(frozen=True, slots=True)
class VerifiedProof:
    """Provider proof for canonical manifest construction."""

    anchor: Anchor
    verification: dict[str, object]


class ArtifactSource(Protocol):
    resolved: ResolvedRange

    def chunks(self, first: int, last: int) -> AsyncIterator[list[Row]]: ...

    async def prove(self, target: Header) -> VerifiedProof: ...

    async def revalidate(self, artifact: Artifact) -> None: ...


@dataclass(frozen=True, slots=True)
class ArtifactIdentity:
    dataset_id: UUID
    chain_name: str
    chain_id: int
    finality_tag: str
    source: dict[str, object]
    requested_range: dict[str, object]
    resolved_range: dict[str, int]
    plan: Plan
    output_format: OutputFormat
    root: Path
    destination: Path

    @classmethod
    def from_request(cls, request: DownloadRequest, resolved: ResolvedRange) -> ArtifactIdentity:
        root = request.output_root.resolve()
        return cls(
            request.dataset_id,
            request.chain.name,
            request.chain.chain_id,
            request.chain.finality_tag,
            request.source_document(),
            request.requested_range.document(),
            resolved.document(),
            request.plan,
            request.output_format,
            root,
            root / str(request.dataset_id),
        )

    def binding(self) -> dict[str, object]:
        return {
            "dataset_id": str(self.dataset_id),
            "chain": {"name": self.chain_name, "chain_id": self.chain_id, "finality_tag": self.finality_tag},
            "source": self.source,
            "requested_range": self.requested_range,
            "resolved_range": self.resolved_range,
            "features": list(self.plan.columns[1:]),
            "format": self.output_format,
        }

    def manifest(self, tool_version: str, target_hash: str, proof: VerifiedProof) -> dict[str, object]:
        return {
            "tool_version": tool_version,
            "dataset_id": str(self.dataset_id),
            "chain": {"name": self.chain_name, "chain_id": self.chain_id},
            "source": self.source,
            "requested_range": self.requested_range,
            "resolved_range": self.resolved_range,
            "schema": self.plan.schema_document(),
            "acquisition_plan": self.plan.document(validate_manifest_source(self.source)),
            "row_count": self.resolved_range["to_block"] - self.resolved_range["from_block"] + 1,
            "target_hash": target_hash,
            "finalized_anchor": proof.anchor.document(),
            "verification": proof.verification,
        }

    def validate(self, artifact: Artifact) -> None:
        manifest = artifact.manifest
        recovered = {
            "dataset_id": manifest["dataset_id"],
            "chain": {**manifest["chain"], "finality_tag": artifact.anchor.tag},
            "source": manifest["source"],
            "requested_range": manifest["requested_range"],
            "resolved_range": manifest["resolved_range"],
            "features": list(artifact.dataset.schema[1:]),
            "format": artifact.dataset.output_format,
        }
        if recovered != self.binding():
            raise BlockweaverError("RESUME_MISMATCH", "Recovered dataset does not match the immutable work binding")


@dataclass(frozen=True, slots=True)
class Checkpoint:
    path: Path
    first: int
    last: int
    digest: str
    first_header: Header
    last_header: Header


class CheckpointSet:
    """A deterministic checkpoint prefix retaining only per-file metadata."""

    def __init__(self, chunks: Path, identity: ArtifactIdentity, size: int) -> None:
        self.chunks = chunks
        self.identity = identity
        self.size = size
        self.items: list[Checkpoint] = []
        self.next_block = identity.resolved_range["from_block"]
        self.previous: Header | None = None

    @classmethod
    def recover(cls, chunks: Path, identity: ArtifactIdentity, size: int) -> CheckpointSet:
        checkpoints = cls(chunks, identity, size)
        parsed: list[tuple[int, int, str, Path]] = []
        for path in chunks.iterdir():
            match = _CHUNK.fullmatch(path.name)
            if match is None:
                raise BlockweaverError("RESUME_INVALID", f"Unexpected checkpoint file: {path.name}")
            parsed.append((int(match.group(1)), int(match.group(2)), match.group(3), path))
        for first, last, digest, path in sorted(parsed):
            checkpoints._append(_read_checkpoint(path, identity.plan, first, last, digest))
        return checkpoints

    def write(self, rows: list[Row]) -> Checkpoint:
        first = self.next_block
        last = min(first + self.size - 1, self.identity.resolved_range["to_block"])
        headers = [Header.of(row) for row in rows]
        if not headers or (headers[0].block_number, headers[-1].block_number) != (first, last):
            raise ValueError("Source did not return a deterministic complete chunk")
        validate_links(headers, self.previous)
        checkpoint = _write_checkpoint(self.chunks, first, last, self.identity.plan, rows)
        self._append(checkpoint)
        return checkpoint

    def _append(self, checkpoint: Checkpoint) -> None:
        expected_last = min(self.next_block + self.size - 1, self.identity.resolved_range["to_block"])
        if (checkpoint.first, checkpoint.last) != (self.next_block, expected_last):
            raise BlockweaverError("RESUME_INVALID", "Checkpoints are not a deterministic complete prefix")
        try:
            validate_links([checkpoint.first_header], self.previous)
        except ValueError as error:
            raise BlockweaverError("RESUME_INVALID", str(error)) from None
        self.items.append(checkpoint)
        self.previous = checkpoint.last_header
        self.next_block = checkpoint.last + 1

    def reseal(self) -> None:
        for checkpoint in self.items:
            if file_hash(checkpoint.path) != checkpoint.digest:
                raise BlockweaverError("RESUME_INVALID", f"Checkpoint changed during acquisition: {checkpoint.path.name}")

    @property
    def paths(self) -> list[Path]:
        return [item.path for item in self.items]


@dataclass(frozen=True, slots=True)
class Recovery:
    kind: Literal["checkpointing", "staged", "committed"]
    checkpoints: CheckpointSet | None = None
    artifact: Artifact | None = None


@dataclass(frozen=True, slots=True)
class WorkLock:
    path: Path
    recoverable: bool
    generation: tuple[int, int]


@contextmanager
def _locked_work(identity: ArtifactIdentity) -> Iterator[WorkLock]:
    identity.root.mkdir(parents=True, exist_ok=True)
    hidden = identity.root / f".blockweaver-{identity.dataset_id}"
    while True:
        if identity.destination.exists() and not hidden.exists():
            raise BlockweaverError("DESTINATION_EXISTS", f"Destination already exists: {identity.destination}")
        try:
            hidden.mkdir()
            recoverable = False
        except FileExistsError:
            recoverable = True
        try:
            descriptor = os.open(hidden, os.O_RDONLY | os.O_DIRECTORY)
        except FileNotFoundError:
            continue
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            descriptor_stat = os.fstat(descriptor)
            generation = (descriptor_stat.st_dev, descriptor_stat.st_ino)
            if not _owns_generation(hidden, generation):
                continue
            try:
                yield WorkLock(hidden, recoverable, generation)
            finally:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            return
        finally:
            os.close(descriptor)


def _owns_generation(path: Path, generation: tuple[int, int]) -> bool:
    try:
        current = os.stat(path, follow_symlinks=False)
    except FileNotFoundError:
        return False
    return stat.S_ISDIR(current.st_mode) and (current.st_dev, current.st_ino) == generation


def _prepare_work(hidden: Path, identity: ArtifactIdentity, size: int, *, recoverable: bool) -> Recovery:
    binding_path = hidden / "binding.json"
    chunks = hidden / "chunks"
    ready = hidden / "ready"
    expected_binding = identity.binding()
    if binding_path.exists():
        try:
            stored_binding = _read_json(binding_path)
        except ValueError:
            raise BlockweaverError("RESUME_MISMATCH", "Stored work binding is invalid") from None
        if stored_binding != expected_binding:
            raise BlockweaverError("RESUME_MISMATCH", "Incomplete work belongs to a different immutable request")
    if identity.destination.exists():
        if not recoverable:
            raise BlockweaverError("DESTINATION_EXISTS", f"Destination already exists: {identity.destination}")
        artifact = read_artifact(identity.destination, work=False)
        identity.validate(artifact)
        return Recovery("committed", artifact=artifact)
    for path in hidden.rglob("*.tmp"):
        shutil.rmtree(path) if path.is_dir() else path.unlink()
    if binding_path.is_file():
        if ready.exists():
            try:
                artifact = read_artifact(ready, work=True)
            except BlockweaverError:
                shutil.rmtree(ready) if ready.is_dir() else ready.unlink()
            else:
                identity.validate(artifact)
                return Recovery("staged", artifact=artifact)
        if chunks.is_dir():
            checkpoints = CheckpointSet.recover(chunks, identity, size)
            return Recovery("checkpointing", checkpoints)
    for path in hidden.iterdir():
        shutil.rmtree(path) if path.is_dir() else path.unlink()
    chunks.mkdir()
    _write_json(binding_path, expected_binding)
    fsync_directory(hidden)
    return Recovery("checkpointing", CheckpointSet(chunks, identity, size))


def _write_checkpoint(chunks: Path, first: int, last: int, plan: Plan, rows: list[Row]) -> Checkpoint:
    temporary = chunks / f".{first:020d}-{last:020d}.parquet.tmp"
    try:
        pl.DataFrame(rows, schema=polars_schema(plan.stored_schema)).write_parquet(temporary, compression="zstd", row_group_size=4096)
        _fsync_file(temporary)
        digest = file_hash(temporary)
        path = chunks / f"{first:020d}-{last:020d}-{digest}.parquet"
        os.replace(temporary, path)
        fsync_directory(chunks)
        return Checkpoint(path, first, last, digest, Header.of(rows[0]), Header.of(rows[-1]))
    finally:
        temporary.unlink(missing_ok=True)


def _read_checkpoint(path: Path, plan: Plan, first: int, last: int, digest: str) -> Checkpoint:
    try:
        if file_hash(path) != digest:
            raise ValueError
        frame = pl.read_parquet(path)
        if frame.schema != polars_schema(plan.stored_schema):
            raise ValueError
        validate_rows(frame.lazy(), plan.stored, first=first, last=last)
        headers = [Header.of(row) for row in frame.iter_rows(named=True)]
        validate_links(headers)
        return Checkpoint(path, first, last, digest, headers[0], headers[-1])
    except Exception as error:
        raise BlockweaverError("RESUME_INVALID", f"Invalid checkpoint: {path.name}") from error


def _write_candidate(
    candidate: Path,
    *,
    plan: Plan,
    output_format: str,
    sources: list[Path],
    manifest: dict[str, object],
) -> Artifact:
    candidate.mkdir()
    filename = f"blocks.{output_format}"
    data_path = candidate / filename
    temporary = candidate / f"{filename}.tmp"
    combined = pl.scan_parquet(sources).select(plan.columns)
    if output_format == "parquet":
        combined.sink_parquet(temporary, compression="zstd", row_group_size=4096, maintain_order=True)
    else:
        combined.sink_csv(temporary, maintain_order=True)
    _fsync_file(temporary)
    os.replace(temporary, data_path)
    output = {
        "filename": filename,
        "format": output_format,
        "bytes": data_path.stat().st_size,
        "sha256": file_hash(data_path),
    }
    _write_json(candidate / "manifest.json", {**manifest, "completed_at": _completion_time(), "output": output})
    fsync_directory(candidate)
    return read_artifact(candidate, work=True)


async def materialize_artifact(
    request: DownloadRequest,
    source: ArtifactSource,
    *,
    tool_version: str,
    chunk_size: int,
    progress: Progress,
    publication: Publication,
) -> dict[str, object]:
    """Recover or create one complete immutable artifact in a single ordered workflow."""

    identity = ArtifactIdentity.from_request(request, source.resolved)
    with _locked_work(identity) as lock:
        hidden = lock.path
        recovery = _prepare_work(hidden, identity, chunk_size, recoverable=lock.recoverable)
        recovered = recovery.kind != "checkpointing" or bool(recovery.checkpoints and recovery.checkpoints.items)
        if recovery.kind == "committed":
            assert recovery.artifact is not None
            receipt = _download_receipt(recovery.artifact, identity.destination, recovery.artifact.dataset.row_count, 0)
            publication("committed")
        else:
            candidate = recovery.artifact
            reused = 0
            if recovery.kind == "checkpointing":
                checkpoints = recovery.checkpoints
                assert checkpoints is not None
                reused = checkpoints.next_block - identity.resolved_range["from_block"]
                if reused:
                    progress({"event": "resume", "reused_rows": reused})
                async for rows in source.chunks(checkpoints.next_block, identity.resolved_range["to_block"]):
                    checkpoint = checkpoints.write(rows)
                    progress({"event": "checkpoint", "from_block": checkpoint.first, "to_block": checkpoint.last})
                target = checkpoints.previous
                if (
                    target is None
                    or checkpoints.next_block != identity.resolved_range["to_block"] + 1
                    or checkpoints.items[0].first_header.timestamp != identity.resolved_range["from_timestamp"]
                    or target.timestamp != identity.resolved_range["to_timestamp"]
                ):
                    raise ValueError("Source did not return the exact resolved range")
                proof = await source.prove(target)
                checkpoints.reseal()
                candidate = _write_candidate(
                    hidden / "ready.tmp",
                    plan=identity.plan,
                    output_format=identity.output_format,
                    sources=checkpoints.paths,
                    manifest=identity.manifest(tool_version, target.block_hash, proof),
                )
                acquired = candidate.dataset.row_count - reused
            else:
                assert candidate is not None
                reused = candidate.dataset.row_count
                acquired = 0
            await source.revalidate(candidate)
            candidate.assert_unchanged()
            receipt = _download_receipt(candidate, identity.destination, reused, acquired)
            _stage_candidate(hidden, candidate.dataset.path)
            publication("publishing")
            _publish(hidden, identity.destination)
            publication("committed")
        _discard_work(lock)
        progress(
            {
                "event": "published",
                "dataset_id": str(identity.dataset_id),
                "path": str(identity.destination),
                **({"recovered": True} if recovered else {}),
            }
        )
        return receipt


def _download_receipt(artifact: Artifact, destination: Path, reused: int, acquired: int) -> dict[str, object]:
    return {
        "operation": "download",
        "dataset_id": artifact.manifest["dataset_id"],
        "path": str(destination),
        "chain": artifact.manifest["chain"],
        "resolved_range": artifact.manifest["resolved_range"],
        "rows": artifact.dataset.row_count,
        "reused_rows": reused,
        "acquired_rows": acquired,
        "finalized_anchor": artifact.anchor.document(),
        "artifact_sha256": artifact.fingerprint.hashes(),
    }


def _stage_candidate(hidden: Path, candidate: Path) -> None:
    ready = hidden / "ready"
    if candidate != ready:
        os.rename(candidate, ready)
    fsync_directory(hidden)


def _publish(hidden: Path, destination: Path) -> None:
    ready = hidden / "ready"
    files = list(ready.iterdir())
    if len(files) != 2 or not all(path.is_file() for path in files) or not (ready / "manifest.json").is_file():
        raise BlockweaverError("PUBLICATION_FAILED", "Candidate does not contain exactly the canonical artifact pair")
    for path in files:
        _sync_final_file(path)
    fsync_directory(ready)
    _rename_no_replace(ready, destination)
    fsync_directory(destination.parent)
    fsync_directory(hidden)


def _discard_work(lock: WorkLock) -> None:
    root = lock.path.parent
    if not _owns_generation(lock.path, lock.generation):
        raise BlockweaverError("PUBLICATION_FAILED", "Work directory generation changed before cleanup")
    try:
        shutil.rmtree(lock.path)
    except FileNotFoundError:
        raise BlockweaverError("PUBLICATION_FAILED", "Work directory disappeared before cleanup") from None
    fsync_directory(root)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        data = path.read_bytes()
    except OSError as error:
        raise ValueError(f"Invalid JSON file: {path.name}") from error
    return decode_json(data, path.name)


def _write_json(path: Path, value: object) -> None:
    data = canonical_json(value)
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        temporary.write_bytes(data)
        _fsync_file(temporary)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _completion_time() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _fsync_file(path: Path) -> None:
    with path.open("rb") as stream:
        os.fsync(stream.fileno())


def _sync_final_file(path: Path) -> None:
    with path.open("rb") as stream:
        if sys.platform == "darwin":
            fcntl.fcntl(stream.fileno(), getattr(fcntl, "F_FULLFSYNC", 51))
        elif sys.platform.startswith("linux"):
            os.fsync(stream.fileno())
        else:
            raise OSError(errno.ENOTSUP, "Durable artifact publication is unavailable")


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _rename_no_replace(source: Path, destination: Path) -> None:
    if sys.platform == "darwin":
        library = ctypes.CDLL(None, use_errno=True)
        try:
            rename = library.renamex_np
        except AttributeError as error:
            raise OSError(errno.ENOTSUP, "Atomic no-replace publication is unavailable") from error
        rename.argtypes = (ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint)
        rename.restype = ctypes.c_int
        result = rename(os.fsencode(source), os.fsencode(destination), 0x00000004)
    elif sys.platform.startswith("linux"):
        library = ctypes.CDLL(None, use_errno=True)
        try:
            rename = library.renameat2
        except AttributeError as error:
            raise OSError(errno.ENOTSUP, "Atomic no-replace publication is unavailable") from error
        rename.argtypes = (ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint)
        rename.restype = ctypes.c_int
        result = rename(-100, os.fsencode(source), -100, os.fsencode(destination), 1)
    else:
        raise OSError(errno.ENOTSUP, "Atomic no-replace publication is unavailable")
    if result == 0:
        return
    code = ctypes.get_errno()
    if code in {errno.EEXIST, errno.ENOTEMPTY}:
        raise BlockweaverError("DESTINATION_EXISTS", f"Destination already exists: {destination}")
    raise OSError(code, os.strerror(code), destination)
