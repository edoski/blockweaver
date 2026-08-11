"""Canonical two-file dataset storage and resumable work state."""

from __future__ import annotations

import csv
import ctypes
import errno
import fcntl
import hashlib
import json
import os
import re
import shutil
import stat
import sys
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Protocol
from uuid import UUID

import polars as pl

from ._contract import (
    Anchor,
    BlockweaverError,
    DownloadRequest,
    Header,
    OutputFormat,
    Plan,
    ResolvedRange,
    Value,
    plan_features,
    validate_links,
    validate_manifest_source,
    validate_verification,
)

_CHUNK = re.compile(r"(\d{20})-(\d{20})-([0-9a-f]{64})\.parquet\Z")
_HASH = re.compile(r"0x[0-9a-f]{64}\Z")
_DECIMAL = re.compile(r"0|[1-9][0-9]*\Z")
_NAME = re.compile(r"[a-z0-9][a-z0-9_-]*\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MANIFEST_KEYS = {
    "tool_version",
    "dataset_id",
    "completed_at",
    "chain",
    "source",
    "requested_range",
    "resolved_range",
    "schema",
    "acquisition_plan",
    "row_count",
    "output",
    "target_hash",
    "finalized_anchor",
    "verification",
}
_RECEIPT_KEYS = {
    "operation",
    "dataset_id",
    "path",
    "chain",
    "resolved_range",
    "rows",
    "reused_rows",
    "acquired_rows",
    "finalized_anchor",
    "artifact_sha256",
}

Progress = Callable[[dict[str, object]], None]
Publication = Callable[[Literal["publishing", "committed"]], None]
FactReader = Callable[[list[int]], dict[int, dict[str, Value]]]


@dataclass(frozen=True, slots=True)
class VerifiedProof:
    """Provider facts retained across candidate assembly."""

    anchor: Anchor
    verification: dict[str, object]
    samples: dict[int, dict[str, Value]]


class ArtifactSource(Protocol):
    resolved: ResolvedRange

    @property
    def chunk_size(self) -> int: ...

    def chunks(self, first: int, last: int) -> AsyncIterator[tuple[list[Header], list[dict[str, Value]]]]: ...

    async def prove(self, target: Header, read_facts: FactReader) -> VerifiedProof: ...

    async def revalidate(self, dataset: Dataset) -> None: ...


@dataclass(frozen=True, slots=True)
class PairFingerprint:
    manifest_sha256: str
    data_name: str
    data_bytes: int
    data_sha256: str

    def hashes(self) -> dict[str, str]:
        return {"manifest.json": self.manifest_sha256, self.data_name: self.data_sha256}


@dataclass(frozen=True, slots=True)
class ArtifactState:
    document: dict[str, Any]
    plan: Plan
    anchor: Anchor
    target_hash: str
    source: dict[str, object]
    requested_range: dict[str, object]
    resolved_range: dict[str, int]
    verification: dict[str, object]
    fingerprints: PairFingerprint


_DATASET_TOKEN = object()


@dataclass(frozen=True, slots=True, init=False)
class Dataset:
    """A strictly validated immutable local dataset."""

    path: Path
    dataset_id: UUID
    chain_name: str
    chain_id: int
    first_block: int
    last_block: int
    first_timestamp: int
    last_timestamp: int
    schema: tuple[str, ...]
    output_format: OutputFormat
    row_count: int
    data_path: Path
    _state: ArtifactState = field(repr=False, compare=False)

    def __new__(cls, token: object = None) -> Dataset:
        if token is not _DATASET_TOKEN:
            raise TypeError("Dataset objects must be created by open_dataset()")
        return object.__new__(cls)

    def __init__(self, token: object = None) -> None:
        del token

    @property
    def _plan(self) -> Plan:
        return self._state.plan

    @property
    def _anchor(self) -> Anchor:
        return self._state.anchor

    @property
    def _target_hash(self) -> str:
        return self._state.target_hash

    @property
    def _pair_hashes(self) -> dict[str, str]:
        return self._state.fingerprints.hashes()

    def _assert_unchanged(self) -> None:
        try:
            current = _fingerprint_pair(self.path, self._state.fingerprints.data_name)
        except (OSError, ValueError):
            current = None
        if current != self._state.fingerprints:
            raise BlockweaverError("ARTIFACT_INVALID", "Dataset bytes changed during verification")

    def _facts(self, numbers: list[int]) -> dict[int, dict[str, Value]]:
        frame = _scan_data(self.data_path, self._plan, self.output_format)
        selected = frame.filter(pl.col("block_number").is_in(numbers)).collect(engine="streaming")
        if selected["block_number"].to_list() != numbers:
            raise BlockweaverError("ARTIFACT_INVALID", "Requested dataset rows are missing")
        return {int(row["block_number"]): {name: value for name, value in row.items()} for row in selected.iter_rows(named=True)}

    def _fact_chunks(self, size: int) -> Iterator[dict[int, dict[str, Value]]]:
        batches = _scan_data(self.data_path, self._plan, self.output_format).collect_batches(
            chunk_size=size,
            maintain_order=True,
            engine="streaming",
        )
        for batch in batches:
            for frame in batch.iter_slices(n_rows=size):
                yield {int(row["block_number"]): {name: value for name, value in row.items()} for row in frame.iter_rows(named=True)}


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

    def validate_dataset(self, dataset: Dataset) -> None:
        state = dataset._state
        matches = (
            dataset.dataset_id == self.dataset_id
            and dataset.chain_name == self.chain_name
            and dataset.chain_id == self.chain_id
            and state.anchor.tag == self.finality_tag
            and state.source == self.source
            and state.requested_range == self.requested_range
            and state.resolved_range == self.resolved_range
            and dataset._plan == self.plan
            and dataset.output_format == self.output_format
        )
        if not matches:
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
            checkpoints._append(_read_checkpoint(path, identity.plan, digest), first, last)
        return checkpoints

    def write(self, headers: list[Header], rows: list[dict[str, Value]]) -> Checkpoint:
        first = self.next_block
        last = min(first + self.size - 1, self.identity.resolved_range["to_block"])
        if not headers or len(headers) != len(rows) or (headers[0].block_number, headers[-1].block_number) != (first, last):
            raise ValueError("Source did not return a deterministic complete chunk")
        validate_links(headers, self.previous)
        checkpoint = _write_checkpoint(self.chunks, first, last, self.identity.plan, headers, rows)
        self._append(checkpoint, first, last)
        return checkpoint

    def _append(self, checkpoint: Checkpoint, first: int, last: int) -> None:
        expected_last = min(self.next_block + self.size - 1, self.identity.resolved_range["to_block"])
        if (first, last) != (self.next_block, expected_last) or (checkpoint.first, checkpoint.last) != (first, last):
            raise BlockweaverError("RESUME_INVALID", "Checkpoints are not a deterministic complete prefix")
        try:
            validate_links([checkpoint.first_header], self.previous)
        except ValueError as error:
            raise BlockweaverError("RESUME_INVALID", str(error)) from None
        self.items.append(checkpoint)
        self.previous = checkpoint.last_header
        self.next_block = last + 1

    def facts(self, numbers: list[int]) -> dict[int, dict[str, Value]]:
        scans = [pl.scan_parquet(item.path).select(self.identity.plan.columns) for item in self.items]
        selected = pl.concat(scans, how="vertical").filter(pl.col("block_number").is_in(numbers)).collect(engine="streaming")
        if selected["block_number"].to_list() != numbers:
            raise BlockweaverError("RESUME_INVALID", "Requested checkpoint rows are missing")
        return {int(row["block_number"]): {name: value for name, value in row.items()} for row in selected.iter_rows(named=True)}

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
    dataset: Dataset | None = None
    receipt: dict[str, Any] | None = None


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
    receipt_path = hidden / "receipt.json"
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
        dataset = open_dataset(identity.destination)
        identity.validate_dataset(dataset)
        return Recovery("committed", dataset=dataset, receipt=_recover_receipt(receipt_path, dataset, identity.destination))
    for path in hidden.rglob("*.tmp"):
        shutil.rmtree(path) if path.is_dir() else path.unlink()
    if binding_path.is_file():
        if ready.exists():
            try:
                dataset = _open_dataset(ready, work=True)
            except BlockweaverError:
                shutil.rmtree(ready) if ready.is_dir() else ready.unlink()
                receipt_path.unlink(missing_ok=True)
            else:
                identity.validate_dataset(dataset)
                return Recovery("staged", dataset=dataset, receipt=_recover_receipt(receipt_path, dataset, identity.destination))
        if chunks.is_dir():
            checkpoints = CheckpointSet.recover(chunks, identity, size)
            receipt_path.unlink(missing_ok=True)
            return Recovery("checkpointing", checkpoints)
    for path in hidden.iterdir():
        shutil.rmtree(path) if path.is_dir() else path.unlink()
    chunks.mkdir()
    _write_json(binding_path, expected_binding)
    _fsync_directory(hidden)
    return Recovery("checkpointing", CheckpointSet(chunks, identity, size))


def _recover_receipt(path: Path, dataset: Dataset, destination: Path) -> dict[str, Any] | None:
    if not path.is_file():
        if path.exists():
            shutil.rmtree(path) if path.is_dir() else path.unlink()
        return None
    try:
        receipt = _read_json(path)
    except ValueError:
        return None
    return receipt if _receipt_matches(receipt, dataset, destination) else None


def _receipt_matches(receipt: dict[str, Any], dataset: Dataset, destination: Path) -> bool:
    state = dataset._state
    return (
        set(receipt) == _RECEIPT_KEYS
        and receipt.get("operation") == "download"
        and receipt.get("dataset_id") == str(dataset.dataset_id)
        and receipt.get("path") == str(destination)
        and receipt.get("chain") == state.document["chain"]
        and receipt.get("resolved_range") == state.resolved_range
        and receipt.get("rows") == dataset.row_count
        and receipt.get("finalized_anchor") == state.anchor.document()
        and receipt.get("artifact_sha256") == dataset._pair_hashes
        and type(receipt.get("reused_rows")) is int
        and type(receipt.get("acquired_rows")) is int
        and receipt["reused_rows"] >= 0
        and receipt["acquired_rows"] >= 0
        and receipt["reused_rows"] + receipt["acquired_rows"] == dataset.row_count
    )


def checkpoint_schema(plan: Plan):
    return {
        **_polars_schema(plan),
        "_proof_hash": pl.String,
        "_proof_parent_hash": pl.String,
        "_proof_timestamp": pl.Int64,
    }


def _write_checkpoint(chunks: Path, first: int, last: int, plan: Plan, headers: list[Header], rows: list[dict[str, Value]]) -> Checkpoint:
    temporary = chunks / f".{first:020d}-{last:020d}.parquet.tmp"
    try:
        values = [
            {
                **row,
                "_proof_hash": header.block_hash,
                "_proof_parent_hash": header.parent_hash,
                "_proof_timestamp": header.timestamp,
            }
            for header, row in zip(headers, rows, strict=True)
        ]
        pl.DataFrame(values, schema=checkpoint_schema(plan)).write_parquet(temporary, compression="zstd", row_group_size=4096)
        _fsync_file(temporary)
        digest = file_hash(temporary)
        path = chunks / f"{first:020d}-{last:020d}-{digest}.parquet"
        os.replace(temporary, path)
        _fsync_directory(chunks)
        return _read_checkpoint(path, plan, digest, digest_verified=True)
    finally:
        temporary.unlink(missing_ok=True)


def _read_checkpoint(path: Path, plan: Plan, digest: str, *, digest_verified: bool = False) -> Checkpoint:
    try:
        match = _CHUNK.fullmatch(path.name)
        if match is None or match.group(3) != digest:
            raise ValueError
        if not digest_verified and file_hash(path) != digest:
            raise ValueError
        frame = pl.read_parquet(path)
        if frame.schema != checkpoint_schema(plan) or frame.is_empty() or frame.null_count().row(0) != (0,) * len(frame.columns):
            raise ValueError
        _validate_eager_frame(frame.select(plan.columns), plan)
        for row in frame.iter_rows(named=True):
            if "timestamp" in plan.columns and row["timestamp"] != row["_proof_timestamp"]:
                raise ValueError
            if "block_hash" in plan.columns and row["block_hash"] != row["_proof_hash"]:
                raise ValueError
            if "parent_hash" in plan.columns and row["parent_hash"] != row["_proof_parent_hash"]:
                raise ValueError
        headers = [
            Header(row["block_number"], row["_proof_hash"], row["_proof_parent_hash"], row["_proof_timestamp"], {}) for row in frame.iter_rows(named=True)
        ]
        first, last = int(match.group(1)), int(match.group(2))
        if [header.block_number for header in headers] != list(range(first, last + 1)) or any(
            _HASH.fullmatch(value) is None for header in headers for value in (header.block_hash, header.parent_hash)
        ):
            raise ValueError
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
) -> Dataset:
    candidate.mkdir()
    filename = f"blocks.{output_format}"
    data_path = candidate / filename
    temporary = candidate / f"{filename}.tmp"
    scans = [pl.scan_parquet(path).select(plan.columns) for path in sources]
    combined = pl.concat(scans, how="vertical")
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
    _fsync_directory(candidate)
    return _open_dataset(candidate, work=True)


async def materialize_artifact(
    request: DownloadRequest,
    source: ArtifactSource,
    *,
    tool_version: str,
    progress: Progress,
    publication: Publication,
) -> dict[str, object]:
    """Recover or create one complete immutable artifact in a single ordered workflow."""

    identity = ArtifactIdentity.from_request(request, source.resolved)
    with _locked_work(identity) as lock:
        hidden = lock.path
        recovery = _prepare_work(hidden, identity, source.chunk_size, recoverable=lock.recoverable)
        recovered = recovery.kind != "checkpointing" or bool(recovery.checkpoints and recovery.checkpoints.items)
        if recovery.kind == "committed":
            assert recovery.dataset is not None
            receipt = recovery.receipt or _download_receipt(recovery.dataset, identity.destination, recovery.dataset.row_count, 0)
            publication("committed")
        else:
            candidate = recovery.dataset
            receipt = recovery.receipt
            reused = 0
            acquired = 0
            if recovery.kind == "checkpointing":
                checkpoints = recovery.checkpoints
                assert checkpoints is not None
                reused = checkpoints.next_block - identity.resolved_range["from_block"]
                progress({"event": "resume", "reused_rows": reused})
                async for headers, rows in source.chunks(checkpoints.next_block, identity.resolved_range["to_block"]):
                    checkpoint = checkpoints.write(headers, rows)
                    progress({"event": "checkpoint", "from_block": checkpoint.first, "to_block": checkpoint.last})
                target = checkpoints.previous
                if (
                    target is None
                    or checkpoints.next_block != identity.resolved_range["to_block"] + 1
                    or checkpoints.items[0].first_header.timestamp != identity.resolved_range["from_timestamp"]
                    or target.timestamp != identity.resolved_range["to_timestamp"]
                ):
                    raise ValueError("Source did not return the exact resolved range")
                proof = await source.prove(target, checkpoints.facts)
                checkpoints.reseal()
                candidate = _write_candidate(
                    hidden / "ready.tmp",
                    plan=identity.plan,
                    output_format=identity.output_format,
                    sources=checkpoints.paths,
                    manifest=identity.manifest(tool_version, target.block_hash, proof),
                )
                _validate_verified_samples(candidate, proof)
                acquired = candidate.row_count - reused
            assert candidate is not None
            await source.revalidate(candidate)
            candidate._assert_unchanged()
            if receipt is None:
                receipt = _download_receipt(
                    candidate,
                    identity.destination,
                    candidate.row_count if recovery.kind == "staged" else reused,
                    0 if recovery.kind == "staged" else acquired,
                )
            _stage_candidate(hidden, candidate.path, receipt)
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


def _validate_verified_samples(candidate: Dataset, proof: VerifiedProof) -> None:
    numbers = list(proof.samples)
    if proof.verification.get("sampled_blocks") != numbers or candidate._facts(numbers) != proof.samples:
        raise BlockweaverError("RPC_MISMATCH", "Published candidate does not match the externally verified sample facts")


def _download_receipt(dataset: Dataset, destination: Path, reused: int, acquired: int) -> dict[str, object]:
    state = dataset._state
    return {
        "operation": "download",
        "dataset_id": str(dataset.dataset_id),
        "path": str(destination),
        "chain": state.document["chain"],
        "resolved_range": state.resolved_range,
        "rows": dataset.row_count,
        "reused_rows": reused,
        "acquired_rows": acquired,
        "finalized_anchor": state.anchor.document(),
        "artifact_sha256": dataset._pair_hashes,
    }


def open_dataset(path: str | Path) -> Dataset:
    """Open and strictly validate a published Blockweaver dataset."""

    return _open_dataset(Path(path), work=False)


def _open_dataset(path: Path, *, work: bool) -> Dataset:
    try:
        return _read_dataset(path, work=work)
    except BlockweaverError as error:
        raise BlockweaverError("ARTIFACT_INVALID", str(error)) from None
    except Exception as error:
        raise BlockweaverError("ARTIFACT_INVALID", str(error) or "Invalid dataset") from None


def _read_dataset(path: Path, *, work: bool) -> Dataset:
    path = path.resolve()
    if not path.is_dir():
        raise ValueError(f"Dataset directory does not exist: {path}")
    manifest_path = path / "manifest.json"
    if not manifest_path.is_file():
        raise ValueError("Dataset must contain manifest.json")
    try:
        manifest_bytes = manifest_path.read_bytes()
    except OSError as error:
        raise ValueError("Invalid JSON file: manifest.json") from error
    manifest = _decode_json(manifest_bytes, "manifest.json")
    if manifest_bytes != _canonical_json(manifest):
        raise ValueError("manifest.json is not canonical JSON")
    if set(manifest) != _MANIFEST_KEYS:
        raise ValueError("manifest.json has a noncanonical shape")
    if not isinstance(manifest["tool_version"], str) or not manifest["tool_version"]:
        raise ValueError("Invalid tool version")
    dataset_id = _canonical_uuid(manifest["dataset_id"])
    chain = _exact_table(manifest["chain"], {"name", "chain_id"}, "chain")
    if not isinstance(chain["name"], str) or _NAME.fullmatch(chain["name"]) is None or type(chain["chain_id"]) is not int or chain["chain_id"] <= 0:
        raise ValueError("Invalid manifest chain")
    source_value = manifest["source"]
    if not isinstance(source_value, dict):
        raise ValueError("Invalid manifest source")
    source = validate_manifest_source(source_value)
    requested = manifest["requested_range"]
    if not isinstance(requested, dict) or requested.get("kind") not in {"block", "time"}:
        raise ValueError("Invalid requested range")
    expected_requested = {"kind", "from", "to"} if requested["kind"] == "block" else {"kind", "from", "to", "normalized_from_utc", "normalized_to_utc"}
    if set(requested) != expected_requested:
        raise ValueError("Invalid requested range shape")
    resolved = _exact_table(
        manifest["resolved_range"],
        {"from_block", "to_block", "from_timestamp", "to_timestamp"},
        "resolved range",
    )
    if any(type(resolved[key]) is not int or resolved[key] < 0 for key in resolved):
        raise ValueError("Invalid resolved range values")
    if resolved["from_block"] > resolved["to_block"] or resolved["from_timestamp"] > resolved["to_timestamp"]:
        raise ValueError("Invalid resolved range order")
    if requested["kind"] == "block":
        if any(type(requested[key]) is not int or requested[key] < 0 for key in ("from", "to")):
            raise ValueError("Invalid requested block range")
        if (requested["from"], requested["to"]) != (resolved["from_block"], resolved["to_block"]):
            raise ValueError("Requested and resolved block ranges disagree")
    else:
        if any(not isinstance(requested[key], str) for key in expected_requested - {"kind"}):
            raise ValueError("Invalid requested time range")
        normalized_from = _utc_timestamp(requested["normalized_from_utc"])
        normalized_to = _utc_timestamp(requested["normalized_to_utc"])
        if normalized_from > normalized_to or resolved["from_timestamp"] < normalized_from or resolved["to_timestamp"] > normalized_to:
            raise ValueError("Requested and resolved time ranges disagree")
    schema = manifest["schema"]
    if not isinstance(schema, list) or not schema:
        raise ValueError("Invalid schema")
    names: list[str] = []
    for column in schema:
        parsed = _exact_table(column, {"name", "type", "unit"}, "schema column")
        if any(not isinstance(parsed[key], str) for key in parsed):
            raise ValueError("Invalid schema column")
        names.append(parsed["name"])
    if names[0] != "block_number":
        raise ValueError("block_number must be the first schema column")
    plan = plan_features(names[1:])
    if schema != plan.schema_document() or manifest["acquisition_plan"] != plan.document(source):
        raise ValueError("Schema or acquisition plan is not canonical")
    output = _exact_table(manifest["output"], {"filename", "format", "bytes", "sha256"}, "output")
    if output["format"] not in {"parquet", "csv"} or output["filename"] != f"blocks.{output['format']}":
        raise ValueError("Invalid output descriptor")
    data_path = path / output["filename"]
    if {item.name for item in path.iterdir()} != {"manifest.json", output["filename"]}:
        raise ValueError("Dataset directory must contain exactly manifest.json and its declared data file")
    if (
        type(output["bytes"]) is not int
        or output["bytes"] < 0
        or output["bytes"] != data_path.stat().st_size
        or not isinstance(output["sha256"], str)
        or _SHA256.fullmatch(output["sha256"]) is None
    ):
        raise ValueError("Data file size or digest does not match manifest")
    if not isinstance(manifest["completed_at"], str) or not manifest["completed_at"].endswith("Z"):
        raise ValueError("Invalid completion time")
    completed = datetime.fromisoformat(manifest["completed_at"].replace("Z", "+00:00"))
    if completed.utcoffset() != UTC.utcoffset(completed):
        raise ValueError("Completion time is not UTC")
    anchor = _exact_table(manifest["finalized_anchor"], {"block_number", "block_hash", "tag"}, "finalized anchor")
    if type(anchor["block_number"]) is not int or anchor["block_number"] < resolved["to_block"]:
        raise ValueError("Finalized anchor does not cover the dataset")
    if _HASH.fullmatch(anchor["block_hash"]) is None or anchor["tag"] not in {"finalized", "safe"}:
        raise ValueError("Invalid finalized anchor")
    if not isinstance(manifest["target_hash"], str) or _HASH.fullmatch(manifest["target_hash"]) is None:
        raise ValueError("Invalid target hash")
    verification = validate_verification(manifest["verification"], chain["chain_id"], source)
    samples = verification["sampled_blocks"]
    if (
        not isinstance(samples, list)
        or any(type(number) is not int for number in samples)
        or samples != sorted(set(samples))
        or any(number < resolved["from_block"] or number > resolved["to_block"] for number in samples)
        or resolved["from_block"] not in samples
        or resolved["to_block"] not in samples
    ):
        raise ValueError("Invalid verification samples")
    expected_name = str(dataset_id)
    allowed_names = {expected_name, "ready", "ready.tmp"} if work else {expected_name}
    if path.name not in allowed_names:
        raise ValueError("Dataset directory name does not match its manifest")
    rows = _validate_data(data_path, plan, output["format"], resolved, manifest["target_hash"])
    if type(manifest["row_count"]) is not int or manifest["row_count"] != rows:
        raise ValueError("Manifest row count does not match data")
    fingerprint = _fingerprint_pair(path, output["filename"])
    if fingerprint.manifest_sha256 != hashlib.sha256(manifest_bytes).hexdigest():
        raise ValueError("Manifest changed during validation")
    if fingerprint.data_bytes != output["bytes"] or fingerprint.data_sha256 != output["sha256"]:
        raise ValueError("Data file size or digest does not match manifest")
    state = ArtifactState(
        manifest,
        plan,
        Anchor(anchor["block_number"], anchor["block_hash"], anchor["tag"]),
        manifest["target_hash"],
        source_value,
        requested,
        resolved,
        verification,
        fingerprint,
    )
    return _make_dataset(
        path=path,
        dataset_id=dataset_id,
        chain_name=chain["name"],
        chain_id=chain["chain_id"],
        first_block=resolved["from_block"],
        last_block=resolved["to_block"],
        first_timestamp=resolved["from_timestamp"],
        last_timestamp=resolved["to_timestamp"],
        schema=tuple(names),
        output_format=output["format"],
        row_count=rows,
        data_path=data_path,
        _state=state,
    )


def _make_dataset(**values: object) -> Dataset:
    dataset = Dataset(_DATASET_TOKEN)
    for name, value in values.items():
        object.__setattr__(dataset, name, value)
    return dataset


def _validate_data(path: Path, plan: Plan, output_format: str, resolved: dict[str, int], target_hash: str) -> int:
    if output_format == "parquet":
        if pl.read_parquet_schema(path) != _polars_schema(plan):
            raise ValueError("Parquet schema is not canonical")
    else:
        _validate_csv_tokens(path, plan)
    frame = _scan_data(path, plan, output_format)
    invalid = pl.lit(False)
    for feature in plan.features:
        if feature.dtype == "Int64":
            rule = pl.col(feature.name) < 0
            if feature.name in {"base_fee_per_gas", "gas_limit"}:
                rule = pl.col(feature.name) <= 0
            invalid |= rule
        elif feature.name in {"block_hash", "parent_hash"}:
            invalid |= ~pl.col(feature.name).str.contains(r"^0x[0-9a-f]{64}$")
    if {"gas_used", "gas_limit"} <= set(plan.columns):
        invalid |= pl.col("gas_used") > pl.col("gas_limit")
    expressions = [
        pl.len().alias("rows"),
        pl.col("block_number").first().alias("first"),
        pl.col("block_number").last().alias("last"),
        (pl.col("block_number").diff() != 1).fill_null(False).any().alias("gaps"),
        invalid.any().alias("invalid"),
        pl.sum_horizontal(*(pl.col(name).null_count() for name in plan.columns)).alias("nulls"),
    ]
    if "timestamp" in plan.columns:
        expressions.extend(
            [
                (pl.col("timestamp").diff() < 0).fill_null(False).any().alias("time_decreases"),
                pl.col("timestamp").first().alias("first_timestamp"),
                pl.col("timestamp").last().alias("last_timestamp"),
            ]
        )
    if "block_hash" in plan.columns:
        expressions.append(pl.col("block_hash").last().alias("target_hash"))
    summary = frame.select(*expressions).collect(engine="streaming").row(0, named=True)
    expected_rows = resolved["to_block"] - resolved["from_block"] + 1
    if summary["rows"] != expected_rows or summary["first"] != resolved["from_block"] or summary["last"] != resolved["to_block"] or summary["gaps"]:
        raise ValueError("Data rows are not the resolved contiguous range")
    if summary["invalid"] or summary["nulls"]:
        raise ValueError("Data contains invalid or null values")
    if "timestamp" in plan.columns and (
        summary["time_decreases"] or summary["first_timestamp"] != resolved["from_timestamp"] or summary["last_timestamp"] != resolved["to_timestamp"]
    ):
        raise ValueError("Data timestamps do not match the resolved range")
    if "block_hash" in plan.columns and summary["target_hash"] != target_hash:
        raise ValueError("Data target hash does not match the manifest")
    return expected_rows


def _validate_eager_frame(frame: pl.DataFrame, plan: Plan) -> None:
    if frame.schema != _polars_schema(plan) or frame.null_count().row(0) != (0,) * len(frame.columns):
        raise ValueError("Invalid checkpoint values")
    if frame["block_number"].to_list() != list(range(int(frame[0, "block_number"]), int(frame[-1, "block_number"]) + 1)):
        raise ValueError("Checkpoint block numbers are not contiguous")
    previous_timestamp: int | None = None
    for row in frame.iter_rows(named=True):
        for feature in plan.features:
            value = row[feature.name]
            if feature.dtype == "Int64" and (value < 0 or (feature.name in {"base_fee_per_gas", "gas_limit"} and value == 0)):
                raise ValueError("Checkpoint contains an invalid feature value")
            if feature.name in {"block_hash", "parent_hash"} and _HASH.fullmatch(value) is None:
                raise ValueError("Checkpoint contains an invalid hash")
        if {"gas_used", "gas_limit"} <= set(plan.columns) and row["gas_used"] > row["gas_limit"]:
            raise ValueError("Checkpoint contains invalid gas values")
        if "timestamp" in plan.columns:
            timestamp = row["timestamp"]
            if previous_timestamp is not None and timestamp < previous_timestamp:
                raise ValueError("Checkpoint timestamps decrease")
            previous_timestamp = timestamp


def _validate_csv_tokens(path: Path, plan: Plan) -> None:
    with path.open(newline="", encoding="utf-8") as stream:
        reader = csv.reader(stream)
        try:
            header = next(reader)
        except StopIteration:
            raise ValueError("CSV is empty") from None
        if header != list(plan.columns):
            raise ValueError("CSV header is not canonical")
        integer_indexes = [index for index, dtype in enumerate(plan.schema.values()) if dtype == "Int64"]
        for row in reader:
            if len(row) != len(header) or any(_DECIMAL.fullmatch(row[index]) is None for index in integer_indexes):
                raise ValueError("CSV contains a noncanonical value")


def _scan_data(path: Path, plan: Plan, output_format: str) -> pl.LazyFrame:
    if output_format == "parquet":
        return pl.scan_parquet(path)
    return pl.scan_csv(path, schema=_polars_schema(plan))


def _polars_schema(plan: Plan):
    return {name: pl.Int64 if dtype == "Int64" else pl.String for name, dtype in plan.schema.items()}


def _stage_candidate(hidden: Path, candidate: Path, receipt: dict[str, object]) -> None:
    ready = hidden / "ready"
    _write_json(hidden / "receipt.json", receipt)
    if candidate != ready:
        os.rename(candidate, ready)
    _fsync_directory(hidden)


def _publish(hidden: Path, destination: Path) -> None:
    ready = hidden / "ready"
    files = list(ready.iterdir())
    if len(files) != 2 or not all(path.is_file() for path in files) or not (ready / "manifest.json").is_file():
        raise BlockweaverError("PUBLICATION_FAILED", "Candidate does not contain exactly the canonical artifact pair")
    _ensure_publication_supported()
    for path in files:
        _sync_final_file(path)
    _fsync_directory(ready)
    _rename_no_replace(ready, destination)
    _fsync_directory(destination.parent)
    _fsync_directory(hidden)


def _discard_work(lock: WorkLock) -> None:
    root = lock.path.parent
    if not _owns_generation(lock.path, lock.generation):
        raise BlockweaverError("PUBLICATION_FAILED", "Work directory generation changed before cleanup")
    try:
        shutil.rmtree(lock.path)
    except FileNotFoundError:
        raise BlockweaverError("PUBLICATION_FAILED", "Work directory disappeared before cleanup") from None
    _fsync_directory(root)


def _fingerprint_pair(path: Path, data_name: str) -> PairFingerprint:
    if {item.name for item in path.iterdir()} != {"manifest.json", data_name}:
        raise ValueError("Dataset file set changed during validation")
    manifest = (path / "manifest.json").read_bytes()
    data = path / data_name
    return PairFingerprint(hashlib.sha256(manifest).hexdigest(), data_name, data.stat().st_size, file_hash(data))


def file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_uuid(value: object) -> UUID:
    if not isinstance(value, str):
        raise ValueError("Dataset ID must be a string")
    parsed = UUID(value)
    if parsed.version != 4 or str(parsed) != value:
        raise ValueError("Dataset ID must be a canonical UUID4")
    return parsed


def _utc_timestamp(value: str) -> int:
    if not value.endswith("Z"):
        raise ValueError("Normalized time must be UTC")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.utcoffset() != UTC.utcoffset(parsed):
        raise ValueError("Normalized time must be UTC")
    return int(parsed.timestamp())


def _exact_table(value: object, keys: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != keys:
        raise ValueError(f"Invalid {label}")
    return value


def _read_json(path: Path) -> dict[str, Any]:
    try:
        data = path.read_bytes()
    except OSError as error:
        raise ValueError(f"Invalid JSON file: {path.name}") from error
    return _decode_json(data, path.name)


def _decode_json(data: bytes, name: str) -> dict[str, Any]:
    try:
        value = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"Invalid JSON file: {name}") from error
    if not isinstance(value, dict):
        raise ValueError(f"Invalid JSON object: {name}")
    return value


def _write_json(path: Path, value: object) -> None:
    data = _canonical_json(value)
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        temporary.write_bytes(data)
        _fsync_file(temporary)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _canonical_json(value: object) -> bytes:
    return (json.dumps(value, ensure_ascii=True, allow_nan=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


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


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _rename_no_replace(source: Path, destination: Path) -> None:
    if sys.platform == "darwin":
        library = ctypes.CDLL(None, use_errno=True)
        rename = library.renamex_np
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


def _ensure_publication_supported() -> None:
    library = ctypes.CDLL(None, use_errno=True)
    if sys.platform == "darwin":
        if not hasattr(library, "renamex_np"):
            raise OSError(errno.ENOTSUP, "Atomic no-replace publication is unavailable")
        return
    if sys.platform.startswith("linux") and hasattr(library, "renameat2"):
        return
    raise OSError(errno.ENOTSUP, "Atomic no-replace publication is unavailable")
