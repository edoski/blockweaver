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
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

import polars as pl

from ._contract import (
    Anchor,
    BlockweaverError,
    Header,
    OutputFormat,
    Plan,
    Value,
    plan_features,
    source_definition,
    validate_links,
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
    "version",
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


@dataclass(frozen=True, slots=True)
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
    _manifest_json: bytes = field(repr=False, compare=False)

    @property
    def _plan(self) -> Plan:
        return plan_features(self.schema[1:])

    def _document(self) -> dict[str, Any]:
        return json.loads(self._manifest_json)

    @property
    def _anchor(self) -> Anchor:
        value = self._document()["finalized_anchor"]
        return Anchor(value["block_number"], value["block_hash"], value["tag"])

    @property
    def _target_hash(self) -> str:
        return str(self._document()["target_hash"])

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
class WorkState:
    chunks: Path
    candidate: Path | None = None
    receipt: dict[str, Any] | None = None
    published: bool = False


def dataset_path(root: Path, dataset_id: UUID) -> Path:
    return root / str(dataset_id)


@contextmanager
def locked_work(root: Path, dataset_id: UUID, destination: Path) -> Iterator[Path]:
    root.mkdir(parents=True, exist_ok=True)
    hidden = root / f".blockweaver-{dataset_id}"
    existed = hidden.exists()
    if destination.exists() and not existed:
        raise BlockweaverError("DESTINATION_EXISTS", f"Destination already exists: {destination}")
    hidden.mkdir(exist_ok=True)
    descriptor = os.open(hidden, os.O_RDONLY)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield hidden
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def prepare_work(hidden: Path, destination: Path, binding: dict[str, object]) -> WorkState:
    binding_path = hidden / "binding.json"
    chunks = hidden / "chunks"
    ready = hidden / "ready"
    receipt_path = hidden / "receipt.json"
    for path in hidden.rglob("*.tmp"):
        shutil.rmtree(path) if path.is_dir() else path.unlink()
    if binding_path.is_file() and chunks.is_dir():
        if _read_json(binding_path) != binding:
            raise BlockweaverError("RESUME_MISMATCH", "Incomplete work belongs to a different immutable request")
        if destination.exists():
            dataset = open_dataset(destination)
            _validate_dataset_binding(dataset, binding)
            receipt = _read_receipt(receipt_path) if receipt_path.is_file() else None
            if receipt is not None:
                _validate_recovery_receipt(receipt, dataset, destination)
            return WorkState(chunks, dataset.path, receipt, True)
        if ready.exists():
            try:
                dataset = _open_dataset(ready, work=True)
            except (BlockweaverError, OSError, ValueError):
                shutil.rmtree(ready) if ready.is_dir() else ready.unlink()
                receipt_path.unlink(missing_ok=True)
            else:
                _validate_dataset_binding(dataset, binding)
                receipt = _read_receipt(receipt_path) if receipt_path.is_file() else None
                if receipt is not None:
                    _validate_recovery_receipt(receipt, dataset, destination)
                return WorkState(chunks, dataset.path, receipt)
        receipt_path.unlink(missing_ok=True)
        return WorkState(chunks)
    if destination.exists():
        raise BlockweaverError("DESTINATION_EXISTS", f"Destination already exists: {destination}")
    for path in hidden.iterdir():
        shutil.rmtree(path) if path.is_dir() else path.unlink()
    chunks.mkdir()
    _write_json(binding_path, binding)
    _fsync_directory(hidden)
    return WorkState(chunks)


def _validate_dataset_binding(dataset: Dataset, binding: dict[str, object]) -> None:
    chain = binding["chain"]
    source = binding["source"]
    if not isinstance(chain, dict) or not isinstance(source, dict):
        raise BlockweaverError("RESUME_MISMATCH", "Stored work binding is invalid")
    manifest = dataset._document()
    matches = (
        str(dataset.dataset_id) == binding["dataset_id"]
        and manifest["chain"] == {"name": chain.get("name"), "chain_id": chain.get("chain_id")}
        and manifest["finalized_anchor"]["tag"] == chain.get("finality_tag")
        and manifest["source"] == source
        and manifest["requested_range"] == binding["requested_range"]
        and manifest["resolved_range"] == binding["resolved_range"]
        and list(dataset._plan.columns[1:]) == binding["features"]
        and dataset.output_format == binding["format"]
    )
    if not matches:
        raise BlockweaverError("RESUME_MISMATCH", "Recovered dataset does not match the immutable work binding")


def _validate_recovery_receipt(receipt: dict[str, Any], dataset: Dataset, destination: Path) -> None:
    manifest = dataset._document()
    if (
        set(receipt) != _RECEIPT_KEYS
        or receipt.get("version") != 1
        or receipt.get("operation") != "download"
        or receipt.get("dataset_id") != str(dataset.dataset_id)
        or receipt.get("path") != str(destination)
        or receipt.get("chain") != manifest["chain"]
        or receipt.get("resolved_range") != manifest["resolved_range"]
        or receipt.get("rows") != dataset.row_count
        or receipt.get("finalized_anchor") != manifest["finalized_anchor"]
        or receipt.get("artifact_sha256") != pair_hashes(dataset.path)
        or type(receipt.get("reused_rows")) is not int
        or type(receipt.get("acquired_rows")) is not int
        or receipt["reused_rows"] < 0
        or receipt["acquired_rows"] < 0
        or receipt["reused_rows"] + receipt["acquired_rows"] != dataset.row_count
    ):
        raise BlockweaverError("RESUME_INVALID", "Recovery receipt does not match the immutable dataset")


def _read_receipt(path: Path) -> dict[str, Any]:
    try:
        return _read_json(path)
    except ValueError:
        raise BlockweaverError("RESUME_INVALID", "Recovery receipt is invalid") from None


def checkpoint_paths(
    chunks: Path,
    *,
    first_block: int,
    last_block: int,
    size: int,
    plan: Plan,
) -> tuple[list[Path], int, Header | None]:
    parsed: list[tuple[int, int, Path]] = []
    for path in chunks.iterdir():
        match = _CHUNK.fullmatch(path.name)
        if match is None:
            raise BlockweaverError("RESUME_INVALID", f"Unexpected checkpoint file: {path.name}")
        _validate_checkpoint_digest(path, match)
        parsed.append((int(match.group(1)), int(match.group(2)), path))
    parsed.sort()
    expected = first_block
    previous: Header | None = None
    valid: list[Path] = []
    for first, last, path in parsed:
        expected_last = min(expected + size - 1, last_block)
        if (first, last) != (expected, expected_last):
            raise BlockweaverError("RESUME_INVALID", "Checkpoints are not a deterministic complete prefix")
        headers = read_checkpoint(path, plan)
        if [header.block_number for header in headers] != list(range(first, last + 1)):
            raise BlockweaverError("RESUME_INVALID", "Checkpoint range does not match its filename")
        try:
            validate_links(headers, previous)
        except ValueError as error:
            raise BlockweaverError("RESUME_INVALID", str(error)) from None
        previous = headers[-1]
        valid.append(path)
        expected = last + 1
    return valid, expected, previous


def checkpoint_schema(plan: Plan):
    return {
        **_polars_schema(plan),
        "_proof_hash": pl.String,
        "_proof_parent_hash": pl.String,
        "_proof_timestamp": pl.Int64,
    }


def write_checkpoint(chunks: Path, first: int, last: int, plan: Plan, headers: list[Header], rows: list[dict[str, Value]]) -> Path:
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
        path = chunks / f"{first:020d}-{last:020d}-{file_hash(temporary)}.parquet"
        os.replace(temporary, path)
        _fsync_directory(chunks)
        return path
    finally:
        temporary.unlink(missing_ok=True)


def read_checkpoint(path: Path, plan: Plan) -> list[Header]:
    try:
        match = _CHUNK.fullmatch(path.name)
        if match is None:
            raise ValueError
        _validate_checkpoint_digest(path, match)
        frame = pl.read_parquet(path)
        if frame.schema != checkpoint_schema(plan) or frame.is_empty() or frame.null_count().row(0) != (0,) * len(frame.columns):
            raise ValueError
        _validate_eager_frame(frame.select(plan.columns), plan, None)
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
        if any(_HASH.fullmatch(value) is None for header in headers for value in (header.block_hash, header.parent_hash)):
            raise ValueError
        return headers
    except Exception as error:
        raise BlockweaverError("RESUME_INVALID", f"Invalid checkpoint: {path.name}") from error


def checkpoint_facts(paths: list[Path], plan: Plan, numbers: list[int]) -> dict[int, dict[str, Value]]:
    for path in paths:
        read_checkpoint(path, plan)
    frame = pl.concat([pl.scan_parquet(path).select(plan.columns) for path in paths], how="vertical")
    selected = frame.filter(pl.col("block_number").is_in(numbers)).collect(engine="streaming")
    if selected["block_number"].to_list() != numbers:
        raise BlockweaverError("RESUME_INVALID", "Requested checkpoint rows are missing")
    return {int(row["block_number"]): {name: value for name, value in row.items()} for row in selected.iter_rows(named=True)}


def write_candidate(
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
    for path in sources:
        read_checkpoint(path, plan)
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
    source = source_definition(source_value.get("type"))
    source.validate_manifest(source_value)
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
    if schema != plan.schema_document() or manifest["acquisition_plan"] != plan.document(source.name):
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
        or output["sha256"] != file_hash(data_path)
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
    verification = source.validate_verification(manifest["verification"], chain["chain_id"])
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
    return Dataset(
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
        _manifest_json=manifest_bytes,
    )


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


def _validate_eager_frame(frame: pl.DataFrame, plan: Plan, resolved: dict[str, int] | None) -> None:
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
    del resolved


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


def save_ready(hidden: Path, candidate: Path, receipt: dict[str, object]) -> None:
    ready = hidden / "ready"
    _write_json(hidden / "receipt.json", receipt)
    if candidate != ready:
        os.rename(candidate, ready)
    _fsync_directory(hidden)


def publish(hidden: Path, destination: Path) -> None:
    ready = hidden / "ready"
    for path in ready.iterdir():
        _fsync_file(path)
    if len(list(ready.iterdir())) != 2 or not (ready / "manifest.json").is_file():
        raise BlockweaverError("PUBLICATION_FAILED", "Candidate does not contain exactly the canonical artifact pair")
    _fsync_directory(ready)
    _rename_no_replace(ready, destination)
    _fsync_directory(destination.parent)


def discard_work(hidden: Path) -> None:
    shutil.rmtree(hidden)


def pair_hashes(path: Path) -> dict[str, str]:
    return {item.name: file_hash(item) for item in sorted(path.iterdir()) if item.is_file()}


def file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_checkpoint_digest(path: Path, match: re.Match[str]) -> None:
    if file_hash(path) != match.group(3):
        raise BlockweaverError("RESUME_INVALID", f"Checkpoint digest does not match its filename: {path.name}")


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
    elif os.name == "nt":
        try:
            os.rename(source, destination)
        except FileExistsError:
            raise BlockweaverError("DESTINATION_EXISTS", f"Destination already exists: {destination}") from None
        return
    else:
        raise OSError(errno.ENOTSUP, "Atomic no-replace publication is unavailable")
    if result == 0:
        return
    code = ctypes.get_errno()
    if code in {errno.EEXIST, errno.ENOTEMPTY}:
        raise BlockweaverError("DESTINATION_EXISTS", f"Destination already exists: {destination}")
    raise OSError(code, os.strerror(code), destination)
