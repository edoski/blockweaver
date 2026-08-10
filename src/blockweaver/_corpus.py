"""Canonical two-file dataset storage and resumable work state."""

from __future__ import annotations

import csv
import fcntl
import hashlib
import json
import os
import re
import shutil
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

import polars as pl

from ._contract import Anchor, BlockweaverError, Header, Plan, ResolvedRange, Value, format_utc, plan_features, validate_links

_CHUNK = re.compile(r"(\d{20})-(\d{20})\.parquet\Z")
_HASH = re.compile(r"0x[0-9a-f]{64}\Z")
_DECIMAL = re.compile(r"0|[1-9][0-9]*\Z")
_NAME = re.compile(r"[a-z0-9][a-z0-9_-]*\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MANIFEST_KEYS = {
    "manifest_version",
    "dataset_version",
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


@dataclass(frozen=True, slots=True)
class LoadedDataset:
    path: Path
    manifest: dict[str, Any]
    plan: Plan
    rows: int
    data_path: Path

    @property
    def chain_id(self) -> int:
        return int(self.manifest["chain"]["chain_id"])

    @property
    def first_block(self) -> int:
        return int(self.manifest["resolved_range"]["from_block"])

    @property
    def last_block(self) -> int:
        return int(self.manifest["resolved_range"]["to_block"])

    @property
    def anchor(self) -> Anchor:
        value = self.manifest["finalized_anchor"]
        return Anchor(value["block_number"], value["block_hash"], value["tag"])

    def facts(self, numbers: list[int]) -> dict[int, dict[str, Value]]:
        frame = _scan_data(self.data_path, self.plan, self.manifest["output"]["format"])
        selected = frame.filter(pl.col("block_number").is_in(numbers)).collect(engine="streaming")
        if selected["block_number"].to_list() != numbers:
            raise BlockweaverError("ARTIFACT_INVALID", "Requested dataset rows are missing")
        return {int(row["block_number"]): {name: value for name, value in row.items()} for row in selected.iter_rows(named=True)}


@dataclass(frozen=True, slots=True)
class WorkState:
    chunks: Path
    candidate: Path | None = None
    receipt: dict[str, Any] | None = None
    published: bool = False


def dataset_path(root: Path, chain: str, resolved: ResolvedRange, dataset_id: UUID) -> Path:
    return root / f"{chain}-{format_utc(resolved.first_timestamp, filename=True)}-{dataset_id}"


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
            dataset = load_dataset(destination)
            receipt = _read_json(receipt_path) if receipt_path.is_file() else None
            if receipt is not None and receipt.get("artifact_sha256") != pair_hashes(destination):
                raise BlockweaverError("RESUME_INVALID", "Published recovery receipt does not match the dataset")
            return WorkState(chunks, dataset.path, receipt, True)
        if ready.exists():
            try:
                dataset = load_dataset(ready, work=True)
                receipt = _read_json(receipt_path) if receipt_path.is_file() else None
                if receipt is not None and receipt.get("artifact_sha256") != pair_hashes(ready):
                    raise ValueError
            except (BlockweaverError, OSError, ValueError):
                shutil.rmtree(ready) if ready.is_dir() else ready.unlink()
                receipt_path.unlink(missing_ok=True)
            else:
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


def write_checkpoint(path: Path, plan: Plan, headers: list[Header], rows: list[dict[str, Value]]) -> None:
    temporary = path.with_suffix(".parquet.tmp")
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
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def read_checkpoint(path: Path, plan: Plan) -> list[Header]:
    try:
        frame = pl.read_parquet(path)
        if frame.schema != checkpoint_schema(plan) or frame.is_empty() or frame.null_count().row(0) != (0,) * len(frame.columns):
            raise ValueError
        _validate_eager_frame(frame.select(plan.columns), plan, None)
        headers = [
            Header(row["block_number"], row["_proof_hash"], row["_proof_parent_hash"], row["_proof_timestamp"], {}) for row in frame.iter_rows(named=True)
        ]
        if any(_HASH.fullmatch(value) is None for header in headers for value in (header.block_hash, header.parent_hash)):
            raise ValueError
        return headers
    except Exception as error:
        raise BlockweaverError("RESUME_INVALID", f"Invalid checkpoint: {path.name}") from error


def checkpoint_facts(paths: list[Path], plan: Plan, numbers: list[int]) -> dict[int, dict[str, Value]]:
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
) -> LoadedDataset:
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
    _write_json(candidate / "manifest.json", {**manifest, "output": output})
    _fsync_directory(candidate)
    return load_dataset(candidate, work=True)


def load_dataset(path: Path, *, work: bool = False) -> LoadedDataset:
    try:
        return _load_dataset(path, work=work)
    except BlockweaverError as error:
        raise BlockweaverError("ARTIFACT_INVALID", str(error)) from None
    except Exception as error:
        raise BlockweaverError("ARTIFACT_INVALID", str(error) or "Invalid dataset") from None


def _load_dataset(path: Path, *, work: bool) -> LoadedDataset:
    path = path.resolve()
    if not path.is_dir():
        raise ValueError(f"Dataset directory does not exist: {path}")
    manifest_path = path / "manifest.json"
    if not manifest_path.is_file():
        raise ValueError("Dataset must contain manifest.json")
    manifest = _read_json(manifest_path)
    if set(manifest) != _MANIFEST_KEYS:
        raise ValueError("manifest.json has a noncanonical shape")
    if manifest["manifest_version"] != 1 or manifest["dataset_version"] != 1 or not isinstance(manifest["tool_version"], str) or not manifest["tool_version"]:
        raise ValueError("Unsupported manifest version")
    dataset_id = _canonical_uuid(manifest["dataset_id"])
    chain = _exact_table(manifest["chain"], {"name", "chain_id"}, "chain")
    if not isinstance(chain["name"], str) or _NAME.fullmatch(chain["name"]) is None or type(chain["chain_id"]) is not int or chain["chain_id"] <= 0:
        raise ValueError("Invalid manifest chain")
    source = _exact_table(manifest["source"], {"type", "provider", "verifier"}, "source")
    if source["type"] != "rpc" or any(not isinstance(source[key], str) or _NAME.fullmatch(source[key]) is None for key in ("provider", "verifier")):
        raise ValueError("Invalid manifest source")
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
    if schema != plan.schema_document() or manifest["acquisition_plan"] != plan.document():
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
    verification = _exact_table(
        manifest["verification"],
        {"primary_chain_id", "verifier_chain_id", "target_agreement", "sampled_blocks"},
        "verification",
    )
    if (
        verification["primary_chain_id"] != chain["chain_id"]
        or verification["verifier_chain_id"] != chain["chain_id"]
        or verification["target_agreement"] is not True
    ):
        raise ValueError("Invalid verification facts")
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
    expected_name = f"{chain['name']}-{format_utc(resolved['from_timestamp'], filename=True)}-{dataset_id}"
    allowed_names = {expected_name, "ready", "ready.tmp"} if work else {expected_name}
    if path.name not in allowed_names:
        raise ValueError("Dataset directory name does not match its manifest")
    rows = _validate_data(data_path, plan, output["format"], resolved, manifest["target_hash"])
    if type(manifest["row_count"]) is not int or manifest["row_count"] != rows:
        raise ValueError("Manifest row count does not match data")
    return LoadedDataset(path, manifest, plan, rows, data_path)


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
    os.rename(ready, destination)
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
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"Invalid JSON file: {path.name}") from error
    if not isinstance(value, dict):
        raise ValueError(f"Invalid JSON object: {path.name}")
    return value


def _write_json(path: Path, value: object) -> None:
    data = json.dumps(value, ensure_ascii=True, allow_nan=False, sort_keys=True, separators=(",", ":")) + "\n"
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        temporary.write_text(data, encoding="utf-8")
        _fsync_file(temporary)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _fsync_file(path: Path) -> None:
    with path.open("rb") as stream:
        os.fsync(stream.fileno())


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
