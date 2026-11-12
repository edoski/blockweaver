"""The public reader: strict validation of one published dataset pair."""

from __future__ import annotations

import csv
import hashlib
import json
import re
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

import polars as pl

from ._contract import (
    BLOCK_HASH,
    HASH,
    HASH_PATTERN,
    HEX_PATTERN,
    NAME,
    TIMESTAMP,
    Anchor,
    BlockweaverError,
    Domain,
    Feature,
    OutputFormat,
    Plan,
    Row,
    Source,
    plan_features,
    validate_dataset_identifier,
)

_DECIMAL = re.compile(r"0|[1-9][0-9]*\Z")
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


@dataclass(frozen=True, slots=True)
class PairFingerprint:
    manifest_sha256: str
    data_name: str
    data_bytes: int
    data_sha256: str

    def hashes(self) -> dict[str, str]:
        return {"manifest.json": self.manifest_sha256, self.data_name: self.data_sha256}


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

    def __new__(cls, token: object = None) -> Dataset:
        if token is not _DATASET_TOKEN:
            raise TypeError("Dataset objects must be created by open_dataset()")
        return object.__new__(cls)

    def __init__(self, token: object = None) -> None:
        del token


@dataclass(frozen=True, slots=True)
class Artifact:
    """A validated dataset together with the manifest facts needed to verify it against RPC or to publish it."""

    dataset: Dataset
    manifest: dict[str, Any]
    plan: Plan
    anchor: Anchor
    fingerprint: PairFingerprint

    @property
    def target_hash(self) -> str:
        return self.manifest["target_hash"]

    def assert_unchanged(self) -> None:
        try:
            current = _fingerprint_pair(self.dataset.path, self.fingerprint.data_name)
        except (OSError, ValueError):
            current = None
        if current != self.fingerprint:
            raise BlockweaverError("ARTIFACT_INVALID", "Dataset bytes changed during verification")

    def rows(self, numbers: list[int]) -> dict[int, Row]:
        frame = _scan_data(self.dataset.data_path, self.plan, self.dataset.output_format)
        selected = frame.filter(pl.col("block_number").is_in(numbers)).collect(engine="streaming")
        if selected["block_number"].to_list() != numbers:
            raise BlockweaverError("ARTIFACT_INVALID", "Requested dataset rows are missing")
        return {int(row["block_number"]): row for row in selected.iter_rows(named=True)}

    def row_chunks(self, size: int) -> Iterator[dict[int, Row]]:
        batches = _scan_data(self.dataset.data_path, self.plan, self.dataset.output_format).collect_batches(
            chunk_size=size,
            maintain_order=True,
            engine="streaming",
        )
        for batch in batches:
            for frame in batch.iter_slices(n_rows=size):
                yield {int(row["block_number"]): row for row in frame.iter_rows(named=True)}


def validate_manifest_source(value: object) -> Source:
    if not isinstance(value, dict):
        raise ValueError("Invalid manifest source")
    source = value.get("type")
    if source == "rpc":
        if set(value) != {"type", "provider", "verifier"} or any(
            not isinstance(value[field], str) or NAME.fullmatch(value[field]) is None for field in ("provider", "verifier")
        ):
            raise ValueError("Invalid manifest source")
        return "rpc"
    if source == "bigquery":
        if (
            set(value) != {"type", "dataset", "verifier"}
            or not isinstance(value["dataset"], str)
            or not isinstance(value["verifier"], str)
            or NAME.fullmatch(value["verifier"]) is None
        ):
            raise ValueError("Invalid manifest source")
        validate_dataset_identifier(value["dataset"])
        return "bigquery"
    raise ValueError("Invalid manifest source")


def validate_verification(value: object, chain_id: int, source: Source) -> dict[str, object]:
    chain_fields = {"primary_chain_id", "verifier_chain_id"} if source == "rpc" else {"verifier_chain_id"}
    nonnegative_fields = set() if source == "rpc" else {"dry_run_bytes"}
    fields = {"target_agreement", "sampled_blocks", *chain_fields, *nonnegative_fields}
    if not isinstance(value, dict) or set(value) != fields:
        raise ValueError("Invalid verification facts")
    if value["target_agreement"] is not True or any(value[field] != chain_id for field in chain_fields):
        raise ValueError("Invalid verification facts")
    if any(type(value[field]) is not int or value[field] < 0 for field in nonnegative_fields):
        raise ValueError("Invalid verification facts")
    return value


def open_dataset(path: str | Path) -> Dataset:
    """Open and strictly validate a published Blockweaver dataset."""

    return read_artifact(Path(path), work=False).dataset


def read_artifact(path: Path, *, work: bool) -> Artifact:
    try:
        return _read_artifact(path, work=work)
    except BlockweaverError as error:
        raise BlockweaverError("ARTIFACT_INVALID", str(error)) from None
    except Exception as error:
        raise BlockweaverError("ARTIFACT_INVALID", str(error) or "Invalid dataset") from None


def _read_artifact(path: Path, *, work: bool) -> Artifact:
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
    manifest = decode_json(manifest_bytes, "manifest.json")
    if manifest_bytes != canonical_json(manifest):
        raise ValueError("manifest.json is not canonical JSON")
    if set(manifest) != _MANIFEST_KEYS:
        raise ValueError("manifest.json has a noncanonical shape")
    if not isinstance(manifest["tool_version"], str) or not manifest["tool_version"]:
        raise ValueError("Invalid tool version")
    dataset_id = _canonical_uuid(manifest["dataset_id"])
    chain = _exact_table(manifest["chain"], {"name", "chain_id"}, "chain")
    if not isinstance(chain["name"], str) or NAME.fullmatch(chain["name"]) is None or type(chain["chain_id"]) is not int or chain["chain_id"] <= 0:
        raise ValueError("Invalid manifest chain")
    source_value = manifest["source"]
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
    if schema != plan.schema_document():
        raise ValueError("Schema is not canonical")
    _validate_acquisition_plan(manifest["acquisition_plan"])
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
    if HASH.fullmatch(anchor["block_hash"]) is None or anchor["tag"] not in {"finalized", "safe"}:
        raise ValueError("Invalid finalized anchor")
    if not isinstance(manifest["target_hash"], str) or HASH.fullmatch(manifest["target_hash"]) is None:
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
    dataset = Dataset(_DATASET_TOKEN)
    facts = {
        "path": path,
        "dataset_id": dataset_id,
        "chain_name": chain["name"],
        "chain_id": chain["chain_id"],
        "first_block": resolved["from_block"],
        "last_block": resolved["to_block"],
        "first_timestamp": resolved["from_timestamp"],
        "last_timestamp": resolved["to_timestamp"],
        "schema": tuple(names),
        "output_format": output["format"],
        "row_count": rows,
        "data_path": data_path,
    }
    for name, value in facts.items():
        object.__setattr__(dataset, name, value)
    return Artifact(dataset, manifest, plan, Anchor(anchor["block_number"], anchor["block_hash"], anchor["tag"]), fingerprint)


def _validate_acquisition_plan(value: object) -> None:
    """Accept any well-formed plan. It records how the rows were acquired, by whichever planner wrote the dataset."""
    families = _exact_table(value, {"families"}, "acquisition plan")["families"]
    if not isinstance(families, list) or not families:
        raise ValueError("Invalid acquisition plan")
    for family in families:
        if not isinstance(family, dict) or not isinstance(family.get("family"), str) or not family["family"]:
            raise ValueError("Invalid acquisition plan family")
        for detail in family.values():
            members = detail if isinstance(detail, list) else [detail]
            if any(type(member) not in {str, int} for member in members):
                raise ValueError("Invalid acquisition plan family")


def _validate_data(path: Path, plan: Plan, output_format: str, resolved: dict[str, int], target_hash: str) -> int:
    if output_format == "parquet":
        if pl.read_parquet_schema(path) != polars_schema(plan.schema):
            raise ValueError("Parquet schema is not canonical")
    else:
        _validate_csv_tokens(path, plan)
    return validate_rows(
        _scan_data(path, plan, output_format),
        plan.features,
        first=resolved["from_block"],
        last=resolved["to_block"],
        first_timestamp=resolved["from_timestamp"],
        last_timestamp=resolved["to_timestamp"],
        target_hash=target_hash,
    )


def validate_rows(
    frame: pl.LazyFrame,
    features: tuple[Feature, ...],
    *,
    first: int,
    last: int,
    first_timestamp: int | None = None,
    last_timestamp: int | None = None,
    target_hash: str | None = None,
) -> int:
    names = {feature.name for feature in features}
    invalid = pl.lit(False)
    for feature in features:
        column = pl.col(feature.name)
        invalid |= _outside(feature.domain, column)
        if feature.at_most in names:
            invalid |= column > pl.col(feature.at_most)
        if feature.nondecreasing:
            invalid |= (column.diff() < 0).fill_null(False)
    expressions = [
        pl.len().alias("rows"),
        pl.col("block_number").first().alias("first"),
        pl.col("block_number").last().alias("last"),
        (pl.col("block_number").diff() != 1).fill_null(False).any().alias("gaps"),
        invalid.any().alias("invalid"),
        pl.sum_horizontal(*(pl.col(name).null_count() for name in ("block_number", *names))).alias("nulls"),
    ]
    if TIMESTAMP in features:
        expressions.extend([pl.col(TIMESTAMP.name).first().alias("first_timestamp"), pl.col(TIMESTAMP.name).last().alias("last_timestamp")])
    if BLOCK_HASH in features:
        expressions.append(pl.col(BLOCK_HASH.name).last().alias("target_hash"))
    summary = frame.select(*expressions).collect(engine="streaming").row(0, named=True)
    expected_rows = last - first + 1
    if summary["rows"] != expected_rows or summary["first"] != first or summary["last"] != last or summary["gaps"]:
        raise ValueError("Data rows are not the resolved contiguous range")
    if summary["invalid"] or summary["nulls"]:
        raise ValueError("Data contains invalid or null values")
    if TIMESTAMP in features and (
        (first_timestamp is not None and summary["first_timestamp"] != first_timestamp)
        or (last_timestamp is not None and summary["last_timestamp"] != last_timestamp)
    ):
        raise ValueError("Data timestamps do not match the resolved range")
    if BLOCK_HASH in features and target_hash is not None and summary["target_hash"] != target_hash:
        raise ValueError("Data target hash does not match the manifest")
    return expected_rows


def _outside(domain: Domain, column: pl.Expr) -> pl.Expr:
    if domain in {Domain.HASH, Domain.HEX}:
        return ~column.str.contains(f"^{HASH_PATTERN if domain is Domain.HASH else HEX_PATTERN}$")
    return column <= 0 if domain is Domain.POSITIVE else column < 0


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
    return pl.scan_csv(path, schema=polars_schema(plan.schema))


def polars_schema(schema: dict[str, str]):
    return {name: pl.Int64 if dtype == "Int64" else pl.String for name, dtype in schema.items()}


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


def decode_json(data: bytes, name: str) -> dict[str, Any]:
    try:
        value = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"Invalid JSON file: {name}") from error
    if not isinstance(value, dict):
        raise ValueError(f"Invalid JSON object: {name}")
    return value


def canonical_json(value: object) -> bytes:
    return (json.dumps(value, ensure_ascii=True, allow_nan=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
