"""Canonical two-file Corpus storage and resumable checkpoints."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import shutil
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID

import polars as pl

from ._contract import Anchor, Block, Request, validate_links

FINAL_COLUMNS = [
    "block_number",
    "timestamp",
    "chain_id",
    "base_fee_per_gas",
    "gas_used",
    "gas_limit",
    "tx_count",
]
FINAL_SCHEMA = {name: pl.Int64 for name in FINAL_COLUMNS}
CHECKPOINT_SCHEMA = {
    **FINAL_SCHEMA,
    "block_hash": pl.String,
    "parent_hash": pl.String,
}
_CHUNK = re.compile(r"(\d{20})-(\d{20})\.parquet\Z")


@dataclass(frozen=True, slots=True)
class LoadedCorpus:
    path: Path
    request: Request
    anchor: Anchor
    rows: int

    @property
    def blocks_path(self) -> Path:
        return self.path / "blocks.parquet"

    def fact(self, number: int) -> dict[str, object]:
        return self.facts([number])[number]

    def facts(self, numbers: list[int]) -> dict[int, dict[str, object]]:
        frame = pl.scan_parquet(self.blocks_path).filter(pl.col("block_number").is_in(numbers)).collect(engine="streaming")
        if frame["block_number"].to_list() != numbers:
            raise ValueError("Requested Corpus rows are missing")
        return {int(row["block_number"]): {name: int(value) for name, value in row.items()} for row in frame.iter_rows(named=True)}


@dataclass(frozen=True, slots=True)
class WorkState:
    chunks: Path
    candidate: Path | None = None
    receipt: dict[str, object] | None = None
    published: bool = False


def corpus_path(root: Path, corpus_id: UUID) -> Path:
    return root / "corpora" / str(corpus_id)


@contextmanager
def locked_work(root: Path, corpus_id: UUID) -> Iterator[Path]:
    destination = corpus_path(root, corpus_id)
    parent = destination.parent
    hidden = parent / f".{corpus_id}"
    if destination.exists() and not hidden.exists():
        raise FileExistsError(f"Destination already exists: {destination}")
    parent.mkdir(parents=True, exist_ok=True)
    hidden.mkdir(exist_ok=True)
    descriptor = os.open(hidden, os.O_RDONLY)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        if destination.exists() and not hidden.exists():
            raise FileExistsError(f"Destination already exists: {destination}")
        yield hidden
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def prepare_work(hidden: Path, destination: Path, request: Request, binding: dict[str, object]) -> WorkState:
    manifest = hidden / "binding.json"
    chunks = hidden / "chunks"
    ready = hidden / "ready"
    receipt_path = hidden / "receipt.json"
    if destination.exists():
        candidate = load_corpus(destination)
        if candidate.request != request:
            raise ValueError("Published recovery does not match the command")
        if manifest.is_file():
            _validate_binding(manifest, binding)
        receipt = _read_json(receipt_path) if receipt_path.is_file() else None
        if receipt is not None:
            _validate_receipt(receipt, destination, destination, request, binding)
        return WorkState(chunks, destination, receipt, True)
    for path in hidden.rglob("*.tmp"):
        shutil.rmtree(path) if path.is_dir() else path.unlink()
    if manifest.is_file() and chunks.is_dir():
        _validate_binding(manifest, binding)
        if ready.exists():
            try:
                candidate = load_corpus(ready)
                if candidate.request != request:
                    raise ValueError("Ready candidate does not match the command")
                receipt = _read_json(receipt_path) if receipt_path.is_file() else None
                if receipt is not None:
                    _validate_receipt(receipt, ready, destination, request, binding)
            except (OSError, ValueError):
                shutil.rmtree(ready) if ready.is_dir() else ready.unlink()
                receipt_path.unlink(missing_ok=True)
            else:
                return WorkState(chunks, ready, receipt)
        receipt_path.unlink(missing_ok=True)
        return WorkState(chunks)
    for path in hidden.iterdir():
        shutil.rmtree(path) if path.is_dir() else path.unlink()
    chunks.mkdir(parents=True)
    _write_json(manifest, binding)
    _fsync_directory(hidden)
    return WorkState(chunks)


def checkpoint_paths(chunks: Path, request: Request, size: int) -> tuple[list[Path], int, Block | None]:
    parsed: list[tuple[int, int, Path]] = []
    for path in chunks.iterdir():
        match = _CHUNK.fullmatch(path.name)
        if match is None:
            raise ValueError(f"Unexpected checkpoint file: {path.name}")
        parsed.append((int(match.group(1)), int(match.group(2)), path))
    parsed.sort()
    expected = request.first_block
    previous: Block | None = None
    valid: list[Path] = []
    for first, last, path in parsed:
        expected_last = min(expected + size - 1, request.last_block)
        if (first, last) != (expected, expected_last):
            raise ValueError("Checkpoints are not a deterministic complete prefix")
        blocks = read_checkpoint(path, request.chain_id)
        if [item.block_number for item in blocks] != list(range(first, last + 1)):
            raise ValueError("Checkpoint range does not match its filename")
        validate_links(blocks, previous)
        previous = blocks[-1]
        valid.append(path)
        expected = last + 1
    return valid, expected, previous


def write_checkpoint(path: Path, blocks: list[Block]) -> None:
    temporary = path.with_suffix(".parquet.tmp")
    try:
        frame = pl.DataFrame([item.checkpoint_row() for item in blocks], schema=CHECKPOINT_SCHEMA)
        frame.write_parquet(temporary, compression="zstd", row_group_size=4096)
        _fsync_file(temporary)
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def read_checkpoint(path: Path, chain_id: int) -> list[Block]:
    try:
        frame = pl.read_parquet(path)
    except Exception as error:
        raise ValueError(f"Unreadable checkpoint: {path.name}") from error
    if frame.schema != CHECKPOINT_SCHEMA or frame.null_count().row(0) != (0,) * len(CHECKPOINT_SCHEMA):
        raise ValueError("Invalid checkpoint schema")
    blocks = [Block(**row) for row in frame.iter_rows(named=True)]
    if any(re.fullmatch(r"[0-9a-f]{64}", value) is None for item in blocks for value in (item.block_hash, item.parent_hash)):
        raise ValueError("Invalid checkpoint hash")
    if any(item.chain_id != chain_id for item in blocks) or not blocks:
        raise ValueError("Invalid checkpoint chain")
    return blocks


def write_candidate(
    candidate: Path,
    request: Request,
    anchor: Anchor,
    sources: list[Path],
) -> None:
    candidate.mkdir()
    corpus_path = candidate / "corpus.json"
    blocks_path = candidate / "blocks.parquet"
    temporary = candidate / "blocks.parquet.tmp"
    scans = [pl.scan_parquet(path).select(FINAL_COLUMNS) for path in sources]
    pl.concat(scans, how="vertical").sink_parquet(temporary, compression="zstd", row_group_size=4096, maintain_order=True)
    _fsync_file(temporary)
    os.replace(temporary, blocks_path)
    _write_json(corpus_path, {"request": request.document(), "finalized_anchor": anchor.document()})
    _fsync_directory(candidate)


def load_corpus(path: Path) -> LoadedCorpus:
    path = path.resolve()
    if not path.is_dir():
        raise ValueError(f"Corpus directory does not exist: {path}")
    if {item.name for item in path.iterdir()} != {"corpus.json", "blocks.parquet"}:
        raise ValueError("Corpus directory must contain exactly corpus.json and blocks.parquet")
    try:
        document = json.loads((path / "corpus.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError("Invalid corpus.json") from error
    request, anchor = _parse_document(document)
    rows = validate_blocks(path / "blocks.parquet", request)
    if anchor.block_number < request.last_block:
        raise ValueError("Finalized anchor precedes the Corpus")
    return LoadedCorpus(path, request, anchor, rows)


def validate_blocks(path: Path, request: Request) -> int:
    try:
        schema = pl.read_parquet_schema(path)
        scan = pl.scan_parquet(path)
        invalid = (
            (pl.col("chain_id") != request.chain_id)
            | (pl.col("timestamp") < 0)
            | (pl.col("base_fee_per_gas") <= 0)
            | (pl.col("gas_used") < 0)
            | (pl.col("gas_limit") <= 0)
            | (pl.col("gas_used") > pl.col("gas_limit"))
            | (pl.col("tx_count") < 0)
        )
        summary = (
            scan.select(
                pl.len().alias("rows"),
                pl.col("block_number").first().alias("first"),
                pl.col("block_number").last().alias("last"),
                (pl.col("block_number").diff() != 1).fill_null(False).any().alias("gaps"),
                (pl.col("timestamp").diff() < 0).fill_null(False).any().alias("time_decreases"),
                invalid.any().alias("invalid"),
                pl.sum_horizontal(*(pl.col(name).null_count() for name in FINAL_COLUMNS)).alias("nulls"),
            )
            .collect(engine="streaming")
            .row(0, named=True)
        )
    except Exception as error:
        raise ValueError("Invalid blocks.parquet") from error
    if schema != FINAL_SCHEMA:
        raise ValueError("blocks.parquet has a noncanonical schema")
    if summary["nulls"]:
        raise ValueError("blocks.parquet contains nulls")
    expected_rows = request.last_block - request.first_block + 1
    if summary["rows"] != expected_rows:
        raise ValueError("blocks.parquet row count does not match request")
    if summary["first"] != request.first_block or summary["last"] != request.last_block or summary["gaps"]:
        raise ValueError("Block numbers are not the requested contiguous range")
    if summary["invalid"] or summary["time_decreases"]:
        raise ValueError("blocks.parquet contains invalid block values")
    return expected_rows


def pair_hashes(path: Path) -> dict[str, str]:
    hashes: dict[str, str] = {}
    for name in ("corpus.json", "blocks.parquet"):
        digest = hashlib.sha256()
        with (path / name).open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
        hashes[name] = digest.hexdigest()
    return hashes


def save_ready(hidden: Path, candidate: Path, receipt: dict[str, object]) -> None:
    ready = hidden / "ready"
    _write_json(hidden / "receipt.json", receipt)
    if candidate != ready:
        os.rename(candidate, ready)
    _fsync_directory(hidden)


def publish(hidden: Path, destination: Path) -> None:
    ready = hidden / "ready"
    for name in ("corpus.json", "blocks.parquet"):
        _fsync_file(ready / name)
    if {item.name for item in ready.iterdir()} != {"corpus.json", "blocks.parquet"}:
        raise ValueError("Candidate contains unexpected files")
    _fsync_directory(ready)
    os.rename(ready, destination)
    _fsync_directory(destination.parent)


def discard_work(hidden: Path) -> None:
    shutil.rmtree(hidden)


def _validate_binding(path: Path, binding: dict[str, object]) -> None:
    if _read_json(path) != binding:
        raise ValueError("Incomplete work belongs to a different command")


def _validate_receipt(
    receipt: dict[str, object],
    candidate: Path,
    destination: Path,
    request: Request,
    binding: dict[str, object],
) -> None:
    expected = {
        "operation": binding["operation"],
        "corpus_id": str(request.corpus_id),
        "path": str(destination),
        "chain_id": request.chain_id,
        "first_block": request.first_block,
        "last_block": request.last_block,
        "rows": request.last_block - request.first_block + 1,
    }
    if any(receipt.get(name) != value for name, value in expected.items()):
        raise ValueError("Recovery receipt does not match the command")
    if receipt.get("pair_sha256") != pair_hashes(candidate):
        raise ValueError("Recovery receipt does not match the Corpus")


def _read_json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"Invalid work state: {path.name}") from error
    if not isinstance(value, dict):
        raise ValueError(f"Invalid work state: {path.name}")
    return value


def _parse_document(value: Any) -> tuple[Request, Anchor]:
    if not isinstance(value, dict) or set(value) != {"request", "finalized_anchor"}:
        raise ValueError("corpus.json has a noncanonical shape")
    request = value["request"]
    anchor = value["finalized_anchor"]
    if not isinstance(request, dict) or set(request) != {"corpus_id", "definition"}:
        raise ValueError("Invalid Corpus request")
    definition = request["definition"]
    if not isinstance(definition, dict) or set(definition) != {"chain_id", "first_block", "last_block"}:
        raise ValueError("Invalid Corpus definition")
    if any(type(definition[key]) is not int for key in definition):
        raise ValueError("Corpus definition values must be integers")
    if not isinstance(request["corpus_id"], str):
        raise ValueError("Corpus ID must be a string")
    try:
        parsed_request = Request(UUID(request["corpus_id"]), **definition)
    except (TypeError, ValueError) as error:
        raise ValueError("Invalid Corpus request") from error
    if not isinstance(anchor, dict) or set(anchor) != {"block_number", "block_hash"}:
        raise ValueError("Invalid finalized anchor")
    if type(anchor["block_number"]) is not int or not isinstance(anchor["block_hash"], str):
        raise ValueError("Invalid finalized anchor")
    if anchor["block_number"] < 0 or re.fullmatch(r"[0-9a-f]{64}", anchor["block_hash"]) is None:
        raise ValueError("Invalid finalized anchor")
    return parsed_request, Anchor(**anchor)


def _write_json(path: Path, value: object) -> None:
    data = json.dumps(value, ensure_ascii=True, allow_nan=False, sort_keys=True, separators=(",", ":"))
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
