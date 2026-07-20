"""Canonical two-file Corpus storage and resumable checkpoints."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
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
    frame: pl.DataFrame


def canonical_paths(root: Path, corpus_id: UUID) -> tuple[Path, Path, Path]:
    directory = root / "corpora" / str(corpus_id)
    return directory, directory / "corpus.json", directory / "blocks.parquet"


def prepare_work(root: Path, request: Request, binding: dict[str, object]) -> tuple[Path, Path]:
    destination, _, _ = canonical_paths(root, request.corpus_id)
    if destination.exists():
        raise FileExistsError(f"Destination already exists: {destination}")
    parent = destination.parent
    parent.mkdir(parents=True, exist_ok=True)
    hidden = parent / f".{request.corpus_id}"
    manifest = hidden / "binding.json"
    chunks = hidden / "chunks"
    if hidden.exists():
        if not hidden.is_dir() or not manifest.is_file() or not chunks.is_dir():
            raise ValueError("Incomplete work directory has an invalid shape")
        persisted = json.loads(manifest.read_text(encoding="utf-8"))
        if persisted != binding:
            raise ValueError("Incomplete work belongs to a different command")
        for path in hidden.rglob("*.tmp"):
            path.unlink()
        return hidden, chunks
    chunks.mkdir(parents=True)
    _write_json(manifest, binding)
    _fsync_directory(hidden)
    return hidden, chunks


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
    hidden: Path,
    request: Request,
    anchor: Anchor,
    sources: list[Path],
) -> tuple[Path, Path]:
    corpus_path = hidden / "corpus.json"
    blocks_path = hidden / "blocks.parquet"
    temporary = hidden / "blocks.parquet.tmp"
    scans = [pl.scan_parquet(path).select(FINAL_COLUMNS) for path in sources]
    pl.concat(scans, how="vertical").sink_parquet(temporary, compression="zstd", row_group_size=4096, maintain_order=True)
    _fsync_file(temporary)
    os.replace(temporary, blocks_path)
    _write_json(corpus_path, {"request": request.document(), "finalized_anchor": anchor.document()})
    return corpus_path, blocks_path


def load_corpus(path: Path, *, exact_files: bool = True) -> LoadedCorpus:
    path = path.resolve()
    if not path.is_dir():
        raise ValueError(f"Corpus directory does not exist: {path}")
    if exact_files and {item.name for item in path.iterdir()} != {"corpus.json", "blocks.parquet"}:
        raise ValueError("Corpus directory must contain exactly corpus.json and blocks.parquet")
    try:
        document = json.loads((path / "corpus.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError("Invalid corpus.json") from error
    request, anchor = _parse_document(document)
    try:
        frame = pl.read_parquet(path / "blocks.parquet")
    except Exception as error:
        raise ValueError("Invalid blocks.parquet") from error
    validate_frame(frame, request)
    if anchor.block_number < request.last_block:
        raise ValueError("Finalized anchor precedes the Corpus")
    return LoadedCorpus(path, request, anchor, frame)


def validate_frame(frame: pl.DataFrame, request: Request) -> None:
    if frame.schema != FINAL_SCHEMA:
        raise ValueError("blocks.parquet has a noncanonical schema")
    if frame.null_count().row(0) != (0,) * len(FINAL_SCHEMA):
        raise ValueError("blocks.parquet contains nulls")
    if frame.height != request.last_block - request.first_block + 1:
        raise ValueError("blocks.parquet row count does not match request")
    numbers = frame["block_number"]
    if numbers.to_list() != list(range(request.first_block, request.last_block + 1)):
        raise ValueError("Block numbers are not the requested contiguous range")
    invalid = frame.select(
        (
            (pl.col("chain_id") != request.chain_id)
            | (pl.col("timestamp") < 0)
            | (pl.col("base_fee_per_gas") <= 0)
            | (pl.col("gas_used") < 0)
            | (pl.col("gas_limit") <= 0)
            | (pl.col("gas_used") > pl.col("gas_limit"))
            | (pl.col("tx_count") < 0)
        ).any()
    ).item()
    if invalid or not frame["timestamp"].is_sorted():
        raise ValueError("blocks.parquet contains invalid block values")


def pair_hashes(path: Path) -> dict[str, str]:
    return {name: hashlib.sha256((path / name).read_bytes()).hexdigest() for name in ("corpus.json", "blocks.parquet")}


def publish(hidden: Path, destination: Path) -> None:
    for name in ("corpus.json", "blocks.parquet"):
        _fsync_file(hidden / name)
    shutil.rmtree(hidden / "chunks")
    (hidden / "binding.json").unlink()
    if {item.name for item in hidden.iterdir()} != {"corpus.json", "blocks.parquet"}:
        raise ValueError("Candidate contains unexpected files")
    _fsync_directory(hidden)
    os.rename(hidden, destination)
    _fsync_directory(destination.parent)


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
