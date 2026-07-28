"""Acquire, extend, verify, and publish Corpus objects."""

from __future__ import annotations

import hashlib
import shutil
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from itertools import islice
from pathlib import Path
from typing import Literal, cast
from uuid import UUID

import polars as pl
from google.cloud import bigquery

from ._contract import Anchor, Block, Request, validate_links
from ._corpus import (
    FINAL_SCHEMA,
    LoadedCorpus,
    checkpoint_paths,
    corpus_path,
    discard_work,
    load_corpus,
    locked_work,
    pair_hashes,
    prepare_work,
    publish,
    read_checkpoint,
    save_ready,
    write_candidate,
    write_checkpoint,
)
from ._rpc import Rpc

Progress = Callable[[dict[str, object]], None]
Publication = Callable[[Literal["publishing", "committed"]], None]
_CHECKPOINT_SIZE = 1024
_AVALANCHE_CHAIN_ID = 43_114
_AVALANCHE_DATASET = "bigquery-public-data.goog_blockchain_avalanche_contract_chain_us"
_AVALANCHE_QUERY = f"""
WITH requested_blocks AS (
  SELECT block_number, block_timestamp, block_hash, parent_hash, base_fee_per_gas, gas_used, gas_limit
  FROM `{_AVALANCHE_DATASET}.blocks`
  WHERE block_timestamp >= TIMESTAMP_SECONDS(@first_timestamp)
    AND block_timestamp < TIMESTAMP_SECONDS(@after_timestamp)
    AND block_number BETWEEN @first_block AND @last_block
),
requested_receipts AS (
  SELECT block_hash, transaction_index, gas_used, effective_gas_price
  FROM `{_AVALANCHE_DATASET}.receipts`
  WHERE block_timestamp >= TIMESTAMP_SECONDS(@first_timestamp)
    AND block_timestamp < TIMESTAMP_SECONDS(@after_timestamp)
),
weighted AS (
  SELECT
    b.block_number,
    r.transaction_index,
    r.gas_used,
    r.effective_gas_price - b.base_fee_per_gas AS priority_fee,
    b.gas_used AS block_gas_used,
    COUNT(*) OVER (PARTITION BY b.block_number) AS tx_count,
    SUM(r.gas_used) OVER (PARTITION BY b.block_number) AS receipt_gas_used,
    SUM(r.gas_used) OVER (
      PARTITION BY b.block_number
      ORDER BY r.effective_gas_price - b.base_fee_per_gas, r.transaction_index
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_gas_used
  FROM requested_blocks AS b
  JOIN requested_receipts AS r USING (block_hash)
),
fees AS (
  SELECT
    block_number,
    ANY_VALUE(tx_count) AS tx_count,
    ANY_VALUE(receipt_gas_used) AS receipt_gas_used,
    ARRAY_AGG(
      IF(cumulative_gas_used >= DIV(block_gas_used, 2), priority_fee, NULL)
      IGNORE NULLS
      ORDER BY priority_fee, transaction_index
      LIMIT 1
    )[OFFSET(0)] AS priority_fee_p50,
    ARRAY_AGG(
      IF(cumulative_gas_used >= DIV(block_gas_used * 9, 10), priority_fee, NULL)
      IGNORE NULLS
      ORDER BY priority_fee, transaction_index
      LIMIT 1
    )[OFFSET(0)] AS priority_fee_p90
  FROM weighted
  GROUP BY block_number
)
SELECT
  b.block_number,
  UNIX_SECONDS(b.block_timestamp) AS timestamp,
  {_AVALANCHE_CHAIN_ID} AS chain_id,
  b.base_fee_per_gas,
  b.gas_used,
  b.gas_limit,
  COALESCE(f.tx_count, 0) AS tx_count,
  COALESCE(f.receipt_gas_used, 0) AS receipt_gas_used,
  COALESCE(f.priority_fee_p50, 0) AS effective_priority_fee_per_gas_p50,
  COALESCE(f.priority_fee_p90, 0) AS effective_priority_fee_per_gas_p90,
  b.block_hash,
  b.parent_hash
FROM requested_blocks AS b
LEFT JOIN fees AS f USING (block_number)
ORDER BY block_number
"""


@dataclass(frozen=True, slots=True)
class ExtensionSource:
    corpus: LoadedCorpus
    hashes: dict[str, str]

    def ensure_unchanged(self) -> None:
        if pair_hashes(self.corpus.path) != self.hashes:
            raise ValueError("Source Corpus changed during extension")


async def extend_corpus(
    source_path: Path,
    *,
    storage_root: Path,
    corpus_id: UUID,
    last_block: int,
    rpc_url: str,
    verify_rpc_url: str,
    batch_size: int,
    concurrency: int,
    progress: Progress,
    publication: Publication,
) -> dict[str, object]:
    source_path = source_path.resolve()
    source_hashes = pair_hashes(source_path)
    corpus = load_corpus(source_path)
    if pair_hashes(source_path) != source_hashes:
        raise ValueError("Source Corpus changed during validation")
    if corpus_id == corpus.request.corpus_id:
        raise ValueError("Extension requires a new corpus_id")
    if last_block <= corpus.request.last_block:
        raise ValueError("Extension last_block must exceed the source endpoint")
    request = Request(
        corpus_id,
        corpus.request.chain_id,
        corpus.request.first_block,
        last_block,
    )
    return await acquire_corpus(
        request,
        storage_root=storage_root,
        rpc_url=rpc_url,
        verify_rpc_url=verify_rpc_url,
        batch_size=batch_size,
        concurrency=concurrency,
        progress=progress,
        extension=ExtensionSource(corpus, source_hashes),
        publication=publication,
    )


async def acquire_avalanche_bigquery(
    request: Request,
    *,
    storage_root: Path,
    gcp_project: str,
    maximum_bytes_billed: int,
    rpc_url: str,
    progress: Progress,
    publication: Publication,
) -> dict[str, object]:
    if request.chain_id != _AVALANCHE_CHAIN_ID:
        raise ValueError("BigQuery acquisition requires Avalanche C-Chain")
    destination = corpus_path(storage_root.resolve(), request.corpus_id)
    with locked_work(storage_root.resolve(), request.corpus_id) as hidden:
        try:
            for path in hidden.iterdir():
                shutil.rmtree(path) if path.is_dir() else path.unlink()
            async with Rpc(rpc_url, batch_size=20, concurrency=6) as rpc:
                if await rpc.chain_id() != _AVALANCHE_CHAIN_ID:
                    raise ValueError("RPC chain ID does not match Avalanche C-Chain")
                first, target = await rpc.blocks(
                    [request.first_block, request.last_block],
                    chain_id=_AVALANCHE_CHAIN_ID,
                )
                config = bigquery.QueryJobConfig(
                    maximum_bytes_billed=maximum_bytes_billed,
                    query_parameters=[
                        bigquery.ScalarQueryParameter(
                            "first_block",
                            "INT64",
                            request.first_block,
                        ),
                        bigquery.ScalarQueryParameter(
                            "last_block",
                            "INT64",
                            request.last_block,
                        ),
                        bigquery.ScalarQueryParameter(
                            "first_timestamp",
                            "INT64",
                            first.timestamp,
                        ),
                        bigquery.ScalarQueryParameter(
                            "after_timestamp",
                            "INT64",
                            target.timestamp + 1,
                        ),
                    ],
                )
                rows = iter(
                    bigquery.Client(project=gcp_project)
                    .query(
                        _AVALANCHE_QUERY,
                        job_config=config,
                        location="US",
                    )
                    .result(page_size=10_000)
                )
                part_paths: list[Path] = []
                query_target: Block | None = None
                expected = request.first_block
                while page := list(islice(rows, 100_000)):
                    frame_rows: list[dict[str, object]] = []
                    page_first = expected
                    for row in page:
                        values = dict(row.items())
                        if expected > request.last_block or values["block_number"] != expected:
                            raise ValueError("BigQuery did not return the requested contiguous range")
                        block_hash = str(values.pop("block_hash")).removeprefix("0x")
                        parent_hash = str(values.pop("parent_hash")).removeprefix("0x")
                        receipt_gas_used = values.pop("receipt_gas_used")
                        if receipt_gas_used != values["gas_used"]:
                            raise ValueError(f"BigQuery receipts are incomplete for block {expected}")
                        frame_rows.append(values)
                        if expected == request.last_block:
                            query_target = Block(
                                block_number=expected,
                                block_hash=block_hash,
                                parent_hash=parent_hash,
                                timestamp=cast(int, values["timestamp"]),
                                chain_id=_AVALANCHE_CHAIN_ID,
                                base_fee_per_gas=cast(
                                    int,
                                    values["base_fee_per_gas"],
                                ),
                                gas_used=cast(int, values["gas_used"]),
                                gas_limit=cast(int, values["gas_limit"]),
                                tx_count=cast(int, values["tx_count"]),
                            )
                        expected += 1
                    part_path = hidden / f"{page_first:020d}-{expected - 1:020d}.parquet"
                    pl.DataFrame(frame_rows, schema=FINAL_SCHEMA).write_parquet(
                        part_path,
                        compression="zstd",
                        row_group_size=4096,
                    )
                    part_paths.append(part_path)
                if expected != request.last_block + 1:
                    raise ValueError("BigQuery did not return the requested contiguous range")
                if query_target is None:
                    raise ValueError("BigQuery returned no target block")
                anchor, proof = await _prove_finality(query_target, rpc, request)
            progress(
                {
                    "event": "bigquery_complete",
                    "first_block": request.first_block,
                    "last_block": request.last_block,
                }
            )
            candidate_path = hidden / "ready"
            write_candidate(
                candidate_path,
                request,
                anchor,
                part_paths,
            )
            candidate = load_corpus(candidate_path)
            receipt = _receipt(
                operation="acquire-bigquery",
                request=request,
                path=destination,
                source_id=None,
                source_rows=0,
                reused=0,
                acquired=candidate.rows,
                anchor=anchor,
                hashes=pair_hashes(candidate_path),
                samples=[candidate.fact(number) for number in _sample_numbers(request, None)],
                verifier={
                    **proof,
                    "mode": "bigquery_rpc",
                    "project": gcp_project,
                    "dataset": _AVALANCHE_DATASET,
                },
            )
            publication("publishing")
            publish(hidden, destination)
            publication("committed")
            progress(
                {
                    "event": "published",
                    "corpus_id": str(request.corpus_id),
                }
            )
            return receipt
        finally:
            discard_work(hidden)


async def acquire_corpus(
    request: Request,
    *,
    storage_root: Path,
    rpc_url: str,
    verify_rpc_url: str,
    batch_size: int,
    concurrency: int,
    progress: Progress,
    publication: Publication,
    extension: ExtensionSource | None = None,
) -> dict[str, object]:
    if rpc_url == verify_rpc_url:
        raise ValueError("Primary and verifier RPC endpoints must be independent")
    destination = corpus_path(storage_root.resolve(), request.corpus_id)
    if extension is None:
        operation, source_id, source_rows, source_last, source_paths = "acquire", None, 0, None, []
    else:
        source = extension.corpus
        operation, source_id, source_rows = "extend", source.request.corpus_id, source.rows
        source_last, source_paths = source.request.last_block, [source.blocks_path]
    suffix_first = request.first_block if source_last is None else source_last + 1
    suffix_request = Request(request.corpus_id, request.chain_id, suffix_first, request.last_block)
    binding: dict[str, object] = {
        "version": "0.1.0",
        "operation": operation,
        "request": request.document(),
    }
    if extension is not None:
        binding["source"] = {
            "path": str(extension.corpus.path),
            "pair_sha256": extension.hashes,
        }
    with locked_work(storage_root.resolve(), request.corpus_id) as hidden:
        work = prepare_work(hidden, destination, request, binding)
        candidate_path, receipt = work.candidate, work.receipt
        recovered = candidate_path is not None
        if recovered and receipt is not None:
            receipt = {**receipt, "reused_rows": request.last_block - request.first_block + 1, "acquired_rows": 0}
        paths, next_block, previous, reused_suffix = [], suffix_first, None, 0
        if candidate_path is None:
            paths, next_block, previous = checkpoint_paths(work.chunks, suffix_request, _CHECKPOINT_SIZE)
            reused_suffix = next_block - suffix_first
            progress({"event": "resume", "reused_rows": source_rows + reused_suffix})
        if receipt is None:
            async with (
                Rpc(rpc_url, batch_size=batch_size, concurrency=concurrency) as primary,
                Rpc(verify_rpc_url, batch_size=batch_size, concurrency=concurrency) as verifier,
            ):
                if await primary.chain_id() != request.chain_id or await verifier.chain_id() != request.chain_id:
                    raise ValueError("RPC chain ID does not match the Corpus request")
                boundary = None if extension is None else await _validate_source_boundary(extension.corpus, primary, verifier)
                if candidate_path is None:
                    if previous is not None and boundary is not None:
                        validate_links([read_checkpoint(paths[0], request.chain_id)[0]], boundary)
                    while next_block <= request.last_block:
                        last = min(next_block + _CHECKPOINT_SIZE - 1, request.last_block)
                        blocks = await primary.blocks(range(next_block, last + 1), chain_id=request.chain_id)
                        priority_fees = await primary.priority_fees(next_block, last)
                        validate_links(blocks, previous or boundary)
                        path = work.chunks / f"{next_block:020d}-{last:020d}.parquet"
                        write_checkpoint(path, blocks, priority_fees)
                        paths.append(path)
                        previous, next_block = blocks[-1], last + 1
                        progress({"event": "checkpoint", "first_block": blocks[0].block_number, "last_block": last})
                    assert previous is not None
                    anchor, verifier_fact = await _prove_finality(previous, verifier, request)
                    candidate_path = hidden / "ready.tmp"
                    write_candidate(candidate_path, request, anchor, source_paths + paths)
                    candidate = load_corpus(candidate_path)
                    reused, acquired = source_rows + reused_suffix, request.last_block - suffix_first + 1 - reused_suffix
                else:
                    candidate = load_corpus(candidate_path)
                    anchor = candidate.anchor
                    verifier_fact = await _validate_candidate_finality(candidate, primary, verifier)
                    reused, acquired = candidate.rows, 0
                samples = await _check_samples(candidate, primary, _sample_numbers(request, source_last))
            receipt = _receipt(
                operation=operation,
                request=request,
                path=destination,
                source_id=source_id,
                source_rows=source_rows,
                reused=reused,
                acquired=acquired,
                anchor=anchor,
                hashes=pair_hashes(candidate_path),
                samples=samples,
                verifier=verifier_fact,
            )
        if extension is not None:
            extension.ensure_unchanged()
        assert candidate_path is not None and receipt is not None
        if work.published:
            publication("committed")
        else:
            save_ready(hidden, candidate_path, receipt)
            publication("publishing")
            publish(hidden, destination)
            publication("committed")
        discard_work(hidden)
        event: dict[str, object] = {"event": "published", "corpus_id": str(request.corpus_id)}
        if recovered:
            event["recovered"] = True
        progress(event)
        return receipt


async def verify_corpus(
    path: Path,
    *,
    rpc_url: str | None,
    full_rpc: bool,
    batch_size: int,
    concurrency: int,
    progress: Progress,
) -> dict[str, object]:
    corpus = load_corpus(path)
    progress({"event": "local_valid", "rows": corpus.rows})
    sample_numbers = _sample_numbers(corpus.request, None)
    numbers: Iterable[int] = range(corpus.request.first_block, corpus.request.last_block + 1) if full_rpc else sample_numbers
    samples: list[dict[str, object]]
    verifier_fact: dict[str, object] = {"mode": "local"}
    if rpc_url is None:
        if full_rpc:
            raise ValueError("--full-rpc requires --rpc-url or BLOCKWEAVER_RPC_URL")
        samples = [corpus.fact(number) for number in sample_numbers]
    else:
        async with Rpc(rpc_url, batch_size=batch_size, concurrency=concurrency) as rpc:
            chain_id = await rpc.chain_id()
            if chain_id != corpus.request.chain_id:
                raise ValueError("RPC chain ID does not match the Corpus")
            samples = await _check_samples(corpus, rpc, numbers, set(sample_numbers), contiguous=full_rpc)
            target = (await rpc.blocks([corpus.request.last_block], chain_id=chain_id))[0]
            fresh = await _refresh_finality(target, corpus.anchor, rpc, chain_id)
            verifier_fact = {
                "mode": "full_rpc" if full_rpc else "sample_rpc",
                "chain_id": chain_id,
                "finalized_block_number": fresh.block_number,
                "finalized_block_hash": fresh.block_hash,
            }
    return _receipt(
        operation="verify",
        request=corpus.request,
        path=corpus.path,
        source_id=None,
        source_rows=0,
        reused=corpus.rows,
        acquired=0,
        anchor=corpus.anchor,
        hashes=pair_hashes(corpus.path),
        samples=samples,
        verifier=verifier_fact,
    )


async def _validate_source_boundary(source: LoadedCorpus, primary: Rpc, verifier: Rpc) -> Block:
    number = source.request.last_block
    primary_block, verifier_block = (
        await primary.blocks([number], chain_id=source.request.chain_id),
        await verifier.blocks([number], chain_id=source.request.chain_id),
    )
    if primary_block[0] != verifier_block[0]:
        raise ValueError("RPC endpoints disagree on the source boundary")
    source_fact = source.fact(number)
    source_headers = {name: source_fact[name] for name in primary_block[0].durable_row()}
    if primary_block[0].durable_row() != source_headers:
        raise ValueError("Source boundary does not match RPC")
    return primary_block[0]


async def _prove_finality(target: Block, verifier: Rpc, request: Request) -> tuple[Anchor, dict[str, object]]:
    verifier_target = (await verifier.blocks([target.block_number], chain_id=request.chain_id))[0]
    if target != verifier_target:
        raise ValueError("RPC endpoints disagree on the target block")
    tagged = await verifier.finalized_block(chain_id=request.chain_id)
    if tagged.block_number < target.block_number:
        raise ValueError("Verifier finalized head does not cover the target")
    await _connect_ancestry(verifier_target, tagged, verifier, request.chain_id)
    return Anchor(tagged.block_number, tagged.block_hash), {
        "chain_id": request.chain_id,
        "target_block_hash": target.block_hash,
        "finalized_block_number": tagged.block_number,
        "finalized_block_hash": tagged.block_hash,
    }


async def _validate_candidate_finality(corpus: LoadedCorpus, primary: Rpc, verifier: Rpc) -> dict[str, object]:
    chain_id = corpus.request.chain_id
    target = (await primary.blocks([corpus.request.last_block], chain_id=chain_id))[0]
    verifier_target = (await verifier.blocks([target.block_number], chain_id=chain_id))[0]
    if target != verifier_target:
        raise ValueError("RPC endpoints disagree on the target block")
    fresh = await _refresh_finality(verifier_target, corpus.anchor, verifier, chain_id)
    return {
        "chain_id": chain_id,
        "target_block_hash": target.block_hash,
        "finalized_block_number": fresh.block_number,
        "finalized_block_hash": fresh.block_hash,
        "recovered": True,
    }


async def _refresh_finality(target: Block, anchor: Anchor, rpc: Rpc, chain_id: int) -> Block:
    stored = (await rpc.blocks([anchor.block_number], chain_id=chain_id))[0]
    if stored.block_hash != anchor.block_hash:
        raise ValueError("Stored finalized anchor no longer matches RPC")
    await _connect_ancestry(target, stored, rpc, chain_id)
    fresh = await rpc.finalized_block(chain_id=chain_id)
    if fresh.block_number < stored.block_number:
        raise ValueError("RPC finalized head does not cover the stored finalized anchor")
    await _connect_ancestry(stored, fresh, rpc, chain_id)
    return fresh


async def _connect_ancestry(previous: Block, tagged: Block, rpc: Rpc, chain_id: int) -> None:
    cursor = previous.block_number + 1
    while cursor <= tagged.block_number:
        last = min(cursor + _CHECKPOINT_SIZE - 1, tagged.block_number)
        segment = await rpc.blocks(range(cursor, last + 1), chain_id=chain_id)
        validate_links(segment, previous)
        previous = segment[-1]
        cursor = last + 1
    reread = (await rpc.blocks([tagged.block_number], chain_id=chain_id))[0]
    if tagged != reread or previous != tagged:
        raise ValueError("Finalized tag did not survive numbered reread")


async def _check_samples(
    corpus: LoadedCorpus,
    rpc: Rpc,
    numbers: Iterable[int],
    receipt_numbers: set[int] | None = None,
    *,
    contiguous: bool = False,
) -> list[dict[str, object]]:
    facts: list[dict[str, object]] = []
    previous: Block | None = None
    iterator = iter(numbers)
    while batch := list(islice(iterator, _CHECKPOINT_SIZE)):
        remote = await rpc.blocks(batch, chain_id=corpus.request.chain_id)
        if contiguous:
            values = await rpc.priority_fees(batch[0], batch[-1])
            priority_fees = dict(zip(batch, values, strict=True))
        else:
            priority_fees = {number: (await rpc.priority_fees(number, number))[0] for number in batch}
        local = corpus.facts(batch)
        if contiguous:
            validate_links(remote, previous)
            previous = remote[-1]
        for block in remote:
            durable = block.corpus_row(*priority_fees[block.block_number])
            if durable != local[block.block_number]:
                raise ValueError(f"Corpus row {block.block_number} does not match RPC")
            if receipt_numbers is None or block.block_number in receipt_numbers:
                facts.append({**durable, "block_hash": block.block_hash})
    return facts


def _sample_numbers(request: Request, source_last: int | None) -> list[int]:
    selected = {request.first_block, request.last_block}
    if source_last is not None:
        selected.update({source_last, source_last + 1})
    blocked = sorted(number for number in selected if request.first_block < number < request.last_block)
    available = request.last_block - request.first_block - 1 - len(blocked)
    if available > 0:
        seed = int.from_bytes(hashlib.sha256(request.corpus_id.bytes).digest()[:8], "big")
        for offset in range(min(3, available)):
            candidate = request.first_block + 1 + (seed + offset) % available
            for boundary in blocked:
                if candidate >= boundary:
                    candidate += 1
            selected.add(candidate)
    return sorted(selected)


def _receipt(
    *,
    operation: str,
    request: Request,
    path: Path,
    source_id: UUID | None,
    source_rows: int,
    reused: int,
    acquired: int,
    anchor: Anchor,
    hashes: dict[str, str],
    samples: list[dict[str, object]],
    verifier: dict[str, object],
) -> dict[str, object]:
    receipt: dict[str, object] = {
        "version": 1,
        "operation": operation,
        "corpus_id": str(request.corpus_id),
        "path": str(path),
        "chain_id": request.chain_id,
        "first_block": request.first_block,
        "last_block": request.last_block,
        "rows": request.last_block - request.first_block + 1,
        "source_rows": source_rows,
        "reused_rows": reused,
        "acquired_rows": acquired,
        "finalized_anchor": anchor.document(),
        "pair_sha256": hashes,
        "samples": samples,
        "verifier": verifier,
    }
    if source_id is not None:
        receipt["source_corpus_id"] = str(source_id)
    return receipt
