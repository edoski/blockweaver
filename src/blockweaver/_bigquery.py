"""Build Avalanche corpora from Google Blockchain Analytics."""

from __future__ import annotations

import time
from collections.abc import Callable
from pathlib import Path
from typing import cast
from uuid import UUID

import polars as pl
from google.cloud import bigquery

from ._build import Publication, _receipt, _sample_numbers
from ._contract import Anchor, Request
from ._corpus import (
    FINAL_SCHEMA,
    corpus_path,
    discard_work,
    load_corpus,
    load_enrichment_source,
    locked_work,
    pair_hashes,
    publish,
    write_enriched_candidate,
)

Progress = Callable[[dict[str, object]], None]
_CHAIN_ID = 43_114
_DATASET = "bigquery-public-data.goog_blockchain_avalanche_contract_chain_us"
_QUERY = f"""
WITH requested_blocks AS (
  SELECT block_number, block_timestamp, block_hash, base_fee_per_gas, gas_used, gas_limit
  FROM `{_DATASET}.blocks`
  WHERE block_timestamp >= TIMESTAMP_SECONDS(@first_timestamp)
    AND block_timestamp < TIMESTAMP_SECONDS(@after_timestamp)
    AND block_number BETWEEN @first_block AND @last_block
),
requested_receipts AS (
  SELECT block_hash, transaction_index, gas_used, effective_gas_price
  FROM `{_DATASET}.receipts`
  WHERE block_timestamp >= TIMESTAMP_SECONDS(@first_timestamp)
    AND block_timestamp < TIMESTAMP_SECONDS(@after_timestamp)
),
weighted AS (
  SELECT
    b.block_number,
    r.transaction_index,
    r.effective_gas_price - b.base_fee_per_gas AS priority_fee,
    b.gas_used AS block_gas_used,
    COUNT(*) OVER (PARTITION BY b.block_number) AS tx_count,
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
    ARRAY_AGG(priority_fee ORDER BY priority_fee, transaction_index LIMIT 1)[OFFSET(0)] AS priority_fee_p50
  FROM weighted
  WHERE cumulative_gas_used >= DIV(block_gas_used, 2)
  GROUP BY block_number
)
SELECT
  b.block_number,
  UNIX_SECONDS(b.block_timestamp) AS timestamp,
  {_CHAIN_ID} AS chain_id,
  b.base_fee_per_gas,
  b.gas_used,
  b.gas_limit,
  COALESCE(f.tx_count, 0) AS tx_count,
  COALESCE(f.priority_fee_p50, 0) AS effective_priority_fee_per_gas_p50,
  b.block_hash
FROM requested_blocks AS b
LEFT JOIN fees AS f USING (block_number)
ORDER BY block_number
"""


async def enrich_avalanche_bigquery(
    source_path: Path,
    *,
    storage_root: Path,
    corpus_id: UUID,
    last_block: int,
    gcp_project: str,
    maximum_bytes_billed: int,
    progress: Progress,
    publication: Publication,
) -> dict[str, object]:
    source = load_enrichment_source(source_path)
    if source.request.chain_id != _CHAIN_ID:
        raise ValueError("BigQuery enrichment requires an Avalanche C-Chain Corpus")
    if corpus_id == source.request.corpus_id:
        raise ValueError("Enrichment requires a new corpus_id")
    if last_block < source.request.last_block:
        raise ValueError("last_block must not precede the source endpoint")

    request = Request(corpus_id, _CHAIN_ID, source.request.first_block, last_block)
    first_timestamp = cast(int, source.fact(source.request.first_block)["timestamp"])
    source_last_timestamp = cast(int, source.fact(source.request.last_block)["timestamp"])
    after_timestamp = source_last_timestamp + 1 if last_block == source.request.last_block else max(source_last_timestamp + 1, int(time.time()) + 1)
    config = bigquery.QueryJobConfig(
        maximum_bytes_billed=maximum_bytes_billed,
        query_parameters=[
            bigquery.ScalarQueryParameter("first_block", "INT64", request.first_block),
            bigquery.ScalarQueryParameter("last_block", "INT64", request.last_block),
            bigquery.ScalarQueryParameter("first_timestamp", "INT64", first_timestamp),
            bigquery.ScalarQueryParameter("after_timestamp", "INT64", after_timestamp),
        ],
    )
    rows = bigquery.Client(project=gcp_project).query(_QUERY, job_config=config, location="US").result(page_size=10_000)
    priority_fees: list[int] = []
    suffix_rows: list[dict[str, object]] = []
    target_hash = ""
    for expected, row in zip(range(request.first_block, request.last_block + 1), rows, strict=True):
        values = dict(row.items())
        if values["block_number"] != expected:
            raise ValueError("BigQuery did not return the requested contiguous range")
        target_hash = str(values.pop("block_hash")).removeprefix("0x")
        if expected <= source.request.last_block:
            priority_fees.append(cast(int, values["effective_priority_fee_per_gas_p50"]))
        else:
            suffix_rows.append(values)
    progress({"event": "bigquery_complete", "first_block": request.first_block, "last_block": request.last_block})

    suffix = pl.DataFrame(suffix_rows, schema=FINAL_SCHEMA) if suffix_rows else None
    anchor = source.anchor if last_block == source.request.last_block else Anchor(last_block, target_hash)
    destination = corpus_path(storage_root.resolve(), corpus_id)
    with locked_work(storage_root.resolve(), corpus_id) as hidden:
        try:
            candidate_path = hidden / "ready"
            write_enriched_candidate(candidate_path, source, request, priority_fees, suffix, anchor)
            candidate = load_corpus(candidate_path)
            receipt = _receipt(
                operation="enrich-bigquery",
                request=request,
                path=destination,
                source_id=source.request.corpus_id,
                source_rows=source.rows,
                reused=source.rows,
                acquired=last_block - source.request.last_block,
                anchor=anchor,
                hashes=pair_hashes(candidate_path),
                samples=[candidate.fact(number) for number in _sample_numbers(request, source.request.last_block)],
                verifier={"mode": "bigquery", "project": gcp_project, "dataset": _DATASET},
            )
            publication("publishing")
            publish(hidden, destination)
            publication("committed")
            progress({"event": "published", "corpus_id": str(corpus_id)})
            return receipt
        finally:
            discard_work(hidden)
