"""Download, verify, and atomically publish configured datasets."""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Literal
from uuid import UUID

from . import __version__
from ._contract import (
    Anchor,
    BigQuerySettings,
    BlockweaverError,
    Chain,
    Header,
    OutputFormat,
    Plan,
    Provider,
    RequestedRange,
    ResolvedRange,
    SourceDefinition,
    Value,
    block_hash,
    plan_features,
    validate_links,
    validate_uuid,
)
from ._corpus import (
    Dataset,
    _open_dataset,
    checkpoint_facts,
    checkpoint_paths,
    dataset_path,
    discard_work,
    locked_work,
    open_dataset,
    pair_hashes,
    prepare_work,
    publish,
    save_ready,
    write_candidate,
    write_checkpoint,
)
from ._sources import BigQueryClient, BigQueryPlan, Rpc, compile_bigquery, open_bigquery

Progress = Callable[[dict[str, object]], None]
Publication = Callable[[Literal["publishing", "committed"]], None]
ChunkStream = Callable[[int, int], AsyncIterator[tuple[list[Header], list[dict[str, Value]]]]]
_CHUNK_SIZE = 1024
_INTEGRITY_PLAN = plan_features([])


@dataclass(frozen=True, slots=True)
class DownloadSpec:
    dataset_id: UUID
    chain: Chain
    requested_range: RequestedRange
    plan: Plan
    output_root: Path
    output_format: OutputFormat
    source: SourceDefinition
    primary: Provider | None
    verifier: Provider
    bigquery: BigQuerySettings | None = None


async def download(spec: DownloadSpec, *, progress: Progress, publication: Publication) -> dict[str, object]:
    validate_uuid(spec.dataset_id)
    spec.source.validate_runtime(spec.chain, primary=spec.primary, bigquery=spec.bigquery)
    return await _DOWNLOADERS[spec.source.runner](spec, progress=progress, publication=publication)


async def _download_rpc(spec: DownloadSpec, *, progress: Progress, publication: Publication) -> dict[str, object]:
    assert spec.primary is not None
    if spec.primary.name == spec.verifier.name:
        raise BlockweaverError("PROVIDER_INVALID", "Primary and verifier must be distinct provider profiles")
    if spec.primary.url == spec.verifier.url:
        raise BlockweaverError("PROVIDER_INVALID", "Primary and verifier RPC endpoints must be independent")
    progress({"event": "request", "dataset_id": str(spec.dataset_id)})
    try:
        async with _rpc(spec.primary) as primary, _rpc(spec.verifier) as verifier:
            primary_chain_id, verifier_chain_id = await primary.chain_id(), await verifier.chain_id()
            if primary_chain_id != spec.chain.chain_id or verifier_chain_id != spec.chain.chain_id:
                raise BlockweaverError("RPC_CHAIN_MISMATCH", "RPC chain ID does not match the configured chain")
            primary_tag, verifier_tag = (
                await primary.tagged_header(spec.chain.finality_tag, _INTEGRITY_PLAN),
                await verifier.tagged_header(spec.chain.finality_tag, _INTEGRITY_PLAN),
            )
            resolved = await _resolve_range(spec.requested_range, primary, min(primary_tag.block_number, verifier_tag.block_number))
            if spec.requested_range.kind == "time":
                await _verify_time_boundaries(spec.requested_range, resolved, primary, verifier, verifier_tag.block_number)

            async def chunks(first: int, last: int) -> AsyncIterator[tuple[list[Header], list[dict[str, Value]]]]:
                while first <= last:
                    end = min(first + _CHUNK_SIZE - 1, last)
                    yield await primary.rows(first, end, spec.plan)
                    first = end + 1

            verification: dict[str, object] = {"primary_chain_id": primary_chain_id, "verifier_chain_id": verifier_chain_id}
            return await _materialize(spec, resolved, verifier, primary, chunks, verification, progress, publication)
    except BlockweaverError:
        raise
    except ValueError as error:
        raise BlockweaverError("RPC_INVALID", str(error)) from None
    except RuntimeError as error:
        raise BlockweaverError("RPC_FAILED", str(error)) from None
    except OSError as error:
        raise BlockweaverError("IO_FAILED", str(error)) from None


async def _download_bigquery(
    spec: DownloadSpec,
    *,
    progress: Progress,
    publication: Publication,
) -> dict[str, object]:
    assert spec.bigquery is not None and spec.chain.bigquery_dataset is not None
    settings, dataset = spec.bigquery, spec.chain.bigquery_dataset
    warehouse = open_bigquery(settings.project)
    progress({"event": "request", "dataset_id": str(spec.dataset_id)})
    try:
        async with _rpc(spec.verifier) as verifier:
            verifier_chain_id = await verifier.chain_id()
            if verifier_chain_id != spec.chain.chain_id:
                raise BlockweaverError("RPC_CHAIN_MISMATCH", "Verifier RPC chain ID does not match the configured chain")
            tagged = await verifier.tagged_header(spec.chain.finality_tag, _INTEGRITY_PLAN)
            resolved = await _resolve_range(spec.requested_range, verifier, tagged.block_number)
            if spec.requested_range.kind == "time":
                await _verify_time_boundaries(spec.requested_range, resolved, verifier, verifier, tagged.block_number)
            verification: dict[str, object] = {"verifier_chain_id": verifier_chain_id, "dry_run_bytes": 0}

            async def chunks(first: int, last: int) -> AsyncIterator[tuple[list[Header], list[dict[str, Value]]]]:
                if first > last:
                    return
                query = compile_bigquery(dataset, spec.plan)
                parameters = {
                    "first_block": first,
                    "last_block": last,
                    "from_timestamp": resolved.first_timestamp,
                    "to_timestamp": resolved.last_timestamp,
                }
                dry_run_bytes = _prepare_bigquery(
                    warehouse,
                    dataset,
                    query,
                    parameters,
                    settings.maximum_bytes_billed,
                )
                verification["dry_run_bytes"] = dry_run_bytes
                progress({"event": "bigquery_dry_run", "bytes_processed": dry_run_bytes})
                for chunk in _bigquery_chunks(
                    warehouse,
                    query,
                    parameters,
                    settings.maximum_bytes_billed,
                    first,
                    last,
                    spec.plan,
                ):
                    yield chunk

            return await _materialize(spec, resolved, verifier, None, chunks, verification, progress, publication)
    except BlockweaverError:
        raise
    except ValueError as error:
        raise BlockweaverError("BIGQUERY_INVALID", str(error)) from None
    except RuntimeError as error:
        raise BlockweaverError("RPC_FAILED", str(error)) from None
    except OSError as error:
        raise BlockweaverError("IO_FAILED", str(error)) from None
    except Exception as error:
        raise BlockweaverError("BIGQUERY_FAILED", str(error) or type(error).__name__) from None


_DOWNLOADERS = {"rpc": _download_rpc, "bigquery": _download_bigquery}


async def _materialize(
    spec: DownloadSpec,
    resolved: ResolvedRange,
    verifier: Rpc,
    primary: Rpc | None,
    chunks: ChunkStream,
    verification: dict[str, object],
    progress: Progress,
    publication: Publication,
) -> dict[str, object]:
    root = spec.output_root.resolve()
    destination = dataset_path(root, spec.dataset_id)
    with locked_work(root, spec.dataset_id, destination) as hidden:
        work = prepare_work(hidden, destination, _binding(spec, resolved))
        candidate_path, receipt = work.candidate, work.receipt
        recovered = candidate_path is not None
        paths: list[Path] = []
        next_block = resolved.first_block
        previous: Header | None = None
        reused = 0
        if candidate_path is None:
            paths, next_block, previous = checkpoint_paths(
                work.chunks,
                first_block=resolved.first_block,
                last_block=resolved.last_block,
                size=_CHUNK_SIZE,
                plan=spec.plan,
            )
            reused = next_block - resolved.first_block
            progress({"event": "resume", "reused_rows": reused})
        if receipt is None:
            if candidate_path is None:
                async for headers, rows in chunks(next_block, resolved.last_block):
                    expected_last = min(next_block + _CHUNK_SIZE - 1, resolved.last_block)
                    if not headers or len(headers) != len(rows) or (headers[0].block_number, headers[-1].block_number) != (next_block, expected_last):
                        raise ValueError("Source did not return a deterministic complete chunk")
                    validate_links(headers, previous)
                    if next_block == resolved.first_block and headers[0].timestamp != resolved.first_timestamp:
                        raise ValueError("Source start timestamp does not match the resolved range")
                    paths.append(write_checkpoint(work.chunks, next_block, expected_last, spec.plan, headers, rows))
                    previous, next_block = headers[-1], expected_last + 1
                    progress({"event": "checkpoint", "from_block": headers[0].block_number, "to_block": expected_last})
                if previous is None or next_block != resolved.last_block + 1 or previous.timestamp != resolved.last_timestamp:
                    raise ValueError("Source did not return the exact resolved range")
                anchor, verifier_target = await _prove_finality(previous, verifier, spec.chain)
                sample_numbers = _sample_numbers(spec.dataset_id, resolved.first_block, resolved.last_block)
                await _check_rows(checkpoint_facts(paths, spec.plan, sample_numbers), sample_numbers, spec.plan, verifier)
                verification.update({"target_agreement": previous == verifier_target, "sampled_blocks": sample_numbers})
                candidate_path = hidden / "ready.tmp"
                candidate = write_candidate(
                    candidate_path,
                    plan=spec.plan,
                    output_format=spec.output_format,
                    sources=paths,
                    manifest=_manifest(spec, resolved, previous.block_hash, anchor, verification),
                )
                acquired = candidate.row_count - reused
            else:
                candidate = _open_dataset(candidate_path, work=candidate_path.name in {"ready", "ready.tmp"})
                await _validate_candidate(candidate, primary, verifier, spec.chain)
                anchor = candidate._anchor
                acquired, reused = 0, candidate.row_count
            receipt = _receipt("download", candidate, destination, reused, acquired, anchor)
        assert candidate_path is not None and receipt is not None
        if work.published:
            publication("committed")
        else:
            save_ready(hidden, candidate_path, receipt)
            publication("publishing")
            publish(hidden, destination)
            publication("committed")
        discard_work(hidden)
        event: dict[str, object] = {"event": "published", "dataset_id": str(spec.dataset_id), "path": str(destination)}
        if recovered:
            event["recovered"] = True
        progress(event)
        return receipt


def _prepare_bigquery(
    warehouse: BigQueryClient,
    dataset: str,
    query: BigQueryPlan,
    parameters: dict[str, int],
    maximum_bytes_billed: int,
) -> int:
    for table, expected in query.table_fields.items():
        actual = warehouse.table_schema(dataset, table)
        if any(field not in actual or not _compatible_bigquery_type(actual[field], dtype) for field, dtype in expected.items()):
            raise BlockweaverError("SOURCE_FEATURE_UNAVAILABLE", f"Configured BigQuery dataset does not support the selected features in {table}")
    bytes_processed, schema = warehouse.dry_run(query.sql, parameters)
    if set(schema) != set(query.result_schema) or any(not _compatible_bigquery_type(schema[name], dtype) for name, dtype in query.result_schema.items()):
        raise BlockweaverError("BIGQUERY_SCHEMA_INVALID", "BigQuery dry-run result schema does not match the selected feature plan")
    if bytes_processed > maximum_bytes_billed:
        raise BlockweaverError("BIGQUERY_COST_LIMIT", "BigQuery dry run exceeds maximum_bytes_billed")
    return bytes_processed


def _compatible_bigquery_type(actual: tuple[str, str], expected: str) -> bool:
    dtype, mode = actual
    return mode in {"NULLABLE", "REQUIRED"} and (dtype == expected or {dtype, expected} <= {"INTEGER", "INT64"})


def _bigquery_chunks(
    warehouse: BigQueryClient,
    query: BigQueryPlan,
    parameters: dict[str, int],
    maximum_bytes_billed: int,
    first_block: int,
    last_block: int,
    plan: Plan,
) -> Iterator[tuple[list[Header], list[dict[str, Value]]]]:
    headers: list[Header] = []
    rows: list[dict[str, Value]] = []
    expected = first_block
    for page in warehouse.pages(query.sql, parameters, maximum_bytes_billed, _CHUNK_SIZE):
        for value in page:
            header, row = _parse_bigquery_row(value, query.result_schema, plan, expected)
            headers.append(header)
            rows.append(row)
            expected += 1
            if len(headers) == _CHUNK_SIZE:
                yield headers, rows
                headers, rows = [], []
    if headers:
        yield headers, rows
    if expected != last_block + 1:
        raise BlockweaverError("BIGQUERY_INVALID", "BigQuery did not return the exact contiguous requested range")


def _parse_bigquery_row(value: Mapping[str, object], schema: dict[str, str], plan: Plan, expected: int) -> tuple[Header, dict[str, Value]]:
    if set(value) != set(schema):
        raise BlockweaverError("BIGQUERY_INVALID", "BigQuery returned a noncanonical row shape")
    number = _bigquery_int(value["block_number"], "block_number")
    if number != expected:
        raise BlockweaverError("BIGQUERY_INVALID", "BigQuery did not return the exact contiguous requested range")
    timestamp = _bigquery_int(value["_proof_timestamp"], "timestamp")
    header = Header(
        number,
        block_hash(value["_proof_hash"], "block hash"),
        block_hash(value["_proof_parent_hash"], "parent hash"),
        timestamp,
        {},
    )
    if "_proof_gas_limit" in schema:
        gas_used = _bigquery_int(value["_proof_gas_used"], "gas used")
        gas_limit = _bigquery_int(value["_proof_gas_limit"], "gas limit")
        if gas_limit == 0 or gas_used > gas_limit:
            raise BlockweaverError("BIGQUERY_INVALID", f"BigQuery returned an invalid gas domain for block {number}")
    if plan.percentiles and _bigquery_int(value["_receipt_gas_used"], "receipt gas used") != _bigquery_int(value["_proof_gas_used"], "block gas used"):
        raise BlockweaverError("BIGQUERY_INVALID", f"BigQuery receipts are incomplete for block {number}")
    row: dict[str, Value] = {"block_number": number}
    for feature in plan.features:
        raw = value[feature.name]
        row[feature.name] = block_hash(raw, feature.name) if feature.dtype == "UTF-8" else _bigquery_int(raw, feature.name)
    return header, row


def _bigquery_int(value: object, label: str) -> int:
    if type(value) is not int or value < 0 or value > 2**63 - 1:
        raise BlockweaverError("BIGQUERY_INVALID", f"BigQuery returned an invalid {label}")
    return value


async def verify_dataset(
    path: Path,
    *,
    provider: Provider | None,
    full_rpc: bool,
    progress: Progress,
) -> dict[str, object]:
    dataset = open_dataset(path)
    progress({"event": "local_valid", "rows": dataset.row_count})
    anchor = dataset._anchor
    verification: dict[str, object] = {"mode": "local"}
    if provider is None:
        if full_rpc:
            raise BlockweaverError("VERIFY_INVALID", "--full-rpc requires a configured provider or --rpc-url")
    else:
        try:
            async with _rpc(provider) as rpc:
                chain_id = await rpc.chain_id()
                if chain_id != dataset.chain_id:
                    raise BlockweaverError("RPC_CHAIN_MISMATCH", "RPC chain ID does not match the dataset")
                target = await rpc.header(dataset.last_block, dataset._plan)
                if target.block_hash != dataset._target_hash:
                    raise BlockweaverError("RPC_MISMATCH", "Dataset target hash does not match RPC")
                fresh = await _refresh_finality(target, anchor, rpc)
                sample_numbers = _sample_numbers(dataset.dataset_id, dataset.first_block, dataset.last_block)
                if full_rpc:
                    await _check_full_dataset(dataset, rpc)
                else:
                    await _check_rows(dataset._facts(sample_numbers), sample_numbers, dataset._plan, rpc)
                verification = {
                    "mode": "full_rpc" if full_rpc else "sample_rpc",
                    "provider": provider.name,
                    "chain_id": chain_id,
                    "sampled_blocks": sample_numbers,
                    "finalized_anchor": fresh.document(),
                }
        except BlockweaverError:
            raise
        except ValueError as error:
            raise BlockweaverError("RPC_INVALID", str(error)) from None
        except RuntimeError as error:
            raise BlockweaverError("RPC_FAILED", str(error)) from None
    return {
        "version": 1,
        "operation": "verify",
        "dataset_id": str(dataset.dataset_id),
        "path": str(dataset.path),
        "rows": dataset.row_count,
        "artifact_sha256": pair_hashes(dataset.path),
        "verification": verification,
    }


def _rpc(provider: Provider) -> Rpc:
    return Rpc(provider.url, batch_size=provider.batch_size, concurrency=provider.concurrency, timeout=provider.timeout)


async def _resolve_range(request: RequestedRange, rpc: Rpc, finalized: int) -> ResolvedRange:
    if request.kind == "block":
        if request.end > finalized:
            raise BlockweaverError("RANGE_UNFINALIZED", "Requested block range is not fully finalized")
        first, last = await rpc.headers([request.start, request.end], _INTEGRITY_PLAN)
        return ResolvedRange(request.start, request.end, first.timestamp, last.timestamp)
    cache: dict[int, Header] = {}

    async def header(number: int) -> Header:
        if number not in cache:
            cache[number] = await rpc.header(number, _INTEGRITY_PLAN)
        return cache[number]

    genesis, latest = await header(0), await header(finalized)
    if request.start < genesis.timestamp:
        raise BlockweaverError("RANGE_PRE_GENESIS", "Requested time range begins before chain genesis")
    if request.end > latest.timestamp:
        raise BlockweaverError("RANGE_UNFINALIZED", "Requested time range extends beyond the finalized chain")
    first = await _lower_bound_timestamp(request.start, finalized, header)
    after = await _lower_bound_timestamp(request.end + 1, finalized, header)
    last = finalized if after == finalized and (await header(finalized)).timestamp <= request.end else after - 1
    first_header, last_header = await header(first), await header(last)
    if first > last or first_header.timestamp > request.end:
        raise BlockweaverError("RANGE_EMPTY", "Requested time range contains no blocks")
    return ResolvedRange(first, last, first_header.timestamp, last_header.timestamp)


async def _lower_bound_timestamp(target: int, high: int, header: Callable[[int], Awaitable[Header]]) -> int:
    low = 0
    while low < high:
        middle = (low + high) // 2
        value = await header(middle)
        if value.timestamp < target:
            low = middle + 1
        else:
            high = middle
    return low


async def _verify_time_boundaries(
    request: RequestedRange,
    resolved: ResolvedRange,
    primary: Rpc,
    verifier: Rpc,
    verifier_finalized: int,
) -> None:
    boundary_numbers = sorted({resolved.first_block, resolved.last_block})
    primary_boundaries, verifier_boundaries = (
        await primary.headers(boundary_numbers, _INTEGRITY_PLAN),
        await verifier.headers(boundary_numbers, _INTEGRITY_PLAN),
    )
    if any(not _same_core(left, right) for left, right in zip(primary_boundaries, verifier_boundaries, strict=True)):
        raise BlockweaverError("RPC_MISMATCH", "RPC endpoints disagree on a resolved time boundary")
    adjacent_numbers = []
    if resolved.first_block > 0:
        adjacent_numbers.append(resolved.first_block - 1)
    if resolved.last_block < verifier_finalized:
        adjacent_numbers.append(resolved.last_block + 1)
    adjacent = {header.block_number: header for header in await verifier.headers(adjacent_numbers, _INTEGRITY_PLAN)}
    first, last = verifier_boundaries[0], verifier_boundaries[-1]
    if (
        first.timestamp != resolved.first_timestamp
        or last.timestamp != resolved.last_timestamp
        or first.timestamp < request.start
        or last.timestamp > request.end
        or (resolved.first_block > 0 and adjacent[resolved.first_block - 1].timestamp >= request.start)
        or (resolved.last_block < verifier_finalized and adjacent[resolved.last_block + 1].timestamp <= request.end)
    ):
        raise BlockweaverError("RPC_MISMATCH", "Verifier RPC does not prove the resolved time-range edges")


async def _prove_finality(target: Header, verifier: Rpc, chain: Chain) -> tuple[Anchor, Header]:
    verifier_target = await verifier.header(target.block_number, plan_features_for_header(target))
    if not _same_header(target, verifier_target):
        raise BlockweaverError("RPC_MISMATCH", "RPC endpoints disagree on the target block")
    tagged = await verifier.tagged_header(chain.finality_tag, _INTEGRITY_PLAN)
    if tagged.block_number < target.block_number:
        raise BlockweaverError("RANGE_UNFINALIZED", "Verifier finality head does not cover the target")
    await _connect_ancestry(verifier_target, tagged, verifier)
    return Anchor(tagged.block_number, tagged.block_hash, chain.finality_tag), verifier_target


def plan_features_for_header(header: Header) -> Plan:
    return plan_features(list(header.values))


def _same_header(left: Header, right: Header) -> bool:
    return (
        left.block_number,
        left.block_hash,
        left.parent_hash,
        left.timestamp,
        left.values,
    ) == (
        right.block_number,
        right.block_hash,
        right.parent_hash,
        right.timestamp,
        right.values,
    )


async def _refresh_finality(target: Header, anchor: Anchor, rpc: Rpc) -> Anchor:
    stored = await rpc.header(anchor.block_number, _INTEGRITY_PLAN)
    if stored.block_hash != anchor.block_hash:
        raise BlockweaverError("RPC_MISMATCH", "Stored finalized anchor no longer matches RPC")
    await _connect_ancestry(target, stored, rpc)
    fresh = await rpc.tagged_header(anchor.tag, _INTEGRITY_PLAN)
    if fresh.block_number < stored.block_number:
        raise BlockweaverError("RPC_MISMATCH", "RPC finality head regressed behind the stored anchor")
    await _connect_ancestry(stored, fresh, rpc)
    return Anchor(fresh.block_number, fresh.block_hash, anchor.tag)


async def _connect_ancestry(previous: Header, tagged: Header, rpc: Rpc) -> None:
    cursor = previous.block_number + 1
    while cursor <= tagged.block_number:
        last = min(cursor + _CHUNK_SIZE - 1, tagged.block_number)
        segment = await rpc.headers(range(cursor, last + 1), _INTEGRITY_PLAN)
        try:
            validate_links(segment, previous)
        except ValueError as error:
            raise BlockweaverError("RPC_MISMATCH", str(error)) from None
        previous = segment[-1]
        cursor = last + 1
    reread = await rpc.header(tagged.block_number, _INTEGRITY_PLAN)
    if not _same_header(tagged, reread) or not _same_core(previous, tagged):
        raise BlockweaverError("RPC_MISMATCH", "Finality tag did not survive numbered reread")


def _same_core(left: Header, right: Header) -> bool:
    return (left.block_number, left.block_hash, left.parent_hash, left.timestamp) == (
        right.block_number,
        right.block_hash,
        right.parent_hash,
        right.timestamp,
    )


async def _check_rows(
    local: dict[int, dict[str, Value]],
    numbers: list[int],
    plan: Plan,
    rpc: Rpc,
    *,
    previous: Header | None = None,
) -> Header | None:
    contiguous = numbers == list(range(numbers[0], numbers[-1] + 1))
    if contiguous:
        headers, rows = await rpc.rows(numbers[0], numbers[-1], plan)
        try:
            validate_links(headers, previous)
        except ValueError as error:
            raise BlockweaverError("RPC_MISMATCH", str(error)) from None
    else:
        headers = await rpc.headers(numbers, plan)
        rows = []
        for header in headers:
            fees = (await rpc.fee_history(header.block_number, header.block_number, plan.percentiles))[0]
            rows.append(header.row(plan, fees))
    for number, row in zip(numbers, rows, strict=True):
        if local[number] != row:
            raise BlockweaverError("RPC_MISMATCH", f"Dataset row {number} does not match verifier RPC")
    return headers[-1] if contiguous else None


async def _check_full_dataset(dataset: Dataset, rpc: Rpc) -> None:
    previous: Header | None = None
    expected = dataset.first_block
    for local in dataset._fact_chunks(_CHUNK_SIZE):
        numbers = list(local)
        if not numbers or numbers[0] != expected:
            raise BlockweaverError("ARTIFACT_INVALID", "Dataset streaming order changed during verification")
        previous = await _check_rows(local, numbers, dataset._plan, rpc, previous=previous)
        expected = numbers[-1] + 1
    if expected != dataset.last_block + 1:
        raise BlockweaverError("ARTIFACT_INVALID", "Dataset streaming coverage changed during verification")


async def _validate_candidate(dataset: Dataset, primary: Rpc | None, verifier: Rpc, chain: Chain) -> None:
    if dataset.chain_id != chain.chain_id:
        raise BlockweaverError("RESUME_MISMATCH", "Ready candidate chain does not match the request")
    verifier_target = await verifier.header(dataset.last_block, dataset._plan)
    if verifier_target.block_hash != dataset._target_hash:
        raise BlockweaverError("RPC_MISMATCH", "Ready candidate target hash does not match verifier RPC")
    if primary is not None:
        target = await primary.header(dataset.last_block, dataset._plan)
        if not _same_header(target, verifier_target):
            raise BlockweaverError("RPC_MISMATCH", "RPC endpoints disagree on the ready candidate target")
    await _refresh_finality(verifier_target, dataset._anchor, verifier)
    numbers = _sample_numbers(dataset.dataset_id, dataset.first_block, dataset.last_block)
    await _check_rows(dataset._facts(numbers), numbers, dataset._plan, verifier)


def _binding(spec: DownloadSpec, resolved: ResolvedRange) -> dict[str, object]:
    return {
        "version": 1,
        "dataset_id": str(spec.dataset_id),
        "chain": {"name": spec.chain.name, "chain_id": spec.chain.chain_id, "finality_tag": spec.chain.finality_tag},
        "source": spec.source.manifest_document(spec.chain, spec.primary, spec.verifier),
        "requested_range": spec.requested_range.document(),
        "resolved_range": resolved.document(),
        "features": list(spec.plan.columns[1:]),
        "format": spec.output_format,
    }


def _manifest(
    spec: DownloadSpec,
    resolved: ResolvedRange,
    target_hash: str,
    anchor: Anchor,
    verification: dict[str, object],
) -> dict[str, object]:
    return {
        "tool_version": __version__,
        "dataset_id": str(spec.dataset_id),
        "chain": {"name": spec.chain.name, "chain_id": spec.chain.chain_id},
        "source": spec.source.manifest_document(spec.chain, spec.primary, spec.verifier),
        "requested_range": spec.requested_range.document(),
        "resolved_range": resolved.document(),
        "schema": spec.plan.schema_document(),
        "acquisition_plan": spec.plan.document(spec.source.name),
        "row_count": resolved.last_block - resolved.first_block + 1,
        "target_hash": target_hash,
        "finalized_anchor": anchor.document(),
        "verification": verification,
    }


def _sample_numbers(dataset_id: UUID, first_block: int, last_block: int) -> list[int]:
    selected = {first_block, last_block}
    available = last_block - first_block - 1
    if available > 0:
        seed = int.from_bytes(hashlib.sha256(dataset_id.bytes).digest()[:8], "big")
        for offset in range(min(3, available)):
            selected.add(first_block + 1 + (seed + offset) % available)
    return sorted(selected)


def _receipt(
    operation: str,
    dataset: Dataset,
    destination: Path,
    reused: int,
    acquired: int,
    anchor: Anchor,
) -> dict[str, object]:
    manifest = dataset._document()
    return {
        "version": 1,
        "operation": operation,
        "dataset_id": str(dataset.dataset_id),
        "path": str(destination),
        "chain": manifest["chain"],
        "resolved_range": manifest["resolved_range"],
        "rows": dataset.row_count,
        "reused_rows": reused,
        "acquired_rows": acquired,
        "finalized_anchor": anchor.document(),
        "artifact_sha256": pair_hashes(dataset.path),
    }
