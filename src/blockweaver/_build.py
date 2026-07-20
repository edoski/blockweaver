"""Acquire, extend, verify, and publish Corpus objects."""

from __future__ import annotations

import hashlib
import signal
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from uuid import UUID

from filelock import FileLock

from ._contract import Anchor, Block, Request, validate_links
from ._corpus import (
    LoadedCorpus,
    canonical_paths,
    checkpoint_paths,
    load_corpus,
    pair_hashes,
    prepare_work,
    publish,
    read_checkpoint,
    write_candidate,
    write_checkpoint,
)
from ._rpc import Rpc

Progress = Callable[[dict[str, object]], None]
_CHECKPOINT_SIZE = 1024


async def acquire_corpus(
    request: Request,
    *,
    storage_root: Path,
    rpc_url: str,
    verify_rpc_url: str,
    batch_size: int,
    concurrency: int,
    progress: Progress,
) -> dict[str, object]:
    return await _build(
        request,
        storage_root=storage_root,
        rpc_url=rpc_url,
        verify_rpc_url=verify_rpc_url,
        batch_size=batch_size,
        concurrency=concurrency,
        progress=progress,
        source=None,
    )


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
) -> dict[str, object]:
    source = load_corpus(source_path)
    if corpus_id == source.request.corpus_id:
        raise ValueError("Extension requires a new corpus_id")
    if last_block <= source.request.last_block:
        raise ValueError("Extension last_block must exceed the source endpoint")
    request = Request(
        corpus_id,
        source.request.chain_id,
        source.request.first_block,
        last_block,
    )
    return await _build(
        request,
        storage_root=storage_root,
        rpc_url=rpc_url,
        verify_rpc_url=verify_rpc_url,
        batch_size=batch_size,
        concurrency=concurrency,
        progress=progress,
        source=source,
    )


async def _build(
    request: Request,
    *,
    storage_root: Path,
    rpc_url: str,
    verify_rpc_url: str,
    batch_size: int,
    concurrency: int,
    progress: Progress,
    source: LoadedCorpus | None,
) -> dict[str, object]:
    if rpc_url == verify_rpc_url:
        raise ValueError("Primary and verifier RPC endpoints must be independent")
    destination, _, _ = canonical_paths(storage_root.resolve(), request.corpus_id)
    source_hashes = pair_hashes(source.path) if source else None
    suffix_first = source.request.last_block + 1 if source else request.first_block
    suffix_request = Request(request.corpus_id, request.chain_id, suffix_first, request.last_block)
    binding: dict[str, object] = {
        "version": "0.1.0",
        "operation": "extend" if source else "acquire",
        "request": request.document(),
    }
    if source:
        binding["source"] = {
            "path": str(source.path),
            "pair_sha256": source_hashes,
        }
    lock_path = destination.parent / f".{request.corpus_id}.lock"
    with FileLock(lock_path):
        hidden, chunks = prepare_work(storage_root.resolve(), request, binding)
        paths, next_block, previous = checkpoint_paths(chunks, suffix_request, _CHECKPOINT_SIZE)
        reused_suffix = next_block - suffix_first
        progress(
            {
                "event": "resume",
                "reused_rows": (source.frame.height if source else 0) + reused_suffix,
            }
        )
        async with (
            Rpc(rpc_url, batch_size=batch_size, concurrency=concurrency) as primary,
            Rpc(verify_rpc_url, batch_size=batch_size, concurrency=concurrency) as verifier,
        ):
            primary_chain, verifier_chain = await primary.chain_id(), await verifier.chain_id()
            if primary_chain != request.chain_id or verifier_chain != request.chain_id:
                raise ValueError("RPC chain ID does not match the Corpus request")
            boundary: Block | None = None
            if source:
                boundary = await _validate_source_boundary(source, primary, verifier)
                if previous is not None:
                    first_saved = read_checkpoint(paths[0], request.chain_id)[0]
                    validate_links([first_saved], boundary)
            while next_block <= request.last_block:
                last = min(next_block + _CHECKPOINT_SIZE - 1, request.last_block)
                blocks = await primary.blocks(range(next_block, last + 1), chain_id=request.chain_id)
                validate_links(blocks, previous or boundary)
                path = chunks / f"{next_block:020d}-{last:020d}.parquet"
                write_checkpoint(path, blocks)
                paths.append(path)
                previous = blocks[-1]
                next_block = last + 1
                progress(
                    {
                        "event": "checkpoint",
                        "first_block": blocks[0].block_number,
                        "last_block": last,
                    }
                )
            assert previous is not None
            anchor, verifier_fact = await _prove_finality(previous, primary, verifier, request)
            sources = ([source.path / "blocks.parquet"] if source else []) + paths
            corpus_path, blocks_path = write_candidate(hidden, request, anchor, sources)
            candidate = load_corpus(hidden, exact_files=False)
            samples = await _check_samples(candidate, primary, _sample_numbers(request, source))
            if source and pair_hashes(source.path) != source_hashes:
                raise ValueError("Source Corpus changed during extension")
        hashes = {
            "corpus.json": hashlib.sha256(corpus_path.read_bytes()).hexdigest(),
            "blocks.parquet": hashlib.sha256(blocks_path.read_bytes()).hexdigest(),
        }
        with _publication_signals_blocked():
            publish(hidden, destination)
        progress({"event": "published", "corpus_id": str(request.corpus_id)})
    prefix_rows = source.frame.height if source else 0
    return _receipt(
        operation="extend" if source else "acquire",
        request=request,
        path=destination,
        source=source,
        reused=prefix_rows + reused_suffix,
        acquired=request.last_block - suffix_first + 1 - reused_suffix,
        anchor=anchor,
        hashes=hashes,
        samples=samples,
        verifier=verifier_fact,
    )


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
    progress({"event": "local_valid", "rows": corpus.frame.height})
    sample_numbers = _sample_numbers(corpus.request, None)
    numbers = list(range(corpus.request.first_block, corpus.request.last_block + 1)) if full_rpc else sample_numbers
    samples: list[dict[str, object]]
    verifier_fact: dict[str, object] = {"mode": "local"}
    if rpc_url is None:
        if full_rpc:
            raise ValueError("--full-rpc requires --rpc-url or BLOCKWEAVER_RPC_URL")
        samples = [_local_fact(corpus, number) for number in sample_numbers]
    else:
        async with Rpc(rpc_url, batch_size=batch_size, concurrency=concurrency) as rpc:
            chain_id = await rpc.chain_id()
            if chain_id != corpus.request.chain_id:
                raise ValueError("RPC chain ID does not match the Corpus")
            samples = await _check_samples(corpus, rpc, numbers, set(sample_numbers))
            stored_anchor = (await rpc.blocks([corpus.anchor.block_number], chain_id=chain_id))[0]
            if stored_anchor.block_hash != corpus.anchor.block_hash:
                raise ValueError("Stored finalized anchor no longer matches RPC")
            fresh = await rpc.tagged_block("finalized", chain_id=chain_id)
            if fresh.block_number < corpus.request.last_block:
                raise ValueError("RPC finalized head does not cover the Corpus")
            reread = (await rpc.blocks([fresh.block_number], chain_id=chain_id))[0]
            if fresh != reread:
                raise ValueError("Finalized tag changed during verification")
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
        source=None,
        reused=corpus.frame.height,
        acquired=0,
        anchor=corpus.anchor,
        hashes=pair_hashes(corpus.path),
        samples=samples,
        verifier=verifier_fact,
    )


async def _validate_source_boundary(source: LoadedCorpus, primary: Rpc, verifier: Rpc) -> Block:
    number = source.request.last_block
    left, right = (
        await primary.blocks([number], chain_id=source.request.chain_id),
        await verifier.blocks([number], chain_id=source.request.chain_id),
    )
    if left[0] != right[0]:
        raise ValueError("RPC endpoints disagree on the source boundary")
    if left[0].durable_row() != _local_fact(source, number):
        raise ValueError("Source boundary does not match RPC")
    return left[0]


async def _prove_finality(target: Block, primary: Rpc, verifier: Rpc, request: Request) -> tuple[Anchor, dict[str, object]]:
    verifier_target = (await verifier.blocks([target.block_number], chain_id=request.chain_id))[0]
    if target != verifier_target:
        raise ValueError("RPC endpoints disagree on the target block")
    tagged = await verifier.tagged_block("finalized", chain_id=request.chain_id)
    if tagged.block_number < target.block_number:
        raise ValueError("Verifier finalized head does not cover the target")
    previous = verifier_target
    cursor = target.block_number + 1
    while cursor <= tagged.block_number:
        last = min(cursor + _CHECKPOINT_SIZE - 1, tagged.block_number)
        segment = await verifier.blocks(range(cursor, last + 1), chain_id=request.chain_id)
        validate_links(segment, previous)
        previous = segment[-1]
        cursor = last + 1
    reread = (await verifier.blocks([tagged.block_number], chain_id=request.chain_id))[0]
    if tagged != reread or previous != tagged:
        raise ValueError("Finalized tag did not survive numbered reread")
    return Anchor(tagged.block_number, tagged.block_hash), {
        "chain_id": request.chain_id,
        "target_block_hash": target.block_hash,
        "finalized_block_number": tagged.block_number,
        "finalized_block_hash": tagged.block_hash,
    }


async def _check_samples(corpus: LoadedCorpus, rpc: Rpc, numbers: list[int], receipt_numbers: set[int] | None = None) -> list[dict[str, object]]:
    facts: list[dict[str, object]] = []
    contiguous = numbers == list(range(numbers[0], numbers[-1] + 1))
    previous: Block | None = None
    for offset in range(0, len(numbers), _CHECKPOINT_SIZE):
        remote = await rpc.blocks(numbers[offset : offset + _CHECKPOINT_SIZE], chain_id=corpus.request.chain_id)
        if contiguous:
            validate_links(remote, previous)
            previous = remote[-1]
        for block in remote:
            if block.durable_row() != _local_fact(corpus, block.block_number):
                raise ValueError(f"Corpus row {block.block_number} does not match RPC")
            if receipt_numbers is None or block.block_number in receipt_numbers:
                facts.append({**block.durable_row(), "block_hash": block.block_hash})
    return facts


def _local_fact(corpus: LoadedCorpus, number: int) -> dict[str, object]:
    row = corpus.frame.row(number - corpus.request.first_block, named=True)
    if row["block_number"] != number:
        raise ValueError(f"Corpus row {number} is missing")
    return {name: int(value) for name, value in row.items()}


def _sample_numbers(request: Request, source: LoadedCorpus | None) -> list[int]:
    selected = {request.first_block, request.last_block}
    if source:
        selected.update({source.request.last_block, source.request.last_block + 1})
    width = request.last_block - request.first_block + 1
    for counter in range(3):
        digest = hashlib.sha256(request.corpus_id.bytes + bytes([counter])).digest()
        selected.add(request.first_block + int.from_bytes(digest[:8], "big") % width)
    return sorted(selected)


def _receipt(
    *,
    operation: str,
    request: Request,
    path: Path,
    source: LoadedCorpus | None,
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
        "source_rows": source.frame.height if source else 0,
        "reused_rows": reused,
        "acquired_rows": acquired,
        "finalized_anchor": anchor.document(),
        "pair_sha256": hashes,
        "samples": samples,
        "verifier": verifier,
    }
    if source:
        receipt["source_corpus_id"] = str(source.request.corpus_id)
    return receipt


@contextmanager
def _publication_signals_blocked() -> Iterator[None]:
    blocked = {signal.SIGINT, signal.SIGTERM}
    previous = signal.pthread_sigmask(signal.SIG_BLOCK, blocked)
    try:
        yield
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, previous)
