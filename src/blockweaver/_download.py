"""The download command: one source adapter per acquisition route."""

from __future__ import annotations

from collections.abc import AsyncIterator

from . import __version__
from ._bigquery import (
    BigQueryClient,
    compile_bigquery,
    open_bigquery,
    prepare_query,
    query_chunks,
)
from ._config import (
    BigQueryDownloadRequest,
    DownloadRequest,
    RpcDownloadRequest,
)
from ._contract import (
    BlockweaverError,
    Header,
    Progress,
    ResolvedRange,
    Row,
)
from ._dataset import Artifact
from ._proof import (
    candidate_target,
    finish_candidate_validation,
    prove_finality,
    resolve_range,
    sample_numbers,
    verify_time_boundaries,
)
from ._rpc import (
    CHUNK_SIZE,
    Rpc,
    paired,
)
from ._work import (
    ArtifactSource,
    Publication,
    VerifiedProof,
    materialize_artifact,
)


class RpcSource:
    def __init__(
        self,
        request: RpcDownloadRequest,
        primary: Rpc,
        verifier: Rpc,
        resolved: ResolvedRange,
        primary_chain_id: int,
        verifier_chain_id: int,
    ) -> None:
        self.request = request
        self.primary = primary
        self.verifier = verifier
        self.resolved = resolved
        self._verification = {"primary_chain_id": primary_chain_id, "verifier_chain_id": verifier_chain_id}

    async def chunks(self, first: int, last: int) -> AsyncIterator[list[Row]]:
        while first <= last:
            end = min(first + CHUNK_SIZE - 1, last)
            yield await self.primary.rows(range(first, end + 1), self.request.plan)
            first = end + 1

    async def prove(self, target: Header) -> VerifiedProof:
        anchor = await prove_finality(target, self.verifier, self.request)
        samples = sample_numbers(self.request.dataset_id, self.resolved.first_block, self.resolved.last_block)
        return VerifiedProof(anchor, {**self._verification, "target_agreement": True, "sampled_blocks": samples})

    async def revalidate(self, artifact: Artifact) -> None:
        verifier_target = await candidate_target(artifact, self.verifier)
        target = await self.primary.header(artifact.dataset.last_block)
        if target != verifier_target:
            raise BlockweaverError("RPC_MISMATCH", "RPC endpoints disagree on the ready candidate target")
        await finish_candidate_validation(artifact, verifier_target, self.verifier)


class BigQuerySource:
    def __init__(
        self,
        request: BigQueryDownloadRequest,
        verifier: Rpc,
        warehouse: BigQueryClient,
        resolved: ResolvedRange,
        verifier_chain_id: int,
        progress: Progress,
    ) -> None:
        self.request = request
        self.verifier = verifier
        self.warehouse = warehouse
        self.resolved = resolved
        self.verifier_chain_id = verifier_chain_id
        self.progress = progress
        self.dry_run_bytes = 0

    async def chunks(self, first: int, last: int) -> AsyncIterator[list[Row]]:
        if first > last:
            return
        query = compile_bigquery(self.request.dataset, self.request.plan)
        parameters = {
            "first_block": first,
            "last_block": last,
            "from_timestamp": self.resolved.first_timestamp,
            "to_timestamp": self.resolved.last_timestamp,
        }
        self.dry_run_bytes = prepare_query(
            self.warehouse,
            self.request.dataset,
            query,
            parameters,
            self.request.bigquery.maximum_bytes_billed,
        )
        self.progress({"event": "bigquery_dry_run", "bytes_processed": self.dry_run_bytes})
        for chunk in query_chunks(self.warehouse, query, parameters, self.request.bigquery.maximum_bytes_billed, self.request.plan, CHUNK_SIZE):
            yield chunk

    async def prove(self, target: Header) -> VerifiedProof:
        anchor = await prove_finality(target, self.verifier, self.request)
        samples = sample_numbers(self.request.dataset_id, self.resolved.first_block, self.resolved.last_block)
        return VerifiedProof(
            anchor,
            {
                "verifier_chain_id": self.verifier_chain_id,
                "dry_run_bytes": self.dry_run_bytes,
                "target_agreement": True,
                "sampled_blocks": samples,
            },
        )

    async def revalidate(self, artifact: Artifact) -> None:
        verifier_target = await candidate_target(artifact, self.verifier)
        await finish_candidate_validation(artifact, verifier_target, self.verifier)


async def download(request: DownloadRequest, *, progress: Progress, publication: Publication) -> dict[str, object]:
    progress({"event": "request", "dataset_id": str(request.dataset_id)})
    if isinstance(request, RpcDownloadRequest):
        return await _download_rpc(request, progress, publication)
    return await _download_bigquery(request, progress, publication)


async def _materialize(request: DownloadRequest, source: ArtifactSource, progress: Progress, publication: Publication) -> dict[str, object]:
    return await materialize_artifact(request, source, tool_version=__version__, chunk_size=CHUNK_SIZE, progress=progress, publication=publication)


async def _download_rpc(request: RpcDownloadRequest, progress: Progress, publication: Publication) -> dict[str, object]:
    try:
        async with Rpc(request.primary) as primary, Rpc(request.verifier) as verifier:
            primary_chain_id, verifier_chain_id = await paired(primary.chain_id(), verifier.chain_id())
            if primary_chain_id != request.chain.chain_id or verifier_chain_id != request.chain.chain_id:
                raise BlockweaverError("RPC_CHAIN_MISMATCH", "RPC chain ID does not match the configured chain")
            primary_tag, verifier_tag = await paired(
                primary.tagged_header(request.chain.finality_tag),
                verifier.tagged_header(request.chain.finality_tag),
            )
            resolution = await resolve_range(request.requested_range, primary, min(primary_tag.block_number, verifier_tag.block_number))
            if request.requested_range.kind == "time":
                await verify_time_boundaries(
                    request.requested_range,
                    resolution,
                    verifier,
                    verifier_tag.block_number,
                    independent=True,
                )
            source = RpcSource(request, primary, verifier, resolution.value, primary_chain_id, verifier_chain_id)
            return await _materialize(request, source, progress, publication)
    except BlockweaverError:
        raise
    except ValueError as error:
        raise BlockweaverError("RPC_INVALID", str(error)) from None
    except RuntimeError as error:
        raise BlockweaverError("RPC_FAILED", str(error)) from None
    except OSError as error:
        raise BlockweaverError("IO_FAILED", str(error)) from None


async def _download_bigquery(request: BigQueryDownloadRequest, progress: Progress, publication: Publication) -> dict[str, object]:
    try:
        warehouse = open_bigquery(request.bigquery.project)
        async with Rpc(request.verifier) as verifier:
            verifier_chain_id = await verifier.chain_id()
            if verifier_chain_id != request.chain.chain_id:
                raise BlockweaverError("RPC_CHAIN_MISMATCH", "Verifier RPC chain ID does not match the configured chain")
            tagged = await verifier.tagged_header(request.chain.finality_tag)
            resolution = await resolve_range(request.requested_range, verifier, tagged.block_number)
            if request.requested_range.kind == "time":
                await verify_time_boundaries(
                    request.requested_range,
                    resolution,
                    verifier,
                    tagged.block_number,
                    independent=False,
                )
            source = BigQuerySource(request, verifier, warehouse, resolution.value, verifier_chain_id, progress)
            return await _materialize(request, source, progress, publication)
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
