from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from support import ChainServer, artifact_from, block_hash, download_args, error, invoke

from blockweaver import BlockweaverError, _proof, open_dataset


def header_reads(server: ChainServer) -> int:
    return sum(1 for batch in server.requests for call in batch if call["method"] == "eth_getBlockByNumber")


def test_proof_cost_does_not_grow_with_the_distance_to_the_head(tmp_path: Path, make_config: Any) -> None:
    download_reads, verify_reads = [], []
    for head in (30, 16_030):
        with ChainServer(finalized=head) as primary, ChainServer(finalized=head) as verifier:
            config = make_config(tmp_path / f"{head}.toml", primary, verifier, output_root=tmp_path / str(head))
            artifact = artifact_from(invoke(download_args(config)))
            download_reads.append(header_reads(verifier))
            verifier.requests.clear()
            verifier.finalized = head * 2
            assert invoke(["verify", str(artifact), "--rpc-url", verifier.url]).exit_code == 0
            verify_reads.append(header_reads(verifier))

    assert download_reads[0] == download_reads[1] <= 12
    assert verify_reads[0] == verify_reads[1] <= 10


def test_verify_is_strict_locally_and_against_rpc(tmp_path: Path, chains: tuple[ChainServer, ChainServer], make_config: Any) -> None:
    primary, verifier = chains
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out")
    artifact = artifact_from(invoke(download_args(config)))
    full = invoke(["verify", str(artifact), "--config", str(config), "--provider", "verifier", "--full-rpc"])
    full_receipt = json.loads(full.stdout)
    assert set(full_receipt) == {"operation", "dataset_id", "path", "rows", "artifact_sha256", "verification"}
    assert full_receipt["operation"] == "verify" and full_receipt["verification"]["mode"] == "full_rpc"
    moved = artifact.with_name("22222222-2222-4222-8222-222222222222")
    artifact.rename(moved)
    with pytest.raises(BlockweaverError, match="directory name"):
        open_dataset(moved)
    moved.rename(artifact)
    frame = pl.read_parquet(artifact / "blocks.parquet")
    frame.with_columns(pl.when(pl.col("block_number") == 12).then(999).otherwise(pl.col("timestamp")).alias("timestamp")).write_parquet(
        artifact / "blocks.parquet"
    )
    assert error(invoke(["verify", str(artifact)]))["code"] == "ARTIFACT_INVALID"


@pytest.mark.parametrize(
    "options,message",
    [
        (["--full-rpc"], "--full-rpc, --batch-size, --concurrency, and --timeout require --provider or --rpc-url"),
        (["--batch-size", "1"], "--full-rpc, --batch-size, --concurrency, and --timeout require --provider or --rpc-url"),
        (["--concurrency", "1"], "--full-rpc, --batch-size, --concurrency, and --timeout require --provider or --rpc-url"),
        (["--timeout", "1"], "--full-rpc, --batch-size, --concurrency, and --timeout require --provider or --rpc-url"),
        (["--config", "missing.toml"], "--config requires --provider for RPC verification"),
        (["--config", "missing.toml", "--rpc-url", "http://127.0.0.1:1"], "--config requires --provider for RPC verification"),
    ],
)
def test_verify_rejects_inapplicable_rpc_options_before_dataset_or_network(
    options: list[str],
    message: str,
    chains: tuple[ChainServer, ChainServer],
) -> None:
    primary, verifier = chains
    failure = error(invoke(["verify", "missing-dataset", *options]))
    assert failure == {"event": "error", "code": "VERIFY_INVALID", "message": message}
    assert primary.requests == verifier.requests == []


@pytest.mark.parametrize(
    "options,use_environment,expected_server",
    [
        ([], False, None),
        (["--rpc-url", "{url}"], False, "verifier"),
        (["--rpc-url", "{url}", "--full-rpc", "--batch-size", "2", "--concurrency", "1", "--timeout", "1"], False, "verifier"),
        (["--provider", "verifier"], True, "verifier"),
        (["--config", "{config}", "--provider", "verifier"], False, "verifier"),
        (["--config", "{config}", "--provider", "verifier", "--batch-size", "2", "--concurrency", "1", "--timeout", "1"], False, "verifier"),
        (["--config", "{config}", "--provider", "verifier", "--rpc-url", "{override_url}"], False, "primary"),
    ],
)
def test_verify_preserves_each_meaningful_mode(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
    options: list[str],
    use_environment: bool,
    expected_server: str | None,
) -> None:
    primary, verifier = chains
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out")
    artifact = artifact_from(invoke(download_args(config)))
    primary.requests.clear()
    verifier.requests.clear()
    resolved = [item.format(url=verifier.url, override_url=primary.url, config=config) for item in options]
    env = {"BLOCKWEAVER_CONFIG": str(config)} if use_environment else None

    result = invoke(["verify", str(artifact), *resolved], env)

    assert result.exit_code == 0, result.output
    expected_mode = "local" if not resolved else "full_rpc" if "--full-rpc" in resolved else "sample_rpc"
    assert json.loads(result.stdout)["verification"]["mode"] == expected_mode
    assert bool(primary.requests) is (expected_server == "primary")
    assert bool(verifier.requests) is (expected_server == "verifier")


def test_rpc_verify_reseals_artifact_bytes_after_network_wait(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary, verifier = chains
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out")
    artifact = artifact_from(invoke(download_args(config)))
    real_verify = _proof.verify_rpc

    async def mutate_after_rpc(artifact: Any, provider: Any, full: bool) -> dict[str, object]:
        result = await real_verify(artifact, provider, full)
        with artifact.dataset.data_path.open("ab") as stream:
            stream.write(b"changed")
        return result

    monkeypatch.setattr(_proof, "verify_rpc", mutate_after_rpc)

    result = invoke(["verify", str(artifact), "--config", str(config), "--provider", "verifier"])

    assert error(result)["code"] == "ARTIFACT_INVALID"


def test_full_rpc_verification_is_chunked_and_checks_cross_chunk_ancestry(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary, verifier = chains
    config = make_config(
        tmp_path / "config.toml",
        primary,
        verifier,
        output_root=tmp_path / "out",
        features=("effective_priority_fee_per_gas_p50",),
    )
    artifact = artifact_from(invoke(download_args(config, last=15)))
    monkeypatch.setattr(_proof, "CHUNK_SIZE", 2)
    verifier.requests.clear()

    verified = invoke(["verify", str(artifact), "--config", str(config), "--provider", "verifier", "--full-rpc"])
    assert verified.exit_code == 0, verified.output
    fee_calls = [call for batch in verifier.requests for call in batch if call["method"] == "eth_feeHistory"]
    assert len(fee_calls) == 3
    assert all(int(call["params"][0], 16) <= 2 for call in fee_calls)

    verifier.changes[12] = {"parentHash": block_hash(1)}
    mismatch = invoke(["verify", str(artifact), "--config", str(config), "--provider", "verifier", "--full-rpc"])
    assert error(mismatch)["code"] == "RPC_MISMATCH"
