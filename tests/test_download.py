from __future__ import annotations

import errno
import hashlib
import json
import shutil
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import polars as pl
import pytest
from support import DATASET_ID, ChainServer, FakeBigQuery, artifact_from, bigquery_config, block_hash, download_args, error, invoke

from blockweaver import Dataset, _download, _work, open_dataset
from blockweaver._config import parse_time


def test_parquet_download_selects_and_coalesces_features(tmp_path: Path, chains: tuple[ChainServer, ChainServer], make_config: Any) -> None:
    primary, verifier = chains
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out", features=())
    primary.http_failures = 1
    result = invoke(
        [
            *download_args(config),
            "--feature",
            "timestamp",
            "--feature",
            "block_hash",
            "--feature",
            "gas_used",
            "--feature",
            "effective_priority_fee_per_gas_p50",
        ]
    )
    artifact = artifact_from(result)
    receipt = json.loads(result.stdout)
    assert set(receipt) == {
        "operation",
        "dataset_id",
        "path",
        "chain",
        "resolved_range",
        "rows",
        "reused_rows",
        "acquired_rows",
        "finalized_anchor",
        "artifact_sha256",
    }
    assert receipt["operation"] == "download"
    assert receipt["reused_rows"] == 0
    assert receipt["acquired_rows"] == receipt["rows"] == 5
    dataset = open_dataset(str(artifact))
    assert isinstance(dataset, Dataset) and artifact == tmp_path / "out" / DATASET_ID == dataset.path
    with pytest.raises(TypeError, match="open_dataset"):
        Dataset()
    assert (str(dataset.dataset_id), dataset.chain_name, dataset.chain_id, dataset.first_block, dataset.last_block) == (DATASET_ID, "test", 1, 10, 14)
    assert dataset.schema == ("block_number", "timestamp", "block_hash", "gas_used", "effective_priority_fee_per_gas_p50")
    assert dataset.output_format == "parquet" and dataset.row_count == 5
    assert {path.name for path in artifact.iterdir()} == {"manifest.json", "blocks.parquet"}
    frame = pl.read_parquet(artifact / "blocks.parquet")
    assert frame.schema == {
        "block_number": pl.Int64,
        "timestamp": pl.Int64,
        "block_hash": pl.String,
        "gas_used": pl.Int64,
        "effective_priority_fee_per_gas_p50": pl.Int64,
    }
    assert frame["block_number"].to_list() == [10, 11, 12, 13, 14]
    assert frame["gas_used"].to_list() == [0, 21_000, 42_000, 101_000, 100_000]
    assert frame["effective_priority_fee_per_gas_p50"].to_list() == [0, 11, 12, 18, 4]
    fee_calls = [call for batch in primary.requests for call in batch if call["method"] == "eth_feeHistory"]
    assert [call["params"] for call in fee_calls] == [["0x5", "0xe", [50]]]
    verifier_fee_calls = [call for batch in verifier.requests for call in batch if call["method"] == "eth_feeHistory"]
    assert [call["params"] for call in verifier_fee_calls] == [["0x5", "0xe", [50]]]
    block_calls = [call for batch in primary.requests for call in batch if call["method"] == "eth_getBlockByNumber"]
    acquired = [call for call in block_calls if call["params"][0] in {hex(number) for number in range(10, 15)}]
    assert all(call["params"][1] is False for call in acquired)
    manifest_text = (artifact / "manifest.json").read_text()
    manifest = json.loads(manifest_text)
    assert manifest_text.endswith("\n")
    identity_keys = {"tool_version", "dataset_id", "completed_at", "chain", "source", "requested_range", "resolved_range"}
    artifact_keys = {"schema", "acquisition_plan", "row_count", "output", "target_hash", "finalized_anchor", "verification"}
    assert set(manifest) == identity_keys | artifact_keys
    assert {"manifest_version", "dataset_version"}.isdisjoint(manifest)
    assert manifest["schema"] == [
        {"name": "block_number", "type": "Int64", "unit": "block"},
        {"name": "timestamp", "type": "Int64", "unit": "unix_second"},
        {"name": "block_hash", "type": "UTF-8", "unit": "hex"},
        {"name": "gas_used", "type": "Int64", "unit": "gas"},
        {"name": "effective_priority_fee_per_gas_p50", "type": "Int64", "unit": "wei/gas"},
    ]
    assert manifest["source"] == {"type": "rpc", "provider": "primary", "verifier": "verifier"}
    assert manifest["verification"]["target_agreement"] is True
    assert primary.url not in manifest_text and verifier.url not in manifest_text
    events = [json.loads(line)["event"] for line in result.stderr.splitlines()]
    assert events == ["request", "checkpoint", "published"], "a fresh download reports no resume"


def test_header_only_tx_count_uses_only_block_cardinality(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
) -> None:
    primary, verifier = chains
    primary.changes[11] = {"transactions": [{"opaque": "transaction"}]}
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out", features=("tx_count",))
    artifact = artifact_from(invoke(download_args(config)))
    assert pl.read_parquet(artifact / "blocks.parquet")["tx_count"].to_list() == [0, 1, 2, 3, 3]
    assert {call["method"] for batch in primary.requests for call in batch} == {"eth_chainId", "eth_getBlockByNumber"}


def test_download_reads_each_range_boundary_a_fixed_number_of_times(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary, verifier = chains
    config = make_config(tmp_path / "rpc.toml", primary, verifier, output_root=tmp_path / "rpc", features=("timestamp",))
    artifact_from(invoke(download_args(config, first=10, last=10)))
    assert primary.request_counts[10] == 2, "one read for resolution and one for acquisition"

    verifier.request_counts.clear()
    warehouse = bigquery_config(make_config, tmp_path / "bigquery.toml", primary, verifier, tmp_path / "bigquery", features=("timestamp",))
    monkeypatch.setattr(_download, "open_bigquery", lambda _project: FakeBigQuery(verifier))
    start = datetime.fromtimestamp(verifier.timestamp_base + 10, UTC).isoformat().replace("+00:00", "Z")
    end = datetime.fromtimestamp(verifier.timestamp_base + 14, UTC).isoformat().replace("+00:00", "Z")
    artifact_from(invoke(["download", "--config", str(warehouse), "--id", DATASET_ID, "--from-time", start, "--to-time", end]))
    assert {number: verifier.request_counts[number] for number in (10, 14)} == {10: 2, 14: 4}


def test_time_range_csv_and_reduced_precision(tmp_path: Path, chains: tuple[ChainServer, ChainServer], make_config: Any) -> None:
    primary, verifier = chains
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out", output_format="csv")
    start = datetime.fromtimestamp(primary.timestamp_base + 10, UTC).isoformat().replace("+00:00", "Z")
    end = datetime.fromtimestamp(primary.timestamp_base + 14, UTC).isoformat().replace("+00:00", "Z")
    result = invoke(
        [
            "download",
            "--config",
            str(config),
            "--id",
            DATASET_ID,
            "--from-time",
            start,
            "--to-time",
            end,
        ]
    )
    artifact = artifact_from(result)
    assert (artifact / "blocks.csv").read_text().splitlines()[0] == "block_number,timestamp,block_hash"
    manifest = json.loads((artifact / "manifest.json").read_text())
    assert manifest["requested_range"]["kind"] == "time"
    assert manifest["resolved_range"] == {
        "from_block": 10,
        "to_block": 14,
        "from_timestamp": primary.timestamp_base + 10,
        "to_timestamp": primary.timestamp_base + 14,
    }
    assert parse_time("2026-01-02", end=True) - parse_time("2026-01-02", end=False) == 86_399
    assert parse_time("2026-01-02T10+01:00", end=True) - parse_time("2026-01-02T10+01:00", end=False) == 3_599
    assert invoke(["verify", str(artifact)]).exit_code == 0


def test_time_resolution_requires_independent_adjacent_boundary_proof(tmp_path: Path, chains: tuple[ChainServer, ChainServer], make_config: Any) -> None:
    primary, verifier = chains
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out")
    primary.changes[10] = {"timestamp": hex(primary.timestamp_base + 9)}
    start = datetime.fromtimestamp(primary.timestamp_base + 10, UTC).isoformat().replace("+00:00", "Z")
    end = datetime.fromtimestamp(primary.timestamp_base + 14, UTC).isoformat().replace("+00:00", "Z")

    failure = invoke(
        [
            "download",
            "--config",
            str(config),
            "--id",
            DATASET_ID,
            "--from-time",
            start,
            "--to-time",
            end,
        ]
    )
    assert error(failure)["code"] == "RPC_MISMATCH"
    assert not any(path.name.endswith(DATASET_ID) and not path.name.startswith(".") for path in (tmp_path / "out").glob("*"))


@pytest.mark.parametrize(
    ("extra", "code"),
    [
        (["--feature", "unknown"], "FEATURE_INVALID"),
        (["--from-time", "2026-01-01", "--to-time", "2026-01-02"], "RANGE_INVALID"),
        (["--from-block", "31", "--to-block", "31"], "RANGE_UNFINALIZED"),
    ],
)
def test_invalid_requests_fail_before_publication(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
    extra: list[str],
    code: str,
) -> None:
    primary, verifier = chains
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out")
    base = download_args(config)
    if extra[0] == "--from-block":
        base = base[:5]
    failure = error(invoke([*base, *extra]))
    assert failure["code"] == code
    if code in {"FEATURE_INVALID", "RANGE_INVALID"}:
        assert primary.requests == verifier.requests == []
    assert not any(path.name.endswith(DATASET_ID) for path in (tmp_path / "out").glob("*")) if (tmp_path / "out").exists() else True


def test_resume_reuses_only_complete_chunks_and_rejects_rebinding(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary, verifier = chains
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out")
    monkeypatch.setattr(_download, "CHUNK_SIZE", 2)
    primary.changes[12] = {"hash": "invalid"}
    failed = invoke(download_args(config))
    assert error(failed)["code"] == "RPC_INVALID"
    assert primary.request_counts[10] == 2
    assert primary.request_counts[11] == 1

    rebound = error(invoke(download_args(config, last=15)))
    assert rebound["code"] == "RESUME_MISMATCH"
    primary.changes.clear()
    result = invoke(download_args(config))
    receipt = json.loads(result.stdout)
    assert receipt["reused_rows"] == 2
    assert receipt["acquired_rows"] == 3
    assert {"event": "resume", "reused_rows": 2} in [json.loads(line) for line in result.stderr.splitlines()]
    assert primary.request_counts[10] == 4
    assert primary.request_counts[11] == 1


@pytest.mark.parametrize("corruption", ["digest", "domain"])
def test_resume_integrity_binds_checkpoint_bytes_and_domains(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
    monkeypatch: pytest.MonkeyPatch,
    corruption: str,
) -> None:
    primary, verifier = chains
    config = make_config(
        tmp_path / "config.toml",
        primary,
        verifier,
        output_root=tmp_path / "out",
        features=("timestamp", "base_fee_per_gas"),
    )
    monkeypatch.setattr(_download, "CHUNK_SIZE", 2)
    primary.changes[12] = {"hash": "invalid"}
    assert error(invoke(download_args(config)))["code"] == "RPC_INVALID"
    primary.changes.clear()

    chunks = tmp_path / "out" / f".blockweaver-{DATASET_ID}" / "chunks"
    checkpoint = next(chunks.iterdir())
    frame = pl.read_parquet(checkpoint)
    replacement = pl.lit(0) if corruption == "domain" else pl.col("base_fee_per_gas") + 1
    frame.with_columns(replacement.alias("base_fee_per_gas")).write_parquet(checkpoint)
    if corruption == "domain":
        first, last, _digest = checkpoint.stem.split("-")
        digest = hashlib.sha256(checkpoint.read_bytes()).hexdigest()
        checkpoint.rename(checkpoint.with_name(f"{first}-{last}-{digest}.parquet"))

    assert error(invoke(download_args(config)))["code"] == "RESUME_INVALID"
    assert primary.request_counts[13] == 1, "a bad checkpoint fails before any further acquisition"


def test_checkpoint_changed_during_acquisition_is_never_assembled(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary, verifier = chains
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out")
    monkeypatch.setattr(_download, "CHUNK_SIZE", 2)
    chunks = tmp_path / "out" / f".blockweaver-{DATASET_ID}" / "chunks"

    def change_first_checkpoint() -> None:
        with min(chunks.iterdir()).open("ab") as stream:
            stream.write(b"changed")

    primary.when_block_requested[13] = change_first_checkpoint

    assert error(invoke(download_args(config)))["code"] == "RESUME_INVALID"
    assert not (tmp_path / "out" / DATASET_ID).exists()


@pytest.mark.parametrize(
    ("features", "change", "message"),
    [
        (("timestamp",), {"timestamp": hex(2**63)}, "exceeds signed Int64"),
        (("gas_used",), {"gasUsed": hex(30_000_001)}, "Block 12 has gas_used outside its domain"),
        (("base_fee_per_gas",), {"baseFeePerGas": "0x0"}, "Block 12 has base_fee_per_gas outside its domain"),
        (("block_hash",), {"hash": "0xABC"}, "Block 12 has block_hash outside its domain"),
    ],
)
def test_download_rejects_header_values_outside_their_domain(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
    features: tuple[str, ...],
    change: dict[str, str],
    message: str,
) -> None:
    primary, verifier = chains
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out", features=features)
    primary.changes[12] = change
    failure = error(invoke(download_args(config)))
    assert failure["code"] == "RPC_INVALID" and message in failure["message"]


def test_download_rejects_fee_history_for_other_blocks(tmp_path: Path, chains: tuple[ChainServer, ChainServer], make_config: Any) -> None:
    primary, verifier = chains
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out", features=("effective_priority_fee_per_gas_p50",))
    primary.fee_history_changes = {"oldestBlock": hex(9)}
    failure = error(invoke(download_args(config)))
    assert failure["code"] == "RPC_INVALID" and "oldestBlock" in failure["message"]


def test_verifier_disagreement_and_secret_redaction(tmp_path: Path, chains: tuple[ChainServer, ChainServer], make_config: Any) -> None:
    primary, verifier = chains
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out")
    verifier.changes[14] = {"hash": block_hash(999)}
    failure = invoke(download_args(config))
    message = error(failure)
    assert message["code"] == "RPC_MISMATCH"
    assert primary.url not in failure.output and verifier.url not in failure.output
    assert not any(path.name.endswith(DATASET_ID) and not path.name.startswith(".") for path in (tmp_path / "out").glob("*"))


def test_verifier_finality_must_still_cover_the_target_at_proof_time(tmp_path: Path, chains: tuple[ChainServer, ChainServer], make_config: Any) -> None:
    primary, verifier = chains
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out")
    primary.when_block_requested[13] = lambda: setattr(verifier, "finalized", 12)

    assert error(invoke(download_args(config)))["code"] == "RANGE_UNFINALIZED"
    assert not (tmp_path / "out" / DATASET_ID).exists()


def test_publication_failure_recovers_ready_candidate(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary, verifier = chains
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out")
    real_rename = _work._rename_no_replace

    def interrupt(_source: Path, _destination: Path) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(_work, "_rename_no_replace", interrupt)
    assert error(invoke(download_args(config)))["code"] == "INTERRUPTED"
    hidden = tmp_path / "out" / f".blockweaver-{DATASET_ID}"
    assert {path.name for path in (hidden / "ready").iterdir()} == {"manifest.json", "blocks.parquet"}
    assert {path.name for path in hidden.iterdir()} == {"binding.json", "chunks", "ready"}

    requests = len(primary.requests) + len(verifier.requests)
    monkeypatch.setattr(_work, "_rename_no_replace", real_rename)
    artifact = artifact_from(invoke(download_args(config)))
    assert len(primary.requests) + len(verifier.requests) > requests  # range and identity are re-resolved
    assert not hidden.exists()
    assert error(invoke(download_args(config)))["code"] == "DESTINATION_EXISTS"
    assert artifact.exists()


@pytest.mark.parametrize("published", [False, True])
def test_recovery_rejects_artifacts_outside_the_immutable_binding(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
    monkeypatch: pytest.MonkeyPatch,
    published: bool,
) -> None:
    primary, verifier = chains
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out")
    real_rename, real_discard = _work._rename_no_replace, _work._discard_work

    def interrupt(*_args: object) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(_work, "_discard_work" if published else "_rename_no_replace", interrupt)
    assert error(invoke(download_args(config)))["code"] == "INTERRUPTED"
    monkeypatch.setattr(_work, "_rename_no_replace", real_rename)
    monkeypatch.setattr(_work, "_discard_work", real_discard)

    hidden = tmp_path / "out" / f".blockweaver-{DATASET_ID}"
    artifact = next(path for path in (tmp_path / "out").iterdir() if not path.name.startswith(".")) if published else hidden / "ready"
    manifest_path = artifact / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["source"]["provider"] = "other"
    manifest_path.write_text(json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n")

    assert error(invoke(download_args(config)))["code"] == "RESUME_MISMATCH"


def test_publication_never_replaces_a_racing_destination(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary, verifier = chains
    output_root = tmp_path / "out"
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=output_root)
    real_rename = _work._rename_no_replace

    def race(source: Path, destination: Path) -> None:
        destination.mkdir()
        real_rename(source, destination)

    monkeypatch.setattr(_work, "_rename_no_replace", race)
    result = invoke(download_args(config))
    assert error(result)["code"] == "DESTINATION_EXISTS"
    destination = next(path for path in output_root.iterdir() if not path.name.startswith("."))
    assert list(destination.iterdir()) == []
    assert (output_root / f".blockweaver-{DATASET_ID}" / "ready").is_dir()


@pytest.mark.parametrize(
    "dataset_id",
    [
        "44444444-4444-4444-8444-444444444444",
        "55555555-5555-4555-8555-555555555555",
        "66666666-6666-4666-8666-666666666666",
        "77777777-7777-4777-8777-777777777777",
    ],
)
def test_same_uuid_concurrent_cli_has_one_publication_and_stable_loser(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
    dataset_id: str,
) -> None:
    primary, verifier = chains
    output_root = tmp_path / "out"
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=output_root)
    command = [
        sys.executable,
        "-m",
        "blockweaver",
        "download",
        "--config",
        str(config),
        "--id",
        dataset_id,
        "--from-block",
        "10",
        "--to-block",
        "30",
    ]
    processes = [subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) for _ in range(2)]
    results = [(process.returncode, stdout, stderr) for process in processes for stdout, stderr in [process.communicate(timeout=30)]]

    assert sorted(code for code, _stdout, _stderr in results) == [0, 1]
    winner = next(json.loads(stdout) for code, stdout, _stderr in results if code == 0)
    loser = next(json.loads(stderr.splitlines()[-1]) for code, _stdout, stderr in results if code == 1)
    assert loser["code"] == "DESTINATION_EXISTS"
    assert "ENOENT" not in "".join(stdout + stderr for _code, stdout, stderr in results)
    artifact = Path(winner["path"])
    assert artifact == output_root / dataset_id and open_dataset(artifact).row_count == 21
    assert not (output_root / f".blockweaver-{dataset_id}").exists()


@pytest.mark.parametrize("state", ["incomplete", "provisional"])
def test_incomplete_work_states_restart_cleanly(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
    state: str,
) -> None:
    primary, verifier = chains
    output_root = tmp_path / "out"
    hidden = output_root / f".blockweaver-{DATASET_ID}"
    hidden.mkdir(parents=True)
    if state == "incomplete":
        (hidden / "orphan").write_text("incomplete")
    else:
        (hidden / "ready.tmp").mkdir()
        (hidden / "ready.tmp" / "partial").write_text("partial")
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=output_root)

    artifact = artifact_from(invoke(download_args(config)))

    assert artifact.is_dir() and not hidden.exists()


def test_staged_recovery_ignores_obsolete_checkpoints_and_revalidates(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary, verifier = chains
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out")
    real_rename = _work._rename_no_replace
    monkeypatch.setattr(_work, "_rename_no_replace", Mock(side_effect=KeyboardInterrupt))
    assert error(invoke(download_args(config)))["code"] == "INTERRUPTED"
    hidden = tmp_path / "out" / f".blockweaver-{DATASET_ID}"
    assert "version" not in json.loads((hidden / "binding.json").read_text())
    with next((hidden / "chunks").iterdir()).open("ab") as stream:
        stream.write(b"obsolete checkpoint corruption")

    monkeypatch.setattr(_work, "_rename_no_replace", real_rename)
    verifier.changes[14] = {"hash": block_hash(999)}
    assert error(invoke(download_args(config)))["code"] == "RPC_MISMATCH"
    verifier.changes.clear()

    receipt = json.loads(invoke(download_args(config)).stdout)

    assert receipt["reused_rows"] == receipt["rows"] and receipt["acquired_rows"] == 0
    assert not hidden.exists()


def test_committed_recovery_survives_partial_hidden_cleanup(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary, verifier = chains
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out")
    real_discard = _work._discard_work
    monkeypatch.setattr(_work, "_discard_work", Mock(side_effect=KeyboardInterrupt))
    assert error(invoke(download_args(config)))["code"] == "INTERRUPTED"
    hidden = tmp_path / "out" / f".blockweaver-{DATASET_ID}"
    (hidden / "binding.json").unlink()
    shutil.rmtree(hidden / "chunks")
    monkeypatch.setattr(_work, "_discard_work", real_discard)

    receipt = json.loads(invoke(download_args(config)).stdout)

    assert receipt["reused_rows"] == receipt["rows"] and receipt["acquired_rows"] == 0
    assert not hidden.exists()


def test_final_candidate_samples_must_match_verifier(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary, verifier = chains
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out", features=("timestamp",))
    real_write = _work._write_candidate

    def change_candidate(*args: Any, **kwargs: Any) -> Any:
        artifact = real_write(*args, **kwargs)
        data_path = artifact.dataset.data_path
        pl.read_parquet(data_path).with_columns((pl.col("timestamp") + 1).alias("timestamp")).write_parquet(data_path)
        return artifact

    monkeypatch.setattr(_work, "_write_candidate", change_candidate)

    result = invoke(download_args(config))

    assert error(result)["code"] == "RPC_MISMATCH"
    assert not (tmp_path / "out" / DATASET_ID).exists()


def test_candidate_fingerprint_is_resealed_after_live_rpc_validation(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary, verifier = chains
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out", features=("timestamp",))
    real_rename = _work._rename_no_replace
    monkeypatch.setattr(_work, "_rename_no_replace", Mock(side_effect=KeyboardInterrupt))
    assert error(invoke(download_args(config)))["code"] == "INTERRUPTED"
    monkeypatch.setattr(_work, "_rename_no_replace", real_rename)
    real_revalidate = _download.RpcSource.revalidate

    async def mutate_after_rpc(source: _download.RpcSource, artifact: Any) -> None:
        await real_revalidate(source, artifact)
        with artifact.dataset.data_path.open("ab") as stream:
            stream.write(b"changed")

    monkeypatch.setattr(_download.RpcSource, "revalidate", mutate_after_rpc)

    result = invoke(download_args(config))

    assert error(result)["code"] == "ARTIFACT_INVALID"
    assert not (tmp_path / "out" / DATASET_ID).exists()


def test_publication_syncs_both_rename_parents_then_root_after_cleanup(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary, verifier = chains
    output_root = (tmp_path / "out").resolve()
    hidden = output_root / f".blockweaver-{DATASET_ID}"
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=output_root)
    synced: list[Path] = []
    final_files: list[str] = []
    real_directory_sync = _work.fsync_directory
    real_file_sync = _work._sync_final_file

    def sync_directory(path: Path) -> None:
        synced.append(path.resolve())
        real_directory_sync(path)

    def sync_file(path: Path) -> None:
        final_files.append(path.name)
        real_file_sync(path)

    monkeypatch.setattr(_work, "fsync_directory", sync_directory)
    monkeypatch.setattr(_work, "_sync_final_file", sync_file)

    artifact_from(invoke(download_args(config)))

    assert synced[-3:] == [output_root, hidden, output_root]
    assert set(final_files) == {"manifest.json", "blocks.parquet"}


def test_unavailable_atomic_publication_keeps_staged_candidate(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary, verifier = chains
    output_root = tmp_path / "out"
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=output_root)
    monkeypatch.setattr(
        _work,
        "_rename_no_replace",
        Mock(side_effect=OSError(errno.ENOTSUP, "Atomic no-replace publication is unavailable")),
    )

    result = invoke(download_args(config))

    assert error(result)["code"] == "IO_FAILED"
    assert not (output_root / DATASET_ID).exists()
    assert (output_root / f".blockweaver-{DATASET_ID}" / "ready").is_dir()
