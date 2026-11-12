from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import polars as pl
import pytest
from support import (
    BQ_DATASET,
    DATASET_ID,
    ROUNDING_EDGE_BLOCK,
    ChainServer,
    FakeBigQuery,
    artifact_from,
    bigquery_config,
    block_hash,
    download_args,
    error,
    invoke,
)

from blockweaver import _corpus, _sources, open_dataset

P50 = "effective_priority_fee_per_gas_p50"
P90 = "effective_priority_fee_per_gas_p90"


def test_bigquery_only_configuration_needs_no_unused_primary(
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
        source="bigquery",
        dataset=BQ_DATASET,
        include_primary=False,
    )
    warehouse = FakeBigQuery(verifier)
    monkeypatch.setattr(_sources, "open_bigquery", lambda _project: warehouse)
    rejected = error(invoke([*download_args(config), "--provider", "verifier"]))
    assert rejected["code"] == "SOURCE_OPTION_INVALID"
    assert verifier.requests == [] and warehouse.calls == []
    artifact_from(invoke(download_args(config)))
    assert verifier.requests and primary.requests == []


def test_bigquery_rejects_invalid_dataset_and_missing_optional_dependency(
    tmp_path: Path, chains: tuple[ChainServer, ChainServer], make_config: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    primary, verifier = chains
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out", source="bigquery", dataset="bad.dataset;drop")
    assert error(invoke(["chains", "--config", str(config)]))["code"] == "CONFIG_INVALID"
    config = bigquery_config(make_config, tmp_path / "valid.toml", primary, verifier, tmp_path / "out")
    monkeypatch.setattr(_sources, "import_module", Mock(side_effect=ModuleNotFoundError))
    assert error(invoke(download_args(config)))["code"] == "SOURCE_DEPENDENCY_MISSING"
    assert primary.requests == verifier.requests == []


def test_bigquery_schema_and_cost_fail_before_billable_query(
    tmp_path: Path, chains: tuple[ChainServer, ChainServer], make_config: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    primary, verifier = chains
    config = bigquery_config(make_config, tmp_path / "config.toml", primary, verifier, tmp_path / "out", features=(P50,))
    unavailable = FakeBigQuery(verifier)
    unavailable.execute("ALTER TABLE {receipts} DROP COLUMN effective_gas_price")
    monkeypatch.setattr(_sources, "open_bigquery", lambda _project: unavailable)
    assert error(invoke(download_args(config)))["code"] == "SOURCE_FEATURE_UNAVAILABLE"
    assert "dry_run" not in unavailable.calls and "execute" not in unavailable.calls

    repeated = FakeBigQuery(verifier)
    repeated.execute("ALTER TABLE {receipts} ALTER effective_gas_price TYPE BIGINT[] USING [effective_gas_price]")
    monkeypatch.setattr(_sources, "open_bigquery", lambda _project: repeated)
    assert error(invoke(download_args(config, dataset_id="33333333-3333-4333-8333-333333333333")))["code"] == "SOURCE_FEATURE_UNAVAILABLE"
    assert "dry_run" not in repeated.calls and "execute" not in repeated.calls

    expensive = FakeBigQuery(verifier, bytes_processed=1001)
    monkeypatch.setattr(_sources, "open_bigquery", lambda _project: expensive)
    assert error(invoke(download_args(config, dataset_id="22222222-2222-4222-8222-222222222222")))["code"] == "BIGQUERY_COST_LIMIT"
    assert expensive.calls[-1] == "dry_run" and "execute" not in expensive.calls


def test_bigquery_header_only_plan_reads_only_required_block_fields(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary, verifier = chains
    config = bigquery_config(make_config, tmp_path / "config.toml", primary, verifier, tmp_path / "out", features=("timestamp",))
    warehouse = FakeBigQuery(verifier)
    warehouse.execute("DROP TABLE {transactions}")
    warehouse.execute("DROP TABLE {receipts}")
    for column in ("base_fee_per_gas", "gas_used", "gas_limit"):
        warehouse.execute(f"ALTER TABLE {{blocks}} DROP COLUMN {column}")
    monkeypatch.setattr(_sources, "open_bigquery", lambda _project: warehouse)
    artifact = artifact_from(invoke(download_args(config)))
    assert warehouse.calls == [f"schema:{BQ_DATASET}.blocks", "dry_run", "execute"]
    monkeypatch.setattr(_sources, "import_module", Mock(side_effect=AssertionError("local loading imported Google")))
    assert open_dataset(artifact).schema == ("block_number", "timestamp")


def test_bigquery_streams_selected_fields_and_matches_rpc_artifacts(
    tmp_path: Path, chains: tuple[ChainServer, ChainServer], make_config: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    primary, verifier = chains
    features = ("timestamp", "block_hash", "gas_used", "tx_count", P50, P90)
    config = bigquery_config(make_config, tmp_path / "config.toml", primary, verifier, tmp_path / "out", features=features)
    clients = [FakeBigQuery(verifier), FakeBigQuery(verifier)]
    queued_clients = iter(clients)
    monkeypatch.setattr(_sources, "open_bigquery", lambda _project: next(queued_clients))
    monkeypatch.setattr(_sources, "CHUNK_SIZE", 2)
    start = datetime.fromtimestamp(verifier.timestamp_base + 10, UTC).isoformat().replace("+00:00", "Z")
    end = datetime.fromtimestamp(verifier.timestamp_base + 14, UTC).isoformat().replace("+00:00", "Z")
    base = ["download", "--config", str(config), "--from-time", start, "--to-time", end]
    parquet = artifact_from(invoke([*base, "--id", DATASET_ID]))
    csv = artifact_from(invoke([*base, "--id", "22222222-2222-4222-8222-222222222222", "--format", "csv"]))
    frame = pl.read_parquet(parquet / "blocks.parquet")
    assert frame.equals(pl.read_csv(csv / "blocks.csv"))
    assert frame["gas_used"].to_list() == [0, 21_000, 42_000, 101_000, 100_000]
    assert frame["tx_count"].to_list() == [0, 1, 2, 3, 3]
    assert frame[P50].to_list() == [0, 11, 12, 18, 4]
    assert frame[P90].to_list() == [0, 11, 19, 22, 14]
    manifest = json.loads((parquet / "manifest.json").read_text())
    assert manifest["source"] == {"type": "bigquery", "dataset": BQ_DATASET, "verifier": "verifier"}
    assert manifest["verification"]["sampled_blocks"][0 :: len(manifest["verification"]["sampled_blocks"]) - 1] == [10, 14]
    assert all(client.page_size == 2 and client.calls[-1] == "execute" for client in clients)
    assert primary.requests == [] and verifier.requests
    rpc_config = make_config(tmp_path / "rpc.toml", primary, verifier, output_root=tmp_path / "rpc", features=features)
    rpc_artifact = artifact_from(invoke(download_args(rpc_config, dataset_id="33333333-3333-4333-8333-333333333333")))
    assert frame.equals(pl.read_parquet(rpc_artifact / "blocks.parquet"))


def test_bigquery_and_rpc_agree_on_every_feature_and_block_shape(
    tmp_path: Path, chains: tuple[ChainServer, ChainServer], make_config: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    primary, verifier = chains
    features = ("timestamp", "block_hash", "parent_hash", "base_fee_per_gas", "gas_used", "gas_limit", "tx_count", P50, P90)
    monkeypatch.setattr(_sources, "open_bigquery", lambda _project: FakeBigQuery(verifier))
    warehouse = bigquery_config(make_config, tmp_path / "bigquery.toml", primary, verifier, tmp_path / "bigquery", features=features)
    rpc = make_config(tmp_path / "rpc.toml", primary, verifier, output_root=tmp_path / "rpc", features=features)
    last = ROUNDING_EDGE_BLOCK - 1
    from_warehouse = pl.read_parquet(artifact_from(invoke(download_args(warehouse, first=0, last=last))) / "blocks.parquet")
    from_rpc = pl.read_parquet(artifact_from(invoke(download_args(rpc, first=0, last=last))) / "blocks.parquet")
    assert from_warehouse.equals(from_rpc)
    assert from_warehouse["tx_count"].to_list() == [0, 1, 2, 3, 3] * 3 + [0, 1]


@pytest.mark.xfail(strict=True, reason="the query rounds the gas threshold up, the execution client rounds it down")
def test_bigquery_percentile_floors_the_gas_threshold(
    tmp_path: Path, chains: tuple[ChainServer, ChainServer], make_config: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    primary, verifier = chains
    config = bigquery_config(make_config, tmp_path / "config.toml", primary, verifier, tmp_path / "out", features=(P50,))
    monkeypatch.setattr(_sources, "open_bigquery", lambda _project: FakeBigQuery(verifier))
    artifact = artifact_from(invoke(download_args(config, first=ROUNDING_EDGE_BLOCK, last=ROUNDING_EDGE_BLOCK)))
    assert pl.read_parquet(artifact / "blocks.parquet")[P50].to_list() == [1]


def test_bigquery_checks_the_gas_domain_of_an_unselected_field(
    tmp_path: Path, chains: tuple[ChainServer, ChainServer], make_config: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    primary, verifier = chains
    config = bigquery_config(make_config, tmp_path / "config.toml", primary, verifier, tmp_path / "out", features=("gas_used",))
    warehouse = FakeBigQuery(verifier)
    warehouse.execute("UPDATE {blocks} SET gas_limit = 20000 WHERE block_number = 12")
    monkeypatch.setattr(_sources, "open_bigquery", lambda _project: warehouse)
    failure = error(invoke(download_args(config)))
    assert failure["code"] == "BIGQUERY_INVALID" and "Block 12 has gas_used outside its domain" in failure["message"]


def test_bigquery_rejects_a_block_with_a_missing_receipt(
    tmp_path: Path, chains: tuple[ChainServer, ChainServer], make_config: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    primary, verifier = chains
    config = bigquery_config(make_config, tmp_path / "config.toml", primary, verifier, tmp_path / "out", features=(P50,))
    warehouse = FakeBigQuery(verifier)
    warehouse.execute(f"DELETE FROM {{receipts}} WHERE block_hash = '{block_hash(13)}' AND transaction_index = 1")
    monkeypatch.setattr(_sources, "open_bigquery", lambda _project: warehouse)
    failure = error(invoke(download_args(config)))
    assert failure["code"] == "BIGQUERY_INVALID" and "block 13" in failure["message"]


@pytest.mark.parametrize("missing", [12, 14])
def test_bigquery_rejects_a_result_that_does_not_cover_the_range(
    tmp_path: Path, chains: tuple[ChainServer, ChainServer], make_config: Any, monkeypatch: pytest.MonkeyPatch, missing: int
) -> None:
    primary, verifier = chains
    config = bigquery_config(make_config, tmp_path / "config.toml", primary, verifier, tmp_path / "out", features=("timestamp",))
    verifier.changes[14] = {"timestamp": hex(verifier.timestamp_base + 13)}
    warehouse = FakeBigQuery(verifier)
    warehouse.execute(f"DELETE FROM {{blocks}} WHERE block_number = {missing}")
    monkeypatch.setattr(_sources, "open_bigquery", lambda _project: warehouse)
    monkeypatch.setattr(_sources, "CHUNK_SIZE", 2)
    assert error(invoke(download_args(config)))["code"] == "BIGQUERY_INVALID"
    assert not (tmp_path / "out" / DATASET_ID).exists()


def test_bigquery_recovery_requires_exact_binding(
    tmp_path: Path, chains: tuple[ChainServer, ChainServer], make_config: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    primary, verifier = chains
    config = bigquery_config(make_config, tmp_path / "config.toml", primary, verifier, tmp_path / "out")
    clients = iter(FakeBigQuery(primary) for _ in range(3))
    monkeypatch.setattr(_sources, "open_bigquery", lambda _project: next(clients))
    real_rename = _corpus._rename_no_replace

    monkeypatch.setattr(_corpus, "_rename_no_replace", Mock(side_effect=KeyboardInterrupt))
    assert error(invoke(download_args(config)))["code"] == "INTERRUPTED"
    assert error(invoke([*download_args(config), "--format", "csv"]))["code"] == "RESUME_MISMATCH"
    monkeypatch.setattr(_corpus, "_rename_no_replace", real_rename)
    resumed = invoke(download_args(config))
    assert Path(json.loads(resumed.stdout)["path"]).is_dir()
    assert json.loads(resumed.stderr.splitlines()[-1])["recovered"] is True


@pytest.mark.parametrize("disagreement", ["target", "sample", "ancestry", "finality"])
def test_bigquery_rpc_disagreement_prevents_publication(
    tmp_path: Path, chains: tuple[ChainServer, ChainServer], make_config: Any, monkeypatch: pytest.MonkeyPatch, disagreement: str
) -> None:
    primary, verifier = chains
    config = bigquery_config(make_config, tmp_path / "config.toml", primary, verifier, tmp_path / "out", features=("base_fee_per_gas",))
    changes = {"target": (14, {"hash": block_hash(99)}), "sample": (12, {"baseFeePerGas": hex(99)}), "ancestry": (16, {"parentHash": block_hash(1)})}
    if disagreement == "finality":
        verifier.tag_changes["hash"] = block_hash(99)
    else:
        number, change = changes[disagreement]
        verifier.changes[number] = change
    monkeypatch.setattr(_sources, "open_bigquery", lambda _project: FakeBigQuery(primary))

    assert error(invoke(download_args(config)))["code"] == "RPC_MISMATCH"
    assert not any(path.name.endswith(DATASET_ID) and not path.name.startswith(".") for path in (tmp_path / "out").glob("*"))
