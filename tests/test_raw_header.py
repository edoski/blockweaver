from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from support import DATASET_ID, ChainServer, FakeBigQuery, artifact_from, bigquery_config, block_hash, download_args, error, invoke

from blockweaver import _download, open_dataset

RAW = ["--feature", "timestamp", "--feature", "header.mixHash", "--feature", "header.extraData"]


@pytest.mark.parametrize("output_format", ["parquet", "csv"])
def test_raw_header_keys_are_downloaded_as_the_node_returned_them(
    tmp_path: Path, chains: tuple[ChainServer, ChainServer], make_config: Any, output_format: str
) -> None:
    primary, verifier = chains
    primary.keys_from["settledExcess"] = verifier.keys_from["settledExcess"] = 0
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out", output_format=output_format)
    artifact = artifact_from(invoke([*download_args(config), *RAW, "--feature", "header.settledExcess"]))

    dataset = open_dataset(artifact)
    assert dataset.schema == ("block_number", "timestamp", "header.extraData", "header.mixHash", "header.settledExcess")
    frame = pl.read_parquet(dataset.data_path) if output_format == "parquet" else pl.read_csv(dataset.data_path)
    assert frame["header.extraData"].to_list() == ["0x66616b65", "0x66616b6566616b65", "0x", "0x66616b65", "0x66616b6566616b65"]
    assert frame["header.mixHash"].to_list() == [block_hash(number + 7) for number in range(10, 15)]
    assert frame["header.settledExcess"].to_list() == [hex(number * 2**70) for number in range(10, 15)], "values wider than Int64 stay hex strings"
    manifest = json.loads((artifact / "manifest.json").read_text())
    assert manifest["schema"][2:] == [
        {"name": "header.extraData", "type": "UTF-8", "unit": "hex"},
        {"name": "header.mixHash", "type": "UTF-8", "unit": "hex"},
        {"name": "header.settledExcess", "type": "UTF-8", "unit": "hex"},
    ]
    assert manifest["acquisition_plan"]["families"] == [
        {"family": "header", "method": "eth_getBlockByNumber", "fields": ["number", "hash", "parentHash", "timestamp", "extraData", "mixHash", "settledExcess"]}
    ]
    assert invoke(["verify", str(artifact), "--rpc-url", verifier.url, "--full-rpc"]).exit_code == 0


def test_a_key_missing_in_part_of_the_range_fails_at_the_first_block_without_it(
    tmp_path: Path, chains: tuple[ChainServer, ChainServer], make_config: Any
) -> None:
    primary, verifier = chains
    primary.keys_from["settledExcess"] = verifier.keys_from["settledExcess"] = 13
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out")

    failure = error(invoke([*download_args(config), "--feature", "header.settledExcess"]))
    assert failure["code"] == "SOURCE_FEATURE_UNAVAILABLE"
    assert "block 10 has no header key settledExcess" in failure["message"]

    artifact = artifact_from(invoke([*download_args(config, first=13, dataset_id="22222222-2222-4222-8222-222222222222"), "--feature", "header.settledExcess"]))
    assert open_dataset(artifact).row_count == 2


@pytest.mark.parametrize(
    ("feature", "change"),
    [
        ("header.transactions", {}),
        ("header.extraData", {"extraData": "0xAB"}),
        ("header.extraData", {"extraData": "ab"}),
        ("header.extraData", {"extraData": None}),
    ],
)
def test_raw_header_values_must_be_lowercase_hex_strings(
    tmp_path: Path, chains: tuple[ChainServer, ChainServer], make_config: Any, feature: str, change: dict[str, Any]
) -> None:
    primary, verifier = chains
    primary.changes[12] = change
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out")
    failure = error(invoke([*download_args(config), "--feature", feature]))
    assert failure["code"] == "RPC_INVALID" and f"has {feature} outside its domain" in failure["message"]


@pytest.mark.parametrize("name", ["header.", "header.9lives", "header.extra-data", "header.extraData.more", "Header.extraData", "header." + "a" * 65])
def test_malformed_raw_header_names_are_rejected_before_network(tmp_path: Path, chains: tuple[ChainServer, ChainServer], make_config: Any, name: str) -> None:
    primary, verifier = chains
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out")
    assert error(invoke([*download_args(config), "--feature", name]))["code"] == "FEATURE_INVALID"
    assert primary.requests == verifier.requests == []


def test_raw_header_keys_are_refused_for_bigquery_before_any_call(
    tmp_path: Path, chains: tuple[ChainServer, ChainServer], make_config: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    primary, verifier = chains
    warehouse = FakeBigQuery(verifier)
    monkeypatch.setattr(_download, "open_bigquery", lambda _project: warehouse)
    config = bigquery_config(make_config, tmp_path / "config.toml", primary, verifier, tmp_path / "out")
    failure = error(invoke([*download_args(config), *RAW]))
    assert failure["code"] == "SOURCE_FEATURE_UNAVAILABLE" and "header.extraData" in failure["message"]
    assert verifier.requests == [] and warehouse.calls == []


def test_raw_header_values_are_checked_against_the_verifier(tmp_path: Path, chains: tuple[ChainServer, ChainServer], make_config: Any) -> None:
    primary, verifier = chains
    verifier.changes[10] = {"extraData": "0xdead"}
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out")
    assert error(invoke([*download_args(config), *RAW]))["code"] == "RPC_MISMATCH"
    assert not (tmp_path / "out" / DATASET_ID).exists()


def test_features_lists_the_raw_header_pattern_and_per_feature_sources(tmp_path: Path, chains: tuple[ChainServer, ChainServer], make_config: Any) -> None:
    primary, verifier = chains
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out")
    catalog = json.loads(invoke(["features", "--config", str(config)]).stdout)
    assert catalog["patterns"] == [
        {
            "pattern": "header.<key>",
            "type": "UTF-8",
            "unit": "hex",
            "supported_sources": ["rpc"],
            "acquisition_families": {"rpc": "header"},
            "domain_rule": "lowercase 0x-prefixed hex string",
        }
    ]
    assert all(item["supported_sources"] == ["rpc", "bigquery"] for item in catalog["features"])
