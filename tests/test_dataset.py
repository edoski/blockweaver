from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from support import ChainServer, artifact_from, block_hash, download_args, error, invoke

from blockweaver import BlockweaverError, _work, open_dataset


@pytest.mark.parametrize(
    "corruption",
    ["json", "uuid_path", "extra", "digest", "schema", "range", "domain", "target", "verification"],
)
def test_open_dataset_rejects_corrupt_artifact_boundaries(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
    corruption: str,
) -> None:
    primary, verifier = chains
    config = make_config(
        tmp_path / "config.toml",
        primary,
        verifier,
        output_root=tmp_path / "out",
        features=("timestamp", "block_hash", "gas_used", "gas_limit"),
    )
    artifact = artifact_from(invoke(download_args(config)))
    manifest_path = artifact / "manifest.json"
    data_path = artifact / "blocks.parquet"
    manifest = json.loads(manifest_path.read_text())
    if corruption == "json":
        manifest_path.write_text("{broken\n")
    elif corruption == "uuid_path":
        moved = artifact.with_name("22222222-2222-4222-8222-222222222222")
        artifact.rename(moved)
        artifact = moved
    elif corruption == "extra":
        (artifact / "extra").write_text("unexpected")
    elif corruption == "digest":
        with data_path.open("ab") as stream:
            stream.write(b"changed")
    else:
        if corruption == "schema":
            manifest["schema"][1]["unit"] = "wrong"
        elif corruption == "range":
            manifest["resolved_range"]["to_block"] += 1
        elif corruption == "domain":
            frame = pl.read_parquet(data_path).with_columns((pl.col("gas_limit") + 1).alias("gas_used"))
            frame.write_parquet(data_path)
            manifest["output"]["bytes"] = data_path.stat().st_size
            manifest["output"]["sha256"] = hashlib.sha256(data_path.read_bytes()).hexdigest()
        elif corruption == "target":
            manifest["target_hash"] = block_hash(999)
        else:
            manifest["verification"]["target_agreement"] = False
        manifest_path.write_text(json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n")

    with pytest.raises(BlockweaverError) as failure:
        open_dataset(artifact)
    assert failure.value.code == "ARTIFACT_INVALID"


def test_open_dataset_binds_data_bytes_that_still_parse(tmp_path: Path, chains: tuple[ChainServer, ChainServer], make_config: Any) -> None:
    primary, verifier = chains
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out", features=("block_hash",), output_format="csv")
    artifact = artifact_from(invoke(download_args(config)))
    data_path = artifact / "blocks.csv"
    data_path.write_text(data_path.read_text().replace(block_hash(12), block_hash(999)))

    with pytest.raises(BlockweaverError) as failure:
        open_dataset(artifact)
    assert failure.value.code == "ARTIFACT_INVALID" and "digest" in str(failure.value)


def test_manifest_requires_canonical_json_and_completion_follows_data_assembly(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary, verifier = chains
    output_root = tmp_path / "out"
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=output_root)
    observed = False

    def completion_time() -> str:
        nonlocal observed
        observed = any(output_root.glob(".blockweaver-*/ready.tmp/blocks.parquet"))
        return "2026-08-10T12:00:00Z"

    monkeypatch.setattr(_work, "_completion_time", completion_time)
    artifact = artifact_from(invoke(download_args(config)))
    assert observed
    manifest_path = artifact / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    assert manifest["completed_at"] == "2026-08-10T12:00:00Z"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    assert error(invoke(["verify", str(artifact)]))["code"] == "ARTIFACT_INVALID"
