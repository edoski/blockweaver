from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from conftest import ChainServer, block_hash
from typer.testing import CliRunner

from blockweaver import _build
from blockweaver._contract import parse_time
from blockweaver.cli import app

DATASET_ID = "11111111-1111-4111-8111-111111111111"


def invoke(arguments: list[str], env: dict[str, str] | None = None):
    return CliRunner().invoke(app, arguments, env=env)


def download_args(config: Path, *, first: int = 10, last: int = 14, dataset_id: str = DATASET_ID) -> list[str]:
    return [
        "download",
        "--config",
        str(config),
        "--id",
        dataset_id,
        "--from-block",
        str(first),
        "--to-block",
        str(last),
    ]


def artifact_from(result: Any) -> Path:
    assert result.exit_code == 0, result.output
    return Path(json.loads(result.stdout)["path"])


def error(result: Any) -> dict[str, Any]:
    assert result.exit_code != 0
    assert result.stdout == ""
    lines = result.stderr.splitlines()
    return json.loads(lines[-1])


def test_cli_commands_and_machine_usage_errors() -> None:
    help_result = invoke(["--help"])
    assert help_result.exit_code == 0
    assert all(command in help_result.stdout for command in ("init", "chains", "features", "download", "verify"))

    failure = error(invoke(["download"]))
    assert failure["event"] == "error"
    assert failure["code"] == "CONFIG_NOT_FOUND"


def test_init_precedence_permissions_and_no_overwrite(tmp_path: Path) -> None:
    configured = tmp_path / "configured.toml"
    explicit = tmp_path / "explicit.toml"

    result = invoke(["init"], {"BLOCKWEAVER_CONFIG": str(configured)})
    assert json.loads(result.stdout)["path"] == str(configured)
    assert configured.stat().st_mode & 0o777 == 0o600
    assert "url_env" in configured.read_text()

    explicit_result = invoke(["init", "--config", str(explicit)], {"BLOCKWEAVER_CONFIG": str(configured)})
    assert json.loads(explicit_result.stdout)["path"] == str(explicit)
    assert error(invoke(["init", "--config", str(explicit)]))["code"] == "CONFIG_EXISTS"


def test_strict_config_discovery_has_no_secrets(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
) -> None:
    primary, verifier = chains
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out")

    chain_result = invoke(["chains", "--config", str(config)])
    feature_result = invoke(["features", "--config", str(config)])
    assert json.loads(chain_result.stdout)["chains"] == [
        {
            "chain_id": 1,
            "default": True,
            "finality_tag": "finalized",
            "name": "test",
            "provider": "primary",
            "source_support": ["rpc"],
            "verifier": "verifier",
        }
    ]
    catalog = json.loads(feature_result.stdout)
    assert catalog["mandatory"]["name"] == "block_number"
    assert {item["name"] for item in catalog["features"]} >= {"timestamp", "block_hash", "effective_priority_fee_per_gas_p90"}
    assert primary.url not in chain_result.output + feature_result.output
    assert verifier.url not in chain_result.output + feature_result.output

    with config.open("a") as stream:
        stream.write("\nunknown = true\n")
    assert error(invoke(["chains", "--config", str(config)]))["code"] == "CONFIG_INVALID"


def test_download_cli_values_override_defaults_and_profiles(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
) -> None:
    primary, verifier = chains
    configured_root = tmp_path / "configured"
    override_root = tmp_path / "override"
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=configured_root, features=("timestamp",))
    with config.open("a") as stream:
        stream.write(f'''\n[providers.override]\nurl = "{primary.url}"\nbatch_size = 1\nconcurrency = 1\ntimeout = 1\n''')

    artifact = artifact_from(
        invoke(
            [
                *download_args(config),
                "--provider",
                "override",
                "--output-root",
                str(override_root),
                "--format",
                "csv",
                "--feature",
                "block_hash",
                "--batch-size",
                "2",
            ]
        )
    )
    manifest = json.loads((artifact / "manifest.json").read_text())
    assert artifact.parent == override_root
    assert manifest["source"]["provider"] == "override"
    assert manifest["output"]["format"] == "csv"
    assert [column["name"] for column in manifest["schema"]] == ["block_number", "block_hash"]
    assert not configured_root.exists()


def test_parquet_download_selects_and_coalesces_features(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
) -> None:
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
    assert artifact.name == f"test-20231114T221330Z-{DATASET_ID}"
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
    assert frame["effective_priority_fee_per_gas_p50"].to_list() == [500, 550, 600, 650, 700]

    fee_calls = [call for batch in primary.requests for call in batch if call["method"] == "eth_feeHistory"]
    assert [call["params"] for call in fee_calls] == [["0x5", "0xe", [50]]]
    block_calls = [call for batch in primary.requests for call in batch if call["method"] == "eth_getBlockByNumber"]
    acquired = [call for call in block_calls if call["params"][0] in {hex(number) for number in range(10, 15)}]
    assert all(call["params"][1] is False for call in acquired)

    manifest_text = (artifact / "manifest.json").read_text()
    manifest = json.loads(manifest_text)
    assert manifest_text.endswith("\n")
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
    assert all(json.loads(line)["event"] for line in result.stderr.splitlines())


def test_time_range_csv_and_reduced_precision(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
) -> None:
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
    monkeypatch.setattr(_build, "_CHUNK_SIZE", 2)
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
    assert primary.request_counts[10] == 4
    assert primary.request_counts[11] == 1


def test_verifier_disagreement_and_secret_redaction(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
) -> None:
    primary, verifier = chains
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out")
    verifier.changes[14] = {"hash": block_hash(999)}
    failure = invoke(download_args(config))
    message = error(failure)
    assert message["code"] == "RPC_MISMATCH"
    assert primary.url not in failure.output and verifier.url not in failure.output
    assert not any(path.name.endswith(DATASET_ID) and not path.name.startswith(".") for path in (tmp_path / "out").glob("*"))


def test_publication_failure_recovers_ready_candidate(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary, verifier = chains
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out")
    real_publish = _build.publish

    def interrupt(_hidden: Path, _destination: Path) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(_build, "publish", interrupt)
    assert error(invoke(download_args(config)))["code"] == "INTERRUPTED"
    hidden = tmp_path / "out" / f".blockweaver-{DATASET_ID}"
    assert {path.name for path in (hidden / "ready").iterdir()} == {"manifest.json", "blocks.parquet"}

    requests = len(primary.requests) + len(verifier.requests)
    monkeypatch.setattr(_build, "publish", real_publish)
    artifact = artifact_from(invoke(download_args(config)))
    assert len(primary.requests) + len(verifier.requests) > requests  # range and identity are re-resolved
    assert not hidden.exists()
    assert error(invoke(download_args(config)))["code"] == "DESTINATION_EXISTS"
    assert artifact.exists()


def test_verify_is_strict_locally_and_against_rpc(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
) -> None:
    primary, verifier = chains
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out")
    artifact = artifact_from(invoke(download_args(config)))
    full = invoke(["verify", str(artifact), "--config", str(config), "--provider", "verifier", "--full-rpc"])
    assert json.loads(full.stdout)["verification"]["mode"] == "full_rpc"

    frame = pl.read_parquet(artifact / "blocks.parquet")
    frame.with_columns(pl.when(pl.col("block_number") == 12).then(999).otherwise(pl.col("timestamp")).alias("timestamp")).write_parquet(
        artifact / "blocks.parquet"
    )
    assert error(invoke(["verify", str(artifact)]))["code"] == "ARTIFACT_INVALID"


def test_rpc_protocol_failure_is_bounded_and_machine_readable(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
) -> None:
    primary, verifier = chains
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out")
    primary.wrong_id_once = True
    failure = error(invoke(download_args(config)))
    assert failure["code"] == "RPC_INVALID"
    assert "response ID mismatch" in failure["message"]
