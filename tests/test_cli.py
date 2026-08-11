from __future__ import annotations

import asyncio
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
from uuid import UUID

import polars as pl
import pytest
from conftest import ChainServer, FakeBigQuery, block_hash
from typer.testing import CliRunner

from blockweaver import BlockweaverError, Dataset, _corpus, _sources, open_dataset
from blockweaver import cli as cli_module
from blockweaver._contract import Chain, Provider, RpcDownloadRequest, parse_time, plan_features, requested_range
from blockweaver.cli import app

DATASET_ID = "11111111-1111-4111-8111-111111111111"
BQ_DATASET = "bigquery-public-data.goog_blockchain_test_us"


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


def bigquery_config(make_config: Any, path: Path, primary: ChainServer, verifier: ChainServer, output_root: Path, **values: Any) -> Path:
    return make_config(path, primary, verifier, output_root=output_root, source="bigquery", dataset=BQ_DATASET, **values)


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
    assert json.loads(result.stdout) == {"operation": "init", "path": str(configured)}
    assert configured.stat().st_mode & 0o777 == 0o600
    assert "url_env" in configured.read_text()
    generated = invoke(["chains", "--config", str(configured)])
    assert generated.exit_code == 0 and json.loads(generated.stdout)["chains"][0]["name"] == "local"
    explicit_result = invoke(["init", "--config", str(explicit)], {"BLOCKWEAVER_CONFIG": str(configured)})
    assert json.loads(explicit_result.stdout) == {"operation": "init", "path": str(explicit)}
    assert error(invoke(["init", "--config", str(explicit)]))["code"] == "CONFIG_EXISTS"


def test_strict_config_discovery_has_no_secrets(tmp_path: Path, chains: tuple[ChainServer, ChainServer], make_config: Any) -> None:
    primary, verifier = chains
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out")
    chain_result = invoke(["chains", "--config", str(config)])
    feature_result = invoke(["features", "--config", str(config)])
    assert json.loads(chain_result.stdout) == {
        "chains": [
            {
                "chain_id": 1,
                "default": True,
                "finality_tag": "finalized",
                "name": "test",
                "provider": "primary",
                "available_sources": ["rpc"],
                "verifier": "verifier",
            }
        ]
    }
    catalog = json.loads(feature_result.stdout)
    assert set(catalog) == {"chain", "available_sources", "mandatory", "features"}
    assert catalog["available_sources"] == ["rpc"]
    assert catalog["mandatory"]["name"] == "block_number"
    assert {item["name"] for item in catalog["features"]} >= {"timestamp", "block_hash", "effective_priority_fee_per_gas_p90"}
    assert all(item["supported_sources"] == ["rpc", "bigquery"] for item in catalog["features"])
    assert all(
        set(item) == {"name", "type", "unit", "supported_sources", "acquisition_families", "domain_rule", "hidden_dependencies"} for item in catalog["features"]
    )
    assert primary.url not in chain_result.output + feature_result.output
    assert verifier.url not in chain_result.output + feature_result.output
    with config.open("a") as stream:
        stream.write("\nunknown = true\n")
    assert error(invoke(["chains", "--config", str(config)]))["code"] == "CONFIG_INVALID"


@pytest.mark.parametrize(
    "replacement",
    [
        'url = "http://host:abc"',
        'url = "http://host:70000"',
        'url = "http://bad host"',
        'url = "http://user:password@"',
        'url = "http://-bad.example"',
        "timeout = nan",
        "timeout = inf",
        "timeout = 3601",
    ],
)
def test_config_rejects_malformed_urls_and_unbounded_timeouts(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
    replacement: str,
) -> None:
    primary, verifier = chains
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out")
    text = config.read_text()
    text = text.replace(f'url = "{primary.url}"', replacement, 1) if replacement.startswith("url") else text.replace("timeout = 2", replacement, 1)
    config.write_text(text)
    assert error(invoke(["chains", "--config", str(config)]))["code"] == "CONFIG_INVALID"


def test_unknown_default_source_is_a_configuration_failure(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
) -> None:
    primary, verifier = chains
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out", source="unknown")

    failure = error(invoke(["chains", "--config", str(config)]))

    assert failure["code"] == "CONFIG_INVALID"
    assert primary.requests == verifier.requests == []


def test_download_cli_values_override_defaults_and_profiles(tmp_path: Path, chains: tuple[ChainServer, ChainServer], make_config: Any) -> None:
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


def test_request_resolution_rejects_missing_environment_and_dependent_providers_before_network(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
) -> None:
    primary, verifier = chains
    config = make_config(tmp_path / "rpc.toml", primary, verifier, output_root=tmp_path / "out")
    config.write_text(config.read_text().replace(f'url = "{primary.url}"', 'url_env = "BLOCKWEAVER_TEST_MISSING"', 1))
    assert error(invoke(download_args(config)))["code"] == "CONFIG_ENV_MISSING"
    assert error(invoke([*download_args(config), "--feature", "unknown"]))["code"] == "FEATURE_INVALID"
    assert primary.requests == verifier.requests == []

    independent = make_config(tmp_path / "independent.toml", primary, verifier, output_root=tmp_path / "out")
    assert error(invoke([*download_args(independent), "--provider", "verifier"]))["code"] == "PROVIDER_INVALID"
    assert primary.requests == verifier.requests == []

    warehouse = make_config(
        tmp_path / "warehouse.toml",
        primary,
        verifier,
        output_root=tmp_path / "out",
        source="bigquery",
        dataset=BQ_DATASET,
        include_primary=False,
    )
    warehouse.write_text(warehouse.read_text().replace('project = "billing-project"', 'project_env = "BLOCKWEAVER_BQ_MISSING"'))
    assert error(invoke(download_args(warehouse)))["code"] == "CONFIG_ENV_MISSING"
    assert primary.requests == verifier.requests == []


def test_basic_auth_rpc_urls_are_accepted_and_fully_redacted(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary, verifier = chains
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out")
    basic_url = primary.url.replace("http://", "http://user:secret@")
    config.write_text(config.read_text().replace(primary.url, basic_url))
    successful = invoke(download_args(config))
    assert successful.exit_code == 0, successful.output
    assert basic_url not in successful.output

    async def nested_failure(*_args: object, **_kwargs: object) -> dict[str, object]:
        try:
            raise RuntimeError(f"transport failed at {basic_url}")
        except RuntimeError as error:
            raise RuntimeError(f"nested failure: {error}") from error

    monkeypatch.setattr(cli_module, "download_dataset", nested_failure)
    failed = invoke(download_args(config, dataset_id="22222222-2222-4222-8222-222222222222"))
    message = error(failed)
    assert message["code"] == "INTERNAL_ERROR"
    assert message["message"] == "nested failure: transport failed at <redacted>"
    assert basic_url not in failed.output


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
    assert frame["effective_priority_fee_per_gas_p50"].to_list() == [500, 550, 600, 650, 700]
    fee_calls = [call for batch in primary.requests for call in batch if call["method"] == "eth_feeHistory"]
    assert [call["params"] for call in fee_calls] == [["0x5", "0xe", [50]]]
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
    assert all(json.loads(line)["event"] for line in result.stderr.splitlines())


def test_header_only_tx_count_uses_only_block_cardinality(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
) -> None:
    primary, verifier = chains
    primary.changes[10] = {"transactions": [{"opaque": "transaction"}]}
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out", features=("tx_count",))
    artifact = artifact_from(invoke(download_args(config)))
    assert pl.read_parquet(artifact / "blocks.parquet")["tx_count"].to_list()[0] == 1
    assert {call["method"] for batch in primary.requests for call in batch} == {"eth_chainId", "eth_getBlockByNumber"}


@pytest.mark.parametrize(
    ("code", "retries"),
    [(-32601, False), (-32000, False), (-32603, True), (-32002, True), (-32042, True)],
)
def test_rpc_error_disposition_uses_numeric_codes_without_provider_messages(chains: tuple[ChainServer, ChainServer], code: int, retries: bool) -> None:
    primary, _verifier = chains
    primary.rpc_errors = [code]

    async def request() -> list[_sources.Header]:
        async with _sources.Rpc(primary.url, batch_size=3, concurrency=2, timeout=2) as rpc:
            return await rpc.headers([10], plan_features([]))

    if retries:
        assert [header.block_number for header in asyncio.run(request())] == [10]
        assert len(primary.requests) == 2
    else:
        with pytest.raises(RuntimeError, match="rejected") as failure:
            asyncio.run(request())
        assert "provider detail" not in str(failure.value)
        assert len(primary.requests) == 1


def test_rpc_limit_splits_immediately_and_retries_only_pending_calls(chains: tuple[ChainServer, ChainServer]) -> None:
    primary, _verifier = chains
    primary.limit_batches = True

    async def request() -> list[_sources.Header]:
        async with _sources.Rpc(primary.url, batch_size=3, concurrency=2, timeout=2) as rpc:
            return await rpc.headers([10, 11, 12], plan_features([]))

    assert [header.block_number for header in asyncio.run(request())] == [10, 11, 12]
    batch_sizes = [len(batch) for batch in primary.requests]
    assert batch_sizes[0] == 3 and all(size < 3 for size in batch_sizes[1:])
    primary.requests.clear()
    primary.limit_batches = False
    primary.item_errors[11] = [-32603]
    assert [header.block_number for header in asyncio.run(request())] == [10, 11, 12]
    assert [[call["params"][0] for call in batch] for batch in primary.requests] == [["0xa", "0xb", "0xc"], ["0xb"]]
    primary.requests.clear()
    primary.rpc_errors = [-32005]
    with pytest.raises(RuntimeError, match="limit exceeded"):
        asyncio.run(request())
    assert len(primary.requests) == 1


def test_rpc_retries_preserve_original_pending_order(chains: tuple[ChainServer, ChainServer]) -> None:
    primary, _verifier = chains
    for number in (11, 19, 27):
        primary.item_errors[number] = [-32603, -32603]

    async def request() -> list[_sources.Header]:
        async with _sources.Rpc(primary.url, batch_size=18, concurrency=1, timeout=2) as rpc:
            return await rpc.headers(range(10, 28), plan_features([]))

    assert [header.block_number for header in asyncio.run(request())] == list(range(10, 28))
    assert [[call["params"][0] for call in batch] for batch in primary.requests[1:]] == [
        ["0xb", "0x13", "0x1b"],
        ["0xb", "0x13", "0x1b"],
    ]


def test_rpc_retry_is_bounded_and_retry_after_is_validated(chains: tuple[ChainServer, ChainServer], monkeypatch: pytest.MonkeyPatch) -> None:
    primary, _verifier = chains
    delays: list[float] = []

    async def sleep(delay: float) -> None:
        delays.append(delay)

    async def request() -> None:
        async with _sources.Rpc(primary.url, batch_size=1, concurrency=1, timeout=2) as rpc:
            await rpc.headers([10], plan_features([]))

    monkeypatch.setattr(_sources.asyncio, "sleep", sleep)
    monkeypatch.setattr(_sources.random, "uniform", lambda _start, _end: 0.25)
    primary.http_failures, primary.retry_after = 1, "999"
    asyncio.run(request())
    assert delays == [60.0]
    delays.clear()
    primary.http_failures, primary.retry_after = 1, "-1"
    asyncio.run(request())
    assert delays == [0.25]
    delays.clear()
    primary.rpc_errors = [-32603] * 12
    with pytest.raises(RuntimeError, match="12 attempts") as failure:
        asyncio.run(request())
    assert "provider detail" not in str(failure.value)
    assert len(delays) == 11


def test_rpc_groups_are_concurrent_and_cancel_siblings(chains: tuple[ChainServer, ChainServer], monkeypatch: pytest.MonkeyPatch) -> None:
    primary, _verifier = chains

    async def exercise() -> tuple[int, bool]:
        rpc = _sources.Rpc("http://unused", batch_size=1, concurrency=3, timeout=2)
        active = maximum = started = 0
        release = asyncio.Event()

        async def concurrent(calls: list[dict[str, Any]]) -> tuple[int, Any, float | None]:
            nonlocal active, maximum, started
            active += 1
            started += 1
            maximum = max(maximum, active)
            if started == 3:
                release.set()
            await release.wait()
            call = calls[0]
            number = int(call["params"][0], 16)
            active -= 1
            return 200, [{"jsonrpc": "2.0", "id": call["id"], "result": primary.block(number)}], None

        monkeypatch.setattr(rpc, "_post", concurrent)
        assert [item.block_number for item in await rpc.headers([10, 11, 12], plan_features([]))] == [10, 11, 12]

        cancelled = False
        ready = asyncio.Event()
        waiting = 0

        async def fail_one(calls: list[dict[str, Any]]) -> tuple[int, Any, float | None]:
            nonlocal cancelled, waiting
            waiting += 1
            if waiting == 2:
                ready.set()
            await ready.wait()
            if calls[0]["params"][0] == "0xa":
                raise ValueError("bad sibling")
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled = True
                raise
            raise AssertionError("unreachable")

        monkeypatch.setattr(rpc, "_post", fail_one)
        with pytest.raises(ValueError, match="bad sibling"):
            await rpc.headers([10, 11], plan_features([]))
        return maximum, cancelled

    maximum, cancelled = asyncio.run(exercise())
    assert maximum == 3 and cancelled


def test_paired_provider_failure_cancels_and_awaits_sibling(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary, verifier = chains
    request = RpcDownloadRequest(
        dataset_id=UUID(DATASET_ID),
        chain=Chain("test", 1, "finalized", None, None, None),
        requested_range=requested_range(10, 14, None, None),
        plan=plan_features(["timestamp"]),
        output_root=tmp_path,
        output_format="parquet",
        primary=Provider("primary", primary.url, 3, 2, 2),
        verifier=Provider("verifier", verifier.url, 3, 2, 2),
    )
    ready: asyncio.Event | None = None
    started = 0
    sibling_cancelled = False

    async def chain_id(rpc: _sources.Rpc) -> int:
        nonlocal ready, sibling_cancelled, started
        if ready is None:
            ready = asyncio.Event()
        current = ready
        started += 1
        if started == 2:
            current.set()
        await current.wait()
        if rpc._url == primary.url:
            raise RuntimeError("primary failed")
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            sibling_cancelled = True
            raise
        raise AssertionError("unreachable")

    monkeypatch.setattr(_sources.Rpc, "chain_id", chain_id)

    async def exercise() -> tuple[bool, int]:
        async def unused(_source: _corpus.ArtifactSource) -> dict[str, object]:
            raise AssertionError("acquisition continued after provider failure")

        with pytest.raises(BlockweaverError, match="primary failed"):
            await _sources.acquire(request, lambda _event: None, unused)
        current = asyncio.current_task()
        pending = [task for task in asyncio.all_tasks() if task is not current and not task.done()]
        cancelled_before_cleanup = sibling_cancelled
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        return cancelled_before_cleanup, len(pending)

    assert asyncio.run(exercise()) == (True, 0)


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
    monkeypatch.setattr(_sources, "CHUNK_SIZE", 2)
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


@pytest.mark.parametrize("corruption", ["digest", "duplicate"])
def test_resume_integrity_binds_checkpoint_bytes_and_exported_proofs(
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
    monkeypatch.setattr(_sources, "CHUNK_SIZE", 2)
    primary.changes[12] = {"hash": "invalid"}
    assert error(invoke(download_args(config)))["code"] == "RPC_INVALID"
    primary.changes.clear()

    chunks = tmp_path / "out" / f".blockweaver-{DATASET_ID}" / "chunks"
    checkpoint = next(chunks.iterdir())
    frame = pl.read_parquet(checkpoint)
    column = "base_fee_per_gas" if corruption == "digest" else "timestamp"
    frame.with_columns((pl.col(column) + 1).alias(column)).write_parquet(checkpoint)
    if corruption == "duplicate":
        first, last, _digest = checkpoint.stem.split("-")
        digest = hashlib.sha256(checkpoint.read_bytes()).hexdigest()
        checkpoint.rename(checkpoint.with_name(f"{first}-{last}-{digest}.parquet"))

    assert error(invoke(download_args(config)))["code"] == "RESUME_INVALID"


def test_verifier_disagreement_and_secret_redaction(tmp_path: Path, chains: tuple[ChainServer, ChainServer], make_config: Any) -> None:
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
    real_rename = _corpus._rename_no_replace

    def interrupt(_source: Path, _destination: Path) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(_corpus, "_rename_no_replace", interrupt)
    assert error(invoke(download_args(config)))["code"] == "INTERRUPTED"
    hidden = tmp_path / "out" / f".blockweaver-{DATASET_ID}"
    assert {path.name for path in (hidden / "ready").iterdir()} == {"manifest.json", "blocks.parquet"}

    requests = len(primary.requests) + len(verifier.requests)
    monkeypatch.setattr(_corpus, "_rename_no_replace", real_rename)
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
    real_rename, real_discard = _corpus._rename_no_replace, _corpus._discard_work

    def interrupt(*_args: object) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(_corpus, "_discard_work" if published else "_rename_no_replace", interrupt)
    assert error(invoke(download_args(config)))["code"] == "INTERRUPTED"
    monkeypatch.setattr(_corpus, "_rename_no_replace", real_rename)
    monkeypatch.setattr(_corpus, "_discard_work", real_discard)

    hidden = tmp_path / "out" / f".blockweaver-{DATASET_ID}"
    artifact = next(path for path in (tmp_path / "out").iterdir() if not path.name.startswith(".")) if published else hidden / "ready"
    manifest_path = artifact / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["source"]["provider"] = "other"
    manifest_path.write_text(json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n")
    receipt_path = hidden / "receipt.json"
    receipt = json.loads(receipt_path.read_text())
    receipt["artifact_sha256"] = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(artifact.iterdir()) if path.is_file()}
    receipt_path.write_text(json.dumps(receipt, sort_keys=True, separators=(",", ":")) + "\n")

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
    real_rename = _corpus._rename_no_replace

    def race(source: Path, destination: Path) -> None:
        destination.mkdir()
        real_rename(source, destination)

    monkeypatch.setattr(_corpus, "_rename_no_replace", race)
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


@pytest.mark.parametrize("state", ["incomplete", "receipt_only", "provisional"])
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
    elif state == "receipt_only":
        (hidden / "receipt.json").write_text("{}\n")
    else:
        (hidden / "ready.tmp").mkdir()
        (hidden / "ready.tmp" / "partial").write_text("partial")
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=output_root)

    artifact = artifact_from(invoke(download_args(config)))

    assert artifact.is_dir() and not hidden.exists()


def test_staged_recovery_ignores_obsolete_checkpoints_and_regenerates_a_corrupt_receipt(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary, verifier = chains
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out")
    real_rename = _corpus._rename_no_replace
    monkeypatch.setattr(_corpus, "_rename_no_replace", Mock(side_effect=KeyboardInterrupt))
    assert error(invoke(download_args(config)))["code"] == "INTERRUPTED"
    hidden = tmp_path / "out" / f".blockweaver-{DATASET_ID}"
    assert "version" not in json.loads((hidden / "binding.json").read_text())
    assert "version" not in json.loads((hidden / "receipt.json").read_text())
    with next((hidden / "chunks").iterdir()).open("ab") as stream:
        stream.write(b"obsolete checkpoint corruption")

    monkeypatch.setattr(_corpus, "_rename_no_replace", real_rename)
    verifier.changes[14] = {"hash": block_hash(999)}
    assert error(invoke(download_args(config)))["code"] == "RPC_MISMATCH"
    verifier.changes.clear()
    (hidden / "receipt.json").write_text("{broken\n")

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
    real_discard = _corpus._discard_work
    monkeypatch.setattr(_corpus, "_discard_work", Mock(side_effect=KeyboardInterrupt))
    assert error(invoke(download_args(config)))["code"] == "INTERRUPTED"
    hidden = tmp_path / "out" / f".blockweaver-{DATASET_ID}"
    (hidden / "binding.json").unlink()
    shutil.rmtree(hidden / "chunks")
    (hidden / "receipt.json").write_text("{}\n")
    monkeypatch.setattr(_corpus, "_discard_work", real_discard)

    receipt = json.loads(invoke(download_args(config)).stdout)

    assert receipt["reused_rows"] == receipt["rows"] and receipt["acquired_rows"] == 0
    assert not hidden.exists()


def test_candidate_samples_must_equal_retained_provider_verified_facts(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary, verifier = chains
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out", features=("timestamp",))
    real_write = _corpus._write_candidate

    def change_candidate(*args: Any, **kwargs: Any) -> Dataset:
        dataset = real_write(*args, **kwargs)
        frame = pl.read_parquet(dataset.data_path)
        frame.with_columns((pl.col("timestamp") + 1).alias("timestamp")).write_parquet(dataset.data_path)
        return dataset

    monkeypatch.setattr(_corpus, "_write_candidate", change_candidate)

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
    real_rename = _corpus._rename_no_replace
    monkeypatch.setattr(_corpus, "_rename_no_replace", Mock(side_effect=KeyboardInterrupt))
    assert error(invoke(download_args(config)))["code"] == "INTERRUPTED"
    monkeypatch.setattr(_corpus, "_rename_no_replace", real_rename)
    real_revalidate = _sources.RpcSource.revalidate

    async def mutate_after_rpc(source: _sources.RpcSource, dataset: Dataset) -> None:
        await real_revalidate(source, dataset)
        with dataset.data_path.open("ab") as stream:
            stream.write(b"changed")

    monkeypatch.setattr(_sources.RpcSource, "revalidate", mutate_after_rpc)

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
    real_directory_sync = _corpus._fsync_directory
    real_file_sync = _corpus._sync_final_file

    def sync_directory(path: Path) -> None:
        synced.append(path.resolve())
        real_directory_sync(path)

    def sync_file(path: Path) -> None:
        final_files.append(path.name)
        real_file_sync(path)

    monkeypatch.setattr(_corpus, "_fsync_directory", sync_directory)
    monkeypatch.setattr(_corpus, "_sync_final_file", sync_file)

    artifact_from(invoke(download_args(config)))

    assert synced[-3:] == [output_root, hidden, output_root]
    assert set(final_files) == {"manifest.json", "blocks.parquet"}


def test_unsupported_publication_capability_fails_before_destination_mutation(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary, verifier = chains
    output_root = tmp_path / "out"
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=output_root)
    monkeypatch.setattr(
        _corpus,
        "_ensure_publication_supported",
        Mock(side_effect=OSError(errno.ENOTSUP, "Atomic no-replace publication is unavailable")),
    )

    result = invoke(download_args(config))

    assert error(result)["code"] == "IO_FAILED"
    assert not (output_root / DATASET_ID).exists()
    assert (output_root / f".blockweaver-{DATASET_ID}" / "ready").is_dir()


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


def test_rpc_verify_reseals_artifact_bytes_after_network_wait(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary, verifier = chains
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out")
    artifact = artifact_from(invoke(download_args(config)))
    real_verify = _sources.verify_rpc

    async def mutate_after_rpc(dataset: Dataset, provider: Any, full: bool) -> dict[str, object]:
        result = await real_verify(dataset, provider, full)
        with dataset.data_path.open("ab") as stream:
            stream.write(b"changed")
        return result

    monkeypatch.setattr(_sources, "verify_rpc", mutate_after_rpc)

    result = invoke(["verify", str(artifact), "--config", str(config), "--provider", "verifier"])

    assert error(result)["code"] == "ARTIFACT_INVALID"


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

    monkeypatch.setattr(_corpus, "_completion_time", completion_time)
    artifact = artifact_from(invoke(download_args(config)))
    assert observed
    manifest_path = artifact / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    assert manifest["completed_at"] == "2026-08-10T12:00:00Z"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    assert error(invoke(["verify", str(artifact)]))["code"] == "ARTIFACT_INVALID"


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
    monkeypatch.setattr(_sources, "CHUNK_SIZE", 2)
    verifier.requests.clear()

    verified = invoke(["verify", str(artifact), "--config", str(config), "--provider", "verifier", "--full-rpc"])
    assert verified.exit_code == 0, verified.output
    fee_calls = [call for batch in verifier.requests for call in batch if call["method"] == "eth_feeHistory"]
    assert len(fee_calls) == 3
    assert all(int(call["params"][0], 16) <= 2 for call in fee_calls)

    verifier.changes[12] = {"parentHash": block_hash(1)}
    mismatch = invoke(["verify", str(artifact), "--config", str(config), "--provider", "verifier", "--full-rpc"])
    assert error(mismatch)["code"] == "RPC_MISMATCH"


def test_rpc_protocol_failure_is_bounded_and_machine_readable(tmp_path: Path, chains: tuple[ChainServer, ChainServer], make_config: Any) -> None:
    primary, verifier = chains
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out")
    primary.wrong_id_once = True
    failure = error(invoke(download_args(config)))
    assert failure["code"] == "RPC_INVALID"
    assert "response ID mismatch" in failure["message"]


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
    config = bigquery_config(make_config, tmp_path / "config.toml", primary, verifier, tmp_path / "out", features=("effective_priority_fee_per_gas_p50",))
    unavailable = FakeBigQuery(verifier)
    unavailable.tables["receipts"] = {"block_number": ("INTEGER", "NULLABLE")}
    monkeypatch.setattr(_sources, "open_bigquery", lambda _project: unavailable)
    assert error(invoke(download_args(config)))["code"] == "SOURCE_FEATURE_UNAVAILABLE"
    assert "dry_run" not in unavailable.calls and "execute" not in unavailable.calls

    repeated = FakeBigQuery(verifier)
    repeated.tables["receipts"]["effective_gas_price"] = ("INTEGER", "REPEATED")
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
    monkeypatch.setattr(_sources, "open_bigquery", lambda _project: warehouse)
    artifact = artifact_from(invoke(download_args(config)))
    assert warehouse.calls == [f"schema:{BQ_DATASET}.blocks", "dry_run", "execute"]
    assert ".transactions`" not in warehouse.sql and ".receipts`" not in warehouse.sql
    assert "base_fee_per_gas" not in warehouse.sql and "gas_used" not in warehouse.sql
    monkeypatch.setattr(_sources, "import_module", Mock(side_effect=AssertionError("local loading imported Google")))
    assert open_dataset(artifact).schema == ("block_number", "timestamp")


def test_bigquery_streams_selected_fields_and_matches_rpc_artifacts(
    tmp_path: Path, chains: tuple[ChainServer, ChainServer], make_config: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    primary, verifier = chains
    features = ("timestamp", "block_hash", "gas_used", "tx_count", "effective_priority_fee_per_gas_p50", "effective_priority_fee_per_gas_p90")
    config = bigquery_config(make_config, tmp_path / "config.toml", primary, verifier, tmp_path / "out", features=features)
    clients = [FakeBigQuery(verifier, wrong_hash_receipts=True), FakeBigQuery(verifier, wrong_hash_receipts=True)]
    queued_clients = iter(clients)
    monkeypatch.setattr(_sources, "open_bigquery", lambda _project: next(queued_clients))
    monkeypatch.setattr(_sources, "CHUNK_SIZE", 2)
    start = datetime.fromtimestamp(verifier.timestamp_base + 10, UTC).isoformat().replace("+00:00", "Z")
    end = datetime.fromtimestamp(verifier.timestamp_base + 14, UTC).isoformat().replace("+00:00", "Z")
    base = ["download", "--config", str(config), "--from-time", start, "--to-time", end]
    parquet = artifact_from(invoke([*base, "--id", DATASET_ID]))
    csv = artifact_from(invoke([*base, "--id", "22222222-2222-4222-8222-222222222222", "--format", "csv"]))
    assert pl.read_parquet(parquet / "blocks.parquet").equals(pl.read_csv(csv / "blocks.csv"))
    manifest = json.loads((parquet / "manifest.json").read_text())
    assert manifest["source"] == {"type": "bigquery", "dataset": BQ_DATASET, "verifier": "verifier"}
    assert manifest["verification"]["sampled_blocks"][0 :: len(manifest["verification"]["sampled_blocks"]) - 1] == [10, 14]
    assert all(client.page_size == 2 and client.calls[-1] == "execute" and "b.gas_limit AS _proof_gas_limit" in client.sql for client in clients)
    assert primary.requests == [] and verifier.requests
    rpc_config = make_config(tmp_path / "rpc.toml", primary, verifier, output_root=tmp_path / "rpc", features=features)
    rpc_artifact = artifact_from(invoke(download_args(rpc_config, dataset_id="33333333-3333-4333-8333-333333333333")))
    assert pl.read_parquet(parquet / "blocks.parquet").equals(pl.read_parquet(rpc_artifact / "blocks.parquet"))


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
