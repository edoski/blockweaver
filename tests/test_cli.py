from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
import typer
from support import BQ_DATASET, ChainServer, artifact_from, download_args, error, invoke
from typer.testing import CliRunner

from blockweaver import cli as cli_module
from blockweaver.cli import MachineGroup


@pytest.mark.parametrize("arguments", [["--help"], *[[command, "--help"] for command in ("init", "chains", "features", "download", "verify")]])
def test_cli_help_paths(arguments: list[str]) -> None:
    help_result = invoke(arguments)
    assert help_result.exit_code == 0
    assert "Usage:" in help_result.stdout


def test_cli_commands_and_machine_usage_errors() -> None:
    help_result = invoke(["--help"])
    assert all(command in help_result.stdout for command in ("init", "chains", "features", "download", "verify"))
    usage_failure = invoke(["download", "--source", "unknown"])
    assert usage_failure.exit_code == 2
    assert usage_failure.stdout == ""
    assert usage_failure.stderr == (
        '{"code":"CLI_USAGE","event":"error","message":"Invalid value for \'--source\': \'unknown\' is not one of \'rpc\', \'bigquery\'."}\n'
    )
    failure = error(invoke(["download"]))
    assert failure["event"] == "error"
    assert failure["code"] == "CONFIG_NOT_FOUND"


def test_machine_usage_boundary_propagates_unrelated_lookalike() -> None:
    class LookalikeError(RuntimeError):
        exit_code = 2

        def format_message(self) -> str:
            return "not a usage error"

    failure = LookalikeError("failure")
    local_app = typer.Typer(cls=MachineGroup, invoke_without_command=True)

    @local_app.callback()
    def fail() -> None:
        raise failure

    result = CliRunner().invoke(local_app, [])

    assert result.exception is failure
    assert result.stdout == result.stderr == ""


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
    assert set(catalog) == {"chain", "available_sources", "mandatory", "features", "patterns"}
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
        "batch_size = 0",
        "concurrency = -1",
        "timeout = nan",
        "timeout = inf",
        "timeout = 3601",
        "timeout = " + "9" * 400,
    ],
)
def test_provider_resolution_rejects_invalid_domains_before_network(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
    replacement: str,
) -> None:
    primary, verifier = chains
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out")
    text = config.read_text()
    if replacement.startswith("url"):
        text = text.replace(f'url = "{primary.url}"', replacement, 1)
    else:
        setting = replacement.partition(" = ")[0]
        configured = {"batch_size": 3, "concurrency": 2, "timeout": 2}[setting]
        text = text.replace(f"{setting} = {configured}", replacement, 1)
    config.write_text(text)
    assert error(invoke(download_args(config)))["code"] == "CONFIG_INVALID"
    assert primary.requests == verifier.requests == []


@pytest.mark.parametrize(
    ("source", "features"),
    [
        ("unknown", ("timestamp",)),
        ("rpc", ("unknown",)),
        ("rpc", ("block_number",)),
        ("rpc", ("timestamp", "timestamp")),
    ],
)
def test_invalid_configured_defaults_are_configuration_failures(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    make_config: Any,
    source: str,
    features: tuple[str, ...],
) -> None:
    primary, verifier = chains
    config = make_config(tmp_path / "config.toml", primary, verifier, output_root=tmp_path / "out", source=source, features=features)

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

    configured = make_config(tmp_path / "configured.toml", primary, verifier, output_root=tmp_path / "out")
    for index, option in enumerate(("--chain", "--provider", "--verifier", "--rpc-url", "--verify-rpc-url"), 1):
        dataset_id = str(UUID(int=index, version=4))
        assert error(invoke([*download_args(configured, dataset_id=dataset_id), option, ""]))["code"] == "CONFIG_INVALID"
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
