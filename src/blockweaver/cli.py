"""Machine-readable Blockweaver command line interface."""

from __future__ import annotations

import asyncio
import json
import os
import signal
from collections.abc import Callable, Coroutine
from pathlib import Path
from typing import Annotated, Any, Literal, Never
from uuid import UUID, uuid4

import typer
from typer._click.exceptions import ClickException
from typer.core import TyperGroup

from ._build import Publication, verify_dataset
from ._build import download as download_dataset
from ._contract import (
    FEATURES,
    BlockweaverError,
    Provider,
    available_sources,
    load_config,
    resolve_download_request,
    selected_config_path,
)

_EXAMPLE_CONFIG = """[defaults]
chain = "local"
source = "rpc"
provider = "primary"
verifier = "verifier"
output_root = "./downloads"
format = "parquet"
features = ["timestamp", "block_hash", "base_fee_per_gas", "gas_used", "gas_limit", "tx_count"]

[chains.local]
chain_id = 31337
finality_tag = "finalized"
# bigquery_dataset = "project.dataset"

[providers.primary]
url_env = "BLOCKWEAVER_PRIMARY_RPC_URL"
batch_size = 20
concurrency = 6
timeout = 30

[providers.verifier]
url_env = "BLOCKWEAVER_VERIFIER_RPC_URL"
batch_size = 20
concurrency = 6
timeout = 30

# [bigquery]
# project_env = "GOOGLE_CLOUD_PROJECT"
# maximum_bytes_billed = 1000000000
"""


class MachineGroup(TyperGroup):
    def main(self, *args: Any, **kwargs: Any) -> Any:
        standalone_mode = kwargs.get("standalone_mode", True)
        kwargs["standalone_mode"] = False
        try:
            result = super().main(*args, **kwargs)
        except ClickException as error:
            _progress({"event": "error", "code": "CLI_USAGE", "message": error.format_message()})
            if standalone_mode:
                raise SystemExit(error.exit_code) from None
            raise
        if standalone_mode and isinstance(result, int) and result:
            raise SystemExit(result)
        return result


app = typer.Typer(cls=MachineGroup, no_args_is_help=True, add_completion=False, pretty_exceptions_enable=False)
ConfigPath = Annotated[Path | None, typer.Option("--config", help="TOML config; then BLOCKWEAVER_CONFIG, then the user config path.")]


@app.command()
def init(config: ConfigPath = None) -> None:
    """Create a strict environment-backed example configuration."""
    path = selected_config_path(config)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                stream.write(_EXAMPLE_CONFIG)
                stream.flush()
                os.fsync(stream.fileno())
        except BaseException:
            path.unlink(missing_ok=True)
            raise
        _sync_directory(path.parent)
        _output({"operation": "init", "path": str(path)})
    except FileExistsError:
        _abort(BlockweaverError("CONFIG_EXISTS", f"Configuration already exists: {path}"))
    except OSError as error:
        _abort(BlockweaverError("IO_FAILED", str(error)))


@app.command()
def chains(config: ConfigPath = None) -> None:
    """List configured EVM chain profiles without secrets."""
    try:
        settings = load_config(selected_config_path(config))
        values = []
        for chain in settings.chains.values():
            value: dict[str, object] = {
                "name": chain.name,
                "chain_id": chain.chain_id,
                "finality_tag": chain.finality_tag,
                "provider": chain.provider or settings.defaults.provider,
                "verifier": chain.verifier or settings.defaults.verifier,
                "available_sources": list(available_sources(settings, chain)),
                "default": chain.name == settings.defaults.chain,
            }
            if chain.bigquery_dataset is not None:
                value["bigquery_dataset"] = chain.bigquery_dataset
            values.append(value)
        _output({"chains": values})
    except BlockweaverError as error:
        _abort(error)


@app.command()
def features(
    config: ConfigPath = None,
    chain: Annotated[str | None, typer.Option("--chain")] = None,
) -> None:
    """List selectable columns and configured source availability."""
    try:
        settings = load_config(selected_config_path(config))
        selected_chain = settings.chain(chain)
        sources = available_sources(settings, selected_chain)
        _output(
            {
                "chain": selected_chain.name,
                "available_sources": list(sources),
                "mandatory": {"name": "block_number", "type": "Int64", "unit": "block"},
                "features": [feature.document() for feature in FEATURES],
            }
        )
    except BlockweaverError as error:
        _abort(error)


@app.command()
def download(
    *,
    config: ConfigPath = None,
    chain: Annotated[str | None, typer.Option("--chain")] = None,
    source: Annotated[Literal["rpc", "bigquery"] | None, typer.Option("--source")] = None,
    provider: Annotated[str | None, typer.Option("--provider")] = None,
    verifier: Annotated[str | None, typer.Option("--verifier")] = None,
    rpc_url: Annotated[str | None, typer.Option("--rpc-url")] = None,
    verify_rpc_url: Annotated[str | None, typer.Option("--verify-rpc-url")] = None,
    output_root: Annotated[Path | None, typer.Option("--output-root")] = None,
    output_format: Annotated[Literal["parquet", "csv"] | None, typer.Option("--format")] = None,
    feature: Annotated[list[str] | None, typer.Option("--feature")] = None,
    from_block: Annotated[int | None, typer.Option("--from-block")] = None,
    to_block: Annotated[int | None, typer.Option("--to-block")] = None,
    from_time: Annotated[str | None, typer.Option("--from-time")] = None,
    to_time: Annotated[str | None, typer.Option("--to-time")] = None,
    dataset_id: Annotated[UUID | None, typer.Option("--id")] = None,
    batch_size: Annotated[int | None, typer.Option("--batch-size", min=1)] = None,
    concurrency: Annotated[int | None, typer.Option("--concurrency", min=1)] = None,
    timeout: Annotated[float | None, typer.Option("--timeout", min=0.001)] = None,
) -> None:
    """Download an exact finalized range and atomically publish it."""
    secrets = [value for value in (rpc_url, verify_rpc_url) if value]
    try:
        settings = load_config(selected_config_path(config))
        spec = resolve_download_request(
            settings,
            dataset_id=dataset_id or uuid4(),
            chain=chain,
            source=source,
            provider=provider,
            verifier=verifier,
            rpc_url=rpc_url,
            verify_rpc_url=verify_rpc_url,
            output_root=output_root,
            output_format=output_format,
            features=feature,
            from_block=from_block,
            to_block=to_block,
            from_time=from_time,
            to_time=to_time,
            batch_size=batch_size,
            concurrency=concurrency,
            timeout=timeout,
        )
        secrets.extend(spec.secrets())
    except BlockweaverError as error:
        _abort(error, secrets)
    _execute(lambda publication: download_dataset(spec, progress=_progress, publication=publication), secrets)


@app.command()
def verify(
    dataset: Path,
    *,
    config: ConfigPath = None,
    provider: Annotated[str | None, typer.Option("--provider")] = None,
    rpc_url: Annotated[str | None, typer.Option("--rpc-url")] = None,
    full_rpc: Annotated[bool, typer.Option("--full-rpc")] = False,
    batch_size: Annotated[int | None, typer.Option("--batch-size", min=1)] = None,
    concurrency: Annotated[int | None, typer.Option("--concurrency", min=1)] = None,
    timeout: Annotated[float | None, typer.Option("--timeout", min=0.001)] = None,
) -> None:
    """Strictly validate a dataset locally and optionally against RPC."""
    selected_provider: Provider | None = None
    secrets = [rpc_url] if rpc_url else []
    try:
        if config is not None and provider is None:
            raise BlockweaverError("VERIFY_INVALID", "--config requires --provider for RPC verification")
        if provider is None and rpc_url is None and (full_rpc or batch_size is not None or concurrency is not None or timeout is not None):
            raise BlockweaverError(
                "VERIFY_INVALID",
                "--full-rpc, --batch-size, --concurrency, and --timeout require --provider or --rpc-url",
            )
        if provider is not None:
            settings = load_config(selected_config_path(config))
            selected_provider = settings.provider(provider, url=rpc_url, batch_size=batch_size, concurrency=concurrency, timeout=timeout)
        elif rpc_url is not None:
            selected_provider = Provider(
                "cli",
                rpc_url,
                batch_size if batch_size is not None else 20,
                concurrency if concurrency is not None else 6,
                timeout if timeout is not None else 30,
            )
        if selected_provider is not None:
            secrets.append(selected_provider.url)
    except BlockweaverError as error:
        _abort(error, secrets)
    _execute(
        lambda _publication: verify_dataset(dataset, provider=selected_provider, full_rpc=full_rpc, progress=_progress),
        secrets,
    )


def _progress(value: dict[str, object]) -> None:
    typer.echo(json.dumps(value, sort_keys=True, separators=(",", ":")), err=True)


def _output(value: dict[str, object]) -> None:
    typer.echo(json.dumps(value, sort_keys=True, separators=(",", ":")))


def _abort(error: BlockweaverError, secrets: list[str] | None = None) -> Never:
    _progress({"event": "error", "code": error.code, "message": _redact(str(error), secrets or [])})
    raise typer.Exit(1)


def _execute(
    operation: Callable[[Publication], Coroutine[Any, Any, dict[str, object]]],
    secrets: list[str],
) -> None:
    phase = "running"

    def transition(next_phase: Literal["publishing", "committed"]) -> None:
        nonlocal phase
        phase = next_phase

    def interrupt(*_args: object) -> None:
        if phase == "running":
            raise KeyboardInterrupt

    previous_handler = signal.signal(signal.SIGINT, interrupt)
    try:
        receipt = asyncio.run(operation(transition))
    except (KeyboardInterrupt, asyncio.CancelledError):
        _progress({"event": "error", "code": "INTERRUPTED", "message": "Interrupted"})
        raise typer.Exit(130) from None
    except BlockweaverError as error:
        _abort(error, secrets)
    except Exception as error:
        _abort(BlockweaverError("INTERNAL_ERROR", str(error) or type(error).__name__), secrets)
    else:
        _output(receipt)
    finally:
        signal.signal(signal.SIGINT, previous_handler)


def _redact(message: str, secrets: list[str]) -> str:
    for secret in sorted(set(secrets), key=len, reverse=True):
        if secret:
            message = message.replace(secret, "<redacted>")
    return message


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
