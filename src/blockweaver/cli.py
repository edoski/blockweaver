import asyncio
import json
import signal
from collections.abc import Callable, Coroutine
from pathlib import Path
from typing import Annotated, Any, Literal
from uuid import UUID

import typer
from typer._click.exceptions import ClickException
from typer.core import TyperGroup

from ._bigquery import enrich_avalanche_bigquery
from ._build import Publication, acquire_corpus, enrich_corpus, extend_corpus, verify_corpus
from ._contract import Request


class MachineGroup(TyperGroup):
    def main(self, *args: Any, **kwargs: Any) -> Any:
        standalone_mode = kwargs.get("standalone_mode", True)
        kwargs["standalone_mode"] = False
        try:
            result = super().main(*args, **kwargs)
        except ClickException as error:
            _progress({"event": "error", "message": error.format_message()})
            if standalone_mode:
                raise SystemExit(error.exit_code) from None
            raise
        if standalone_mode and isinstance(result, int) and result:
            raise SystemExit(result)
        return result


app = typer.Typer(cls=MachineGroup, no_args_is_help=True, add_completion=False, pretty_exceptions_enable=False)

RpcUrl = Annotated[str, typer.Option(envvar="BLOCKWEAVER_RPC_URL")]
VerifyRpcUrl = Annotated[str, typer.Option(envvar="BLOCKWEAVER_VERIFY_RPC_URL")]
OptionalRpcUrl = Annotated[str | None, typer.Option(envvar="BLOCKWEAVER_RPC_URL")]


@app.command()
def acquire(
    *,
    storage_root: Annotated[Path, typer.Option()],
    corpus_id: Annotated[UUID, typer.Option()],
    chain_id: Annotated[int, typer.Option(min=0)],
    first_block: Annotated[int, typer.Option(min=0)],
    last_block: Annotated[int, typer.Option(min=0)],
    rpc_url: RpcUrl,
    verify_rpc_url: VerifyRpcUrl,
    batch_size: Annotated[int, typer.Option(min=1)] = 20,
    concurrency: Annotated[int, typer.Option(min=1)] = 6,
) -> None:
    """Acquire and atomically publish a Corpus."""
    try:
        request = Request(corpus_id, chain_id, first_block, last_block)
    except ValueError as error:
        _progress({"event": "error", "message": str(error)})
        raise typer.Exit(1) from None
    _execute(
        lambda publication: acquire_corpus(
            request,
            storage_root=storage_root,
            rpc_url=rpc_url,
            verify_rpc_url=verify_rpc_url,
            batch_size=batch_size,
            concurrency=concurrency,
            progress=_progress,
            publication=publication,
        ),
        [rpc_url, verify_rpc_url],
    )


@app.command()
def enrich(
    source_corpus: Path,
    *,
    storage_root: Annotated[Path, typer.Option()],
    corpus_id: Annotated[UUID, typer.Option()],
    rpc_url: RpcUrl,
    verify_rpc_url: VerifyRpcUrl,
    batch_size: Annotated[int, typer.Option(min=1)] = 20,
    concurrency: Annotated[int, typer.Option(min=1)] = 6,
) -> None:
    """Add priority-fee P50 to a validated seven-column Corpus."""
    _execute(
        lambda publication: enrich_corpus(
            source_corpus,
            storage_root=storage_root,
            corpus_id=corpus_id,
            rpc_url=rpc_url,
            verify_rpc_url=verify_rpc_url,
            batch_size=batch_size,
            concurrency=concurrency,
            progress=_progress,
            publication=publication,
        ),
        [rpc_url, verify_rpc_url],
    )


@app.command("enrich-bigquery")
def enrich_bigquery(
    source_corpus: Path,
    *,
    storage_root: Annotated[Path, typer.Option()],
    corpus_id: Annotated[UUID, typer.Option()],
    last_block: Annotated[int, typer.Option(min=0)],
    gcp_project: Annotated[str, typer.Option()],
    maximum_bytes_billed: Annotated[int, typer.Option(min=1)],
) -> None:
    """Enrich and optionally extend Avalanche through BigQuery."""
    _execute(
        lambda publication: enrich_avalanche_bigquery(
            source_corpus,
            storage_root=storage_root,
            corpus_id=corpus_id,
            last_block=last_block,
            gcp_project=gcp_project,
            maximum_bytes_billed=maximum_bytes_billed,
            progress=_progress,
            publication=publication,
        ),
        [],
    )


@app.command()
def extend(
    source_corpus: Path,
    *,
    storage_root: Annotated[Path, typer.Option()],
    corpus_id: Annotated[UUID, typer.Option()],
    last_block: Annotated[int, typer.Option(min=0)],
    rpc_url: RpcUrl,
    verify_rpc_url: VerifyRpcUrl,
    batch_size: Annotated[int, typer.Option(min=1)] = 20,
    concurrency: Annotated[int, typer.Option(min=1)] = 6,
) -> None:
    """Extend a validated Corpus into a new Corpus."""
    _execute(
        lambda publication: extend_corpus(
            source_corpus,
            storage_root=storage_root,
            corpus_id=corpus_id,
            last_block=last_block,
            rpc_url=rpc_url,
            verify_rpc_url=verify_rpc_url,
            batch_size=batch_size,
            concurrency=concurrency,
            progress=_progress,
            publication=publication,
        ),
        [rpc_url, verify_rpc_url],
    )


@app.command()
def verify(
    corpus: Path,
    *,
    rpc_url: OptionalRpcUrl = None,
    full_rpc: Annotated[bool, typer.Option("--full-rpc")] = False,
    batch_size: Annotated[int, typer.Option(min=1)] = 20,
    concurrency: Annotated[int, typer.Option(min=1)] = 6,
) -> None:
    """Validate a Corpus locally and optionally against RPC."""
    _execute(
        lambda _publication: verify_corpus(
            corpus,
            rpc_url=rpc_url,
            full_rpc=full_rpc,
            batch_size=batch_size,
            concurrency=concurrency,
            progress=_progress,
        ),
        [rpc_url] if rpc_url else [],
    )


def _progress(value: dict[str, object]) -> None:
    typer.echo(json.dumps(value, sort_keys=True, separators=(",", ":")), err=True)


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
        _progress({"event": "interrupted"})
        raise typer.Exit(130) from None
    except Exception as error:
        message = str(error) or type(error).__name__
        for secret in secrets:
            message = message.replace(secret, "<redacted>")
        _progress({"event": "error", "message": message})
        raise typer.Exit(1) from None
    else:
        typer.echo(json.dumps(receipt, sort_keys=True, separators=(",", ":")))
    finally:
        signal.signal(signal.SIGINT, previous_handler)
