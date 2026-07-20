import asyncio
import json
from collections.abc import Coroutine
from pathlib import Path
from typing import Annotated, Any
from uuid import UUID

import typer
from typer._click.exceptions import ClickException, Exit
from typer.core import TyperGroup

from ._build import acquire_corpus, extend_corpus, verify_corpus
from ._contract import Request


class MachineGroup(TyperGroup):
    def main(self, *args: Any, **kwargs: Any) -> Any:
        kwargs["standalone_mode"] = False
        try:
            result = super().main(*args, **kwargs)
            if isinstance(result, int) and result:
                raise Exit(result)
            return result
        except ClickException as error:
            _progress({"event": "error", "message": error.format_message()})
            raise Exit(error.exit_code) from None


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
        acquire_corpus(
            request,
            storage_root=storage_root,
            rpc_url=rpc_url,
            verify_rpc_url=verify_rpc_url,
            batch_size=batch_size,
            concurrency=concurrency,
            progress=_progress,
        ),
        [rpc_url, verify_rpc_url],
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
        extend_corpus(
            source_corpus,
            storage_root=storage_root,
            corpus_id=corpus_id,
            last_block=last_block,
            rpc_url=rpc_url,
            verify_rpc_url=verify_rpc_url,
            batch_size=batch_size,
            concurrency=concurrency,
            progress=_progress,
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
        verify_corpus(
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


def _execute(operation: Coroutine[Any, Any, dict[str, object]], secrets: list[str]) -> None:
    try:
        receipt = asyncio.run(operation)
    except (KeyboardInterrupt, asyncio.CancelledError):
        _progress({"event": "interrupted"})
        raise typer.Exit(130) from None
    except Exception as error:
        message = str(error) or type(error).__name__
        for secret in secrets:
            message = message.replace(secret, "<redacted>")
        _progress({"event": "error", "message": message})
        raise typer.Exit(1) from None
    typer.echo(json.dumps(receipt, sort_keys=True, separators=(",", ":")))
