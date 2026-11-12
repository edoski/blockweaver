from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
from support import ChainServer


@pytest.fixture
def chains() -> Iterator[tuple[ChainServer, ChainServer]]:
    with ChainServer() as primary, ChainServer() as verifier:
        yield primary, verifier


@pytest.fixture
def make_config() -> Callable[..., Path]:
    def write(
        path: Path,
        primary: ChainServer,
        verifier: ChainServer,
        *,
        output_root: Path,
        features: tuple[str, ...] = ("timestamp", "block_hash"),
        output_format: str = "parquet",
        source: str = "rpc",
        dataset: str | None = None,
        include_primary: bool = True,
    ) -> Path:
        quoted_features = ", ".join(json.dumps(feature) for feature in features)
        chain_dataset = f"bigquery_dataset = {json.dumps(dataset)}" if dataset else ""
        bigquery_config = '\n[bigquery]\nproject = "billing-project"\nmaximum_bytes_billed = 1000\n' if dataset else ""
        default_provider = 'provider = "primary"' if include_primary else ""
        primary_provider = (
            f"""[providers.primary]\nurl = {json.dumps(primary.url)}\nbatch_size = 3\nconcurrency = 2\ntimeout = 2\n""" if include_primary else ""
        )
        path.write_text(
            f"""[defaults]
chain = "test"
source = {json.dumps(source)}
{default_provider}
verifier = "verifier"
output_root = {json.dumps(str(output_root))}
format = {json.dumps(output_format)}
features = [{quoted_features}]

[chains.test]
chain_id = 1
finality_tag = "finalized"
{chain_dataset}

{primary_provider}
[providers.verifier]
url = {json.dumps(verifier.url)}
batch_size = 3
concurrency = 2
timeout = 2
{bigquery_config}
""",
            encoding="utf-8",
        )
        return path

    return write
