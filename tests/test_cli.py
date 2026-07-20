from __future__ import annotations

import hashlib
import json
from pathlib import Path

import polars as pl
from conftest import ChainServer
from typer.testing import CliRunner

from blockweaver.cli import app

CORPUS_ID = "11111111-1111-4111-8111-111111111111"
EXTENDED_ID = "22222222-2222-4222-8222-222222222222"


def test_cli_exposes_the_three_operations() -> None:
    result = CliRunner().invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "acquire" in result.stdout
    assert "extend" in result.stdout
    assert "verify" in result.stdout


def test_acquire_publishes_exact_corpus_and_machine_receipt(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
) -> None:
    primary, verifier = chains
    primary.http_failures = 1
    primary.omit_once = {12}
    primary.null_once = {13}

    result = CliRunner().invoke(
        app,
        [
            "acquire",
            "--storage-root",
            str(tmp_path),
            "--corpus-id",
            CORPUS_ID,
            "--chain-id",
            "1",
            "--first-block",
            "10",
            "--last-block",
            "14",
            "--batch-size",
            "3",
            "--concurrency",
            "2",
        ],
        env={"BLOCKWEAVER_RPC_URL": primary.url, "BLOCKWEAVER_VERIFY_RPC_URL": verifier.url},
    )

    assert result.exit_code == 0, result.output
    receipt = json.loads(result.stdout)
    destination = tmp_path / "corpora" / CORPUS_ID
    assert sorted(path.name for path in destination.iterdir()) == [
        "blocks.parquet",
        "corpus.json",
    ]
    assert json.loads((destination / "corpus.json").read_text()) == {
        "request": {
            "corpus_id": CORPUS_ID,
            "definition": {"chain_id": 1, "first_block": 10, "last_block": 14},
        },
        "finalized_anchor": {"block_number": 30, "block_hash": f"{31:064x}"},
    }
    frame = pl.read_parquet(destination / "blocks.parquet")
    assert frame.schema == {
        "block_number": pl.Int64,
        "timestamp": pl.Int64,
        "chain_id": pl.Int64,
        "base_fee_per_gas": pl.Int64,
        "gas_used": pl.Int64,
        "gas_limit": pl.Int64,
        "tx_count": pl.Int64,
    }
    assert frame["block_number"].to_list() == [10, 11, 12, 13, 14]
    assert receipt["operation"] == "acquire"
    assert receipt["rows"] == 5
    assert receipt["pair_sha256"] == {name: hashlib.sha256((destination / name).read_bytes()).hexdigest() for name in ("corpus.json", "blocks.parquet")}
    assert primary.url not in result.output
    assert verifier.url not in result.output
    assert all(json.loads(line)["event"] for line in result.stderr.splitlines())


def test_acquire_rejects_non_uuid4_as_a_machine_error(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        app,
        [
            "acquire",
            "--storage-root",
            str(tmp_path),
            "--corpus-id",
            "11111111-1111-1111-8111-111111111111",
            "--chain-id",
            "1",
            "--first-block",
            "10",
            "--last-block",
            "11",
            "--rpc-url",
            "http://primary.invalid",
            "--verify-rpc-url",
            "http://verifier.invalid",
        ],
    )

    assert result.exit_code == 1
    assert json.loads(result.stderr)["event"] == "error"


def test_acquire_resumes_only_complete_checkpoints_and_rejects_rebinding(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
) -> None:
    primary, verifier = chains
    primary.finalized = verifier.finalized = 1_032
    primary.changes[1_025] = {"hash": "invalid"}
    arguments = [
        "acquire",
        "--storage-root",
        str(tmp_path),
        "--corpus-id",
        CORPUS_ID,
        "--chain-id",
        "1",
        "--first-block",
        "0",
        "--last-block",
        "1030",
        "--rpc-url",
        primary.url,
        "--verify-rpc-url",
        verifier.url,
        "--batch-size",
        "100",
    ]

    failed = CliRunner().invoke(app, arguments)

    hidden = tmp_path / "corpora" / f".{CORPUS_ID}"
    assert failed.exit_code == 1
    assert not (tmp_path / "corpora" / CORPUS_ID).exists()
    assert sorted(path.name for path in (hidden / "chunks").iterdir()) == ["00000000000000000000-00000000000000001023.parquet"]
    mismatch = CliRunner().invoke(app, [*arguments[:-8], "--last-block", "1029", *arguments[-6:]])
    assert mismatch.exit_code == 1
    assert "different command" in mismatch.stderr

    primary.changes.clear()
    resumed = CliRunner().invoke(app, arguments)

    assert resumed.exit_code == 0, resumed.output
    assert not hidden.exists()
    events = [json.loads(line) for line in resumed.stderr.splitlines()]
    assert events[0] == {"event": "resume", "reused_rows": 1024}
    assert events[1]["first_block"] == 1024


def test_extend_keeps_source_immutable_and_includes_boundary_samples(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
) -> None:
    primary, verifier = chains
    acquired = CliRunner().invoke(
        app,
        [
            "acquire",
            "--storage-root",
            str(tmp_path),
            "--corpus-id",
            CORPUS_ID,
            "--chain-id",
            "1",
            "--first-block",
            "10",
            "--last-block",
            "14",
            "--rpc-url",
            primary.url,
            "--verify-rpc-url",
            verifier.url,
        ],
    )
    assert acquired.exit_code == 0, acquired.output
    source = tmp_path / "corpora" / CORPUS_ID
    before = {path.name: path.read_bytes() for path in source.iterdir()}

    extended = CliRunner().invoke(
        app,
        [
            "extend",
            str(source),
            "--storage-root",
            str(tmp_path),
            "--corpus-id",
            EXTENDED_ID,
            "--last-block",
            "18",
            "--rpc-url",
            primary.url,
            "--verify-rpc-url",
            verifier.url,
        ],
    )

    assert extended.exit_code == 0, extended.output
    assert {path.name: path.read_bytes() for path in source.iterdir()} == before
    destination = tmp_path / "corpora" / EXTENDED_ID
    assert pl.read_parquet(destination / "blocks.parquet")["block_number"].to_list() == list(range(10, 19))
    receipt = json.loads(extended.stdout)
    assert receipt["source_corpus_id"] == CORPUS_ID
    assert receipt["reused_rows"] == 5
    assert receipt["acquired_rows"] == 4
    assert {fact["block_number"] for fact in receipt["samples"]} >= {10, 14, 15, 18}


def test_verify_always_checks_every_local_row_and_full_rpc(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
) -> None:
    primary, verifier = chains
    acquired = CliRunner().invoke(
        app,
        [
            "acquire",
            "--storage-root",
            str(tmp_path),
            "--corpus-id",
            CORPUS_ID,
            "--chain-id",
            "1",
            "--first-block",
            "10",
            "--last-block",
            "24",
            "--rpc-url",
            primary.url,
            "--verify-rpc-url",
            verifier.url,
        ],
    )
    assert acquired.exit_code == 0, acquired.output
    corpus = tmp_path / "corpora" / CORPUS_ID
    primary.requests.clear()

    verified = CliRunner().invoke(app, ["verify", str(corpus), "--rpc-url", primary.url, "--full-rpc"])

    assert verified.exit_code == 0, verified.output
    receipt = json.loads(verified.stdout)
    assert receipt["verifier"]["mode"] == "full_rpc"
    assert len(receipt["samples"]) <= 5
    requested = {
        int(call["params"][0], 16)
        for batch in primary.requests
        for call in batch
        if call["method"] == "eth_getBlockByNumber" and call["params"][0].startswith("0x")
    }
    assert requested >= set(range(10, 25))

    frame = pl.read_parquet(corpus / "blocks.parquet")
    frame = frame.with_columns(pl.when(pl.col("block_number") == 10).then(0).otherwise(pl.col("base_fee_per_gas")).alias("base_fee_per_gas"))
    frame.write_parquet(corpus / "blocks.parquet")
    invalid = CliRunner().invoke(app, ["verify", str(corpus)])
    assert invalid.exit_code == 1
    assert "invalid block values" in invalid.stderr


def test_rpc_bisects_repeatedly_failing_batches(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
) -> None:
    primary, verifier = chains
    primary.finalized = verifier.finalized = 11
    primary.reject_batches_larger_than = 1

    result = CliRunner().invoke(
        app,
        [
            "acquire",
            "--storage-root",
            str(tmp_path),
            "--corpus-id",
            CORPUS_ID,
            "--chain-id",
            "1",
            "--first-block",
            "10",
            "--last-block",
            "11",
            "--rpc-url",
            primary.url,
            "--verify-rpc-url",
            verifier.url,
            "--batch-size",
            "20",
        ],
    )

    assert result.exit_code == 0, result.output
    assert any(len(batch) == 1 for batch in primary.requests)


def test_rpc_response_id_mismatch_fails_without_leaking_url(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
) -> None:
    primary, verifier = chains
    primary.wrong_id_once = True

    result = CliRunner().invoke(
        app,
        [
            "acquire",
            "--storage-root",
            str(tmp_path),
            "--corpus-id",
            CORPUS_ID,
            "--chain-id",
            "1",
            "--first-block",
            "10",
            "--last-block",
            "11",
            "--rpc-url",
            primary.url,
            "--verify-rpc-url",
            verifier.url,
        ],
    )

    assert result.exit_code == 1
    assert "response ID mismatch" in result.stderr
    assert primary.url not in result.output
