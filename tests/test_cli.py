from __future__ import annotations

import asyncio
import hashlib
import json
import os
import random
import shutil
import signal
import subprocess
import sys
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from conftest import ChainServer
from typer.testing import CliRunner

from blockweaver import _build
from blockweaver._rpc import Rpc
from blockweaver.cli import app

CORPUS_ID = "11111111-1111-4111-8111-111111111111"
EXTENDED_ID = "22222222-2222-4222-8222-222222222222"
ENRICHED_ID = "33333333-3333-4333-8333-333333333333"


def acquire_arguments(
    root: Path,
    primary: ChainServer,
    verifier: ChainServer,
    *,
    first: int = 10,
    last: int = 14,
    batch_size: int | None = None,
) -> list[str]:
    arguments = [
        "acquire",
        "--storage-root",
        str(root),
        "--corpus-id",
        CORPUS_ID,
        "--chain-id",
        "1",
        "--first-block",
        str(first),
        "--last-block",
        str(last),
        "--rpc-url",
        primary.url,
        "--verify-rpc-url",
        verifier.url,
    ]
    if batch_size is not None:
        arguments.extend(["--batch-size", str(batch_size)])
    return arguments


def test_cli_exposes_the_operations() -> None:
    result = CliRunner().invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "acquire" in result.stdout
    assert "enrich" in result.stdout
    assert "enrich-bigquery" in result.stdout
    assert "extend" in result.stdout
    assert "verify" in result.stdout


def test_enrich_bigquery_preserves_source_facts_and_appends_avalanche_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with ChainServer(chain_id=43_114) as primary, ChainServer(chain_id=43_114) as verifier:
        seeded = CliRunner().invoke(
            app,
            [
                "acquire",
                "--storage-root",
                str(tmp_path),
                "--corpus-id",
                CORPUS_ID,
                "--chain-id",
                "43114",
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
    assert seeded.exit_code == 0, seeded.output
    source = tmp_path / "corpora" / CORPUS_ID
    legacy = pl.read_parquet(source / "blocks.parquet").drop("effective_priority_fee_per_gas_p50")
    legacy.write_parquet(source / "blocks.parquet")
    source_before = {path.name: path.read_bytes() for path in source.iterdir()}

    query_rows = [
        {
            "block_number": number,
            "timestamp": 1_700_000_000 + number,
            "chain_id": 43_114,
            "base_fee_per_gas": (2_000_000_000 if number <= 14 else 1_000_000_000) + number,
            "gas_used": (20_000_000 if number <= 14 else 15_000_000) + number,
            "gas_limit": 40_000_000 if number <= 14 else 30_000_000,
            "tx_count": number % (4 if number <= 14 else 3),
            "receipt_gas_used": (20_000_000 if number <= 14 else 15_000_000) + number,
            "effective_priority_fee_per_gas_p50": number * 1_000,
            "block_hash": f"0x{number + 1:064x}",
            "parent_hash": f"0x{number:064x}",
        }
        for number in range(10, 17)
    ]

    class FakeRows:
        def __iter__(self):
            return iter(query_rows)

    class FakeJob:
        def result(self, *, page_size: int) -> FakeRows:
            assert page_size == 10_000
            return FakeRows()

    class FakeClient:
        instance: FakeClient
        queries = 0

        def __init__(self, *, project: str) -> None:
            assert project == "fable-503220"
            self.query_text = ""
            self.job_config: Any = None
            self.location = ""
            FakeClient.instance = self

        def query(self, query: str, *, job_config: Any, location: str) -> FakeJob:
            FakeClient.queries += 1
            self.query_text = query
            self.job_config = job_config
            self.location = location
            return FakeJob()

    monkeypatch.setattr(_build.bigquery, "Client", FakeClient)

    def arguments(corpus_id: str, rpc_url: str) -> list[str]:
        return [
            "enrich-bigquery",
            str(source),
            "--storage-root",
            str(tmp_path),
            "--corpus-id",
            corpus_id,
            "--last-block",
            "16",
            "--gcp-project",
            "fable-503220",
            "--maximum-bytes-billed",
            "200000000000",
            "--rpc-url",
            rpc_url,
        ]

    query_rows[0]["receipt_gas_used"] = 1
    rejected = CliRunner().invoke(app, arguments(EXTENDED_ID, "http://unused"))
    assert rejected.exit_code == 1
    assert "receipts are incomplete for block 10" in rejected.stderr
    query_rows[0]["receipt_gas_used"] = query_rows[0]["gas_used"]

    with ChainServer(chain_id=43_114) as rpc:
        result = CliRunner().invoke(app, arguments(ENRICHED_ID, rpc.url))

    assert result.exit_code == 0, result.output
    assert {path.name: path.read_bytes() for path in source.iterdir()} == source_before
    destination = tmp_path / "corpora" / ENRICHED_ID
    assert {path.name for path in destination.iterdir()} == {"blocks.parquet", "corpus.json"}
    frame = pl.read_parquet(destination / "blocks.parquet")
    assert frame.head(5).select(legacy.columns).equals(legacy)
    assert frame["effective_priority_fee_per_gas_p50"].to_list() == [number * 1_000 for number in range(10, 17)]
    assert frame.tail(2).select(legacy.columns).to_dicts() == [{name: row[name] for name in legacy.columns} for row in query_rows[-2:]]
    document = json.loads((destination / "corpus.json").read_text())
    assert document["request"]["definition"] == {"chain_id": 43_114, "first_block": 10, "last_block": 16}
    assert document["finalized_anchor"] == {"block_number": 30, "block_hash": f"{31:064x}"}
    receipt = json.loads(result.stdout)
    assert receipt["operation"] == "enrich-bigquery"
    assert receipt["reused_rows"] == 5
    assert receipt["acquired_rows"] == 2
    assert receipt["verifier"]["mode"] == "bigquery_rpc"

    client = FakeClient.instance
    assert client.location == "US"
    assert "cumulative_gas_used >= DIV(block_gas_used, 2)" in client.query_text
    assert client.query_text.count("block_timestamp >= TIMESTAMP_SECONDS(@first_timestamp)") == 2
    assert client.query_text.count("block_timestamp < TIMESTAMP_SECONDS(@after_timestamp)") == 2
    config = client.job_config
    assert config is not None
    assert config.maximum_bytes_billed == 200_000_000_000
    parameters = {parameter.name: parameter.value for parameter in config.query_parameters}
    assert parameters["first_block"] == 10
    assert parameters["last_block"] == 16
    assert parameters["first_timestamp"] == legacy["timestamp"].min()
    last_timestamp = legacy["timestamp"].max()
    assert isinstance(last_timestamp, int)
    assert parameters["after_timestamp"] >= last_timestamp + 1

    duplicate = CliRunner().invoke(app, arguments(ENRICHED_ID, "http://unused"))
    assert duplicate.exit_code == 1
    assert FakeClient.queries == 2


def test_verify_exposes_only_the_positive_full_rpc_flag() -> None:
    result = CliRunner().invoke(app, ["verify", "--help"])

    assert result.exit_code == 0
    assert "--full-rpc" in result.stdout
    assert "--no-full-rpc" not in result.stdout


@pytest.mark.parametrize(
    "arguments",
    [
        ["acquire"],
        ["acquire", "--corpus-id", "not-a-uuid"],
        ["acquire", "--chain-id", "not-an-integer"],
    ],
)
def test_cli_parse_failures_are_single_machine_errors(arguments: list[str]) -> None:
    result = CliRunner().invoke(app, arguments)

    assert result.exit_code != 0
    assert result.stdout == ""
    assert json.loads(result.stderr) == {
        "event": "error",
        "message": json.loads(result.stderr)["message"],
    }


def test_installed_executable_failures_are_single_machine_errors(tmp_path: Path) -> None:
    executable = Path(sys.executable).with_name("blockweaver")
    cases = [(["acquire"], 2), (["verify", str(tmp_path / "missing")], 1)]

    for arguments, exit_code in cases:
        result = subprocess.run([executable, *arguments], text=True, capture_output=True, check=False)
        lines = result.stderr.splitlines()
        assert result.returncode == exit_code
        assert result.stdout == ""
        assert len(lines) == 1
        assert json.loads(lines[0])["event"] == "error"


def test_installed_verify_rpc_failure_is_machine_readable(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
) -> None:
    primary, verifier = chains
    seeded = CliRunner().invoke(app, acquire_arguments(tmp_path, primary, verifier, last=11))
    assert seeded.exit_code == 0, seeded.output
    primary.changes[10] = {"hash": "invalid"}

    executable = Path(sys.executable).with_name("blockweaver")
    corpus = tmp_path / "corpora" / CORPUS_ID
    result = subprocess.run(
        [executable, "verify", corpus, "--rpc-url", primary.url, "--full-rpc"],
        text=True,
        capture_output=True,
        check=False,
    )

    messages = [json.loads(line) for line in result.stderr.splitlines()]
    assert result.returncode == 1
    assert result.stdout == ""
    assert [message["event"] for message in messages] == ["local_valid", "error"]
    assert messages[-1]["message"] == "Invalid block hash"


@pytest.mark.parametrize("failure_site", ["outer", "bisected"])
def test_rpc_fanouts_finish_siblings_before_closing_the_session(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    monkeypatch: pytest.MonkeyPatch,
    failure_site: str,
) -> None:
    primary, verifier = chains
    last = 17 if failure_site == "outer" else 11
    seeded = CliRunner().invoke(app, acquire_arguments(tmp_path, primary, verifier, last=last, batch_size=2))
    assert seeded.exit_code == 0, seeded.output
    primary.requests.clear()
    primary.request_counts.clear()

    if failure_site == "outer":
        primary.changes[10] = {"hash": "invalid"}
        primary.delays[10] = 0.05
        primary.omit = set(range(12, 18))
        primary.delays.update({number: 2.0 for number in range(12, 18)})
    else:
        primary.omit_counts[10] = 3
        primary.omit = {11}
        primary.changes_after[10] = (3, {"hash": "invalid"})
        primary.delays[10] = 0.05
        primary.delays_after[11] = (3, 2.0)

    posts_after_close: list[list[dict[str, object]]] = []
    original_post = Rpc._post
    original_exit = Rpc.__aexit__

    async def track_post(self: Rpc, calls: list[dict[str, object]]) -> tuple[int, object, float | None]:
        if self._session is not None and self._session.closed:
            posts_after_close.append(calls)
        return await original_post(self, calls)

    async def yielding_exit(self: Rpc, *args: object) -> None:
        await original_exit(self, *args)
        await asyncio.sleep(0.05)

    monkeypatch.setattr(Rpc, "_post", track_post)
    monkeypatch.setattr(Rpc, "__aexit__", yielding_exit)
    monkeypatch.setattr(random, "uniform", lambda *_args: 0.0)

    corpus = tmp_path / "corpora" / CORPUS_ID
    result = CliRunner().invoke(
        app,
        ["verify", str(corpus), "--rpc-url", primary.url, "--full-rpc", "--batch-size", "2", "--concurrency", "6"],
    )

    assert result.exit_code == 1
    assert posts_after_close == []


def test_acquire_publishes_exact_corpus_and_machine_receipt(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
) -> None:
    primary, verifier = chains
    primary.http_failures = 1
    primary.omit_once = {12}
    primary.null_once = {13}
    primary.priority_fees[12] = 0

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
        "effective_priority_fee_per_gas_p50": pl.Int64,
    }
    assert frame["block_number"].to_list() == [10, 11, 12, 13, 14]
    assert frame["effective_priority_fee_per_gas_p50"].to_list() == [1000, 1100, 0, 1300, 1400]
    fee_calls = [call for batch in primary.requests for call in batch if call["method"] == "eth_feeHistory"]
    assert any(call["params"] == ["0x5", "0xe", [50]] for call in fee_calls)
    assert receipt["operation"] == "acquire"
    assert receipt["rows"] == 5
    assert {fact["block_number"] for fact in receipt["samples"]} == {10, 11, 12, 13, 14}
    assert receipt["pair_sha256"] == {name: hashlib.sha256((destination / name).read_bytes()).hexdigest() for name in ("corpus.json", "blocks.parquet")}
    assert primary.url not in result.output
    assert verifier.url not in result.output
    assert all(json.loads(line)["event"] for line in result.stderr.splitlines())


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"oldestBlock": "0xb"}, "oldestBlock"),
        ({"reward": [["0x1"]]}, "reward coverage"),
        ({"reward": [["0x1", "0x2"]] * 2}, "reward row"),
        ({"reward": [[hex(2**63)], ["0x1"]]}, "signed Int64"),
    ],
)
def test_acquire_rejects_invalid_fee_history(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    change: dict[str, object],
    message: str,
) -> None:
    primary, verifier = chains
    primary.fee_history_changes = change

    result = CliRunner().invoke(app, acquire_arguments(tmp_path, primary, verifier, last=11))

    assert result.exit_code == 1
    assert message in result.stderr
    assert not (tmp_path / "corpora" / CORPUS_ID).exists()


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


def test_acquire_recovers_partial_initialization_without_lock_residue(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
) -> None:
    primary, verifier = chains
    hidden = tmp_path / "corpora" / f".{CORPUS_ID}"
    (hidden / "chunks").mkdir(parents=True)

    result = CliRunner().invoke(app, acquire_arguments(tmp_path, primary, verifier))

    assert result.exit_code == 0, result.output
    assert [path.name for path in (tmp_path / "corpora").iterdir()] == [CORPUS_ID]


@pytest.mark.parametrize("corrupt_ready", [False, True], ids=["valid", "invalid"])
def test_acquire_recovers_ready_state(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    monkeypatch: pytest.MonkeyPatch,
    corrupt_ready: bool,
) -> None:
    primary, verifier = chains
    arguments = acquire_arguments(tmp_path, primary, verifier)
    corpora = tmp_path / "corpora"
    hidden = corpora / f".{CORPUS_ID}"
    original_rename = os.rename

    def interrupt_ready(source: Path, destination: Path) -> None:
        original_rename(source, destination)
        if Path(source).name == "ready.tmp" and Path(destination).name == "ready":
            raise OSError("simulated crash after ready transition")

    monkeypatch.setattr(os, "rename", interrupt_ready)
    interrupted = CliRunner().invoke(app, arguments)

    assert interrupted.exit_code == 1
    assert (hidden / "ready").is_dir()
    assert (hidden / "receipt.json").is_file()
    if corrupt_ready:
        (hidden / "ready" / "blocks.parquet").write_bytes(b"incomplete")

    monkeypatch.setattr(os, "rename", original_rename)
    recovered = CliRunner().invoke(app, arguments)

    assert recovered.exit_code == 0, recovered.output
    assert json.loads(recovered.stdout)["acquired_rows"] == 0
    assert [path.name for path in corpora.iterdir()] == [CORPUS_ID]


def test_acquire_recovers_after_atomic_publication_before_work_cleanup(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary, verifier = chains
    arguments = acquire_arguments(tmp_path, primary, verifier)
    destination = tmp_path / "corpora" / CORPUS_ID
    hidden = tmp_path / "corpora" / f".{CORPUS_ID}"
    original_rmtree = shutil.rmtree

    def interrupt_cleanup(path: Path, *_args: object, **_kwargs: object) -> None:
        if Path(path) == hidden and destination.exists():
            raise OSError("simulated crash after publication")
        original_rmtree(path)

    monkeypatch.setattr(shutil, "rmtree", interrupt_cleanup)
    interrupted = CliRunner().invoke(app, arguments)

    assert interrupted.exit_code == 1
    assert destination.is_dir()
    assert hidden.is_dir()

    monkeypatch.setattr(shutil, "rmtree", original_rmtree)
    recovered = CliRunner().invoke(app, arguments)

    assert recovered.exit_code == 0, recovered.output
    receipt = json.loads(recovered.stdout)
    assert receipt["corpus_id"] == CORPUS_ID
    assert receipt["reused_rows"] == 5
    assert receipt["acquired_rows"] == 0
    assert not hidden.exists()


def test_acquire_returns_receipt_when_sigint_arrives_after_atomic_publication(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary, verifier = chains
    destination = tmp_path / "corpora" / CORPUS_ID
    original_rename = os.rename

    def interrupt_after_rename(source: Path, target: Path) -> None:
        original_rename(source, target)
        if Path(target) == destination:
            os.kill(os.getpid(), signal.SIGINT)

    monkeypatch.setattr(os, "rename", interrupt_after_rename)
    result = CliRunner().invoke(app, acquire_arguments(tmp_path, primary, verifier))

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["corpus_id"] == CORPUS_ID
    assert destination.is_dir()


def test_enrich_copies_validated_legacy_facts_and_acquires_only_p50(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
) -> None:
    primary, verifier = chains
    seeded = CliRunner().invoke(app, acquire_arguments(tmp_path, primary, verifier))
    assert seeded.exit_code == 0, seeded.output
    source = tmp_path / "corpora" / CORPUS_ID
    legacy = pl.read_parquet(source / "blocks.parquet").drop("effective_priority_fee_per_gas_p50")
    legacy.write_parquet(source / "blocks.parquet")
    source_before = {path.name: path.read_bytes() for path in source.iterdir()}
    source_facts = legacy.to_dict(as_series=False)
    primary.requests.clear()
    verifier.requests.clear()

    rejected = CliRunner().invoke(app, ["verify", str(source)])
    enriched = CliRunner().invoke(
        app,
        [
            "enrich",
            str(source),
            "--storage-root",
            str(tmp_path),
            "--corpus-id",
            ENRICHED_ID,
            "--rpc-url",
            primary.url,
            "--verify-rpc-url",
            verifier.url,
        ],
    )

    assert rejected.exit_code == 1
    assert "noncanonical schema" in rejected.stderr
    assert enriched.exit_code == 0, enriched.output
    assert {path.name: path.read_bytes() for path in source.iterdir()} == source_before
    destination = tmp_path / "corpora" / ENRICHED_ID
    frame = pl.read_parquet(destination / "blocks.parquet")
    assert frame.select(legacy.columns).to_dict(as_series=False) == source_facts
    assert frame["effective_priority_fee_per_gas_p50"].to_list() == [1000, 1100, 1200, 1300, 1400]
    methods = {call["method"] for server in chains for batch in server.requests for call in batch}
    assert methods == {"eth_chainId", "eth_feeHistory"}
    receipt = json.loads(enriched.stdout)
    assert receipt["operation"] == "enrich"
    assert receipt["source_corpus_id"] == CORPUS_ID
    assert receipt["rows"] == receipt["reused_rows"] == receipt["source_rows"] == 5
    assert receipt["acquired_rows"] == 0


def test_extend_keeps_source_immutable_and_includes_boundary_samples(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
) -> None:
    primary, verifier = chains
    acquired = CliRunner().invoke(app, acquire_arguments(tmp_path, primary, verifier))
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
    assert len(receipt["samples"]) == 7


def test_extend_binds_the_exact_source_bytes_it_validates(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary, verifier = chains
    seeded = CliRunner().invoke(app, acquire_arguments(tmp_path, primary, verifier))
    assert seeded.exit_code == 0, seeded.output
    source = tmp_path / "corpora" / CORPUS_ID
    original_schema = pl.read_parquet_schema
    changed = False

    def rewrite_during_validation(path: Path, *args: object, **kwargs: object) -> dict[str, pl.DataType]:
        nonlocal changed
        schema = original_schema(path, *args, **kwargs)
        if Path(path) == source / "blocks.parquet" and not changed:
            changed = True
            frame = pl.read_parquet(path)
            frame.write_parquet(path, compression="uncompressed")
        return schema

    monkeypatch.setattr(pl, "read_parquet_schema", rewrite_during_validation)
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

    assert extended.exit_code == 1
    assert "Source Corpus changed during validation" in extended.stderr
    assert not (tmp_path / "corpora" / EXTENDED_ID).exists()


def test_verify_always_checks_every_local_row_and_full_rpc(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
) -> None:
    primary, verifier = chains
    acquired = CliRunner().invoke(app, acquire_arguments(tmp_path, primary, verifier, last=24))
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


def test_large_full_verify_and_extension_use_bounded_rpc_batches(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
) -> None:
    primary, verifier = chains
    primary.finalized = verifier.finalized = 1_030
    seeded = CliRunner().invoke(app, acquire_arguments(tmp_path, primary, verifier, first=0, last=1025, batch_size=100))
    assert seeded.exit_code == 0, seeded.output
    corpus = tmp_path / "corpora" / CORPUS_ID
    primary.requests.clear()
    verified = CliRunner().invoke(
        app,
        ["verify", str(corpus), "--rpc-url", primary.url, "--full-rpc", "--batch-size", "100"],
    )
    extended = CliRunner().invoke(
        app,
        [
            "extend",
            str(corpus),
            "--storage-root",
            str(tmp_path),
            "--corpus-id",
            EXTENDED_ID,
            "--last-block",
            "1028",
            "--rpc-url",
            primary.url,
            "--verify-rpc-url",
            verifier.url,
            "--batch-size",
            "100",
        ],
    )

    assert verified.exit_code == 0, verified.output
    assert len(json.loads(verified.stdout)["samples"]) == 5
    assert extended.exit_code == 0, extended.output
    assert max(map(len, primary.requests)) <= 100


def test_rpc_verification_rejects_finality_regression_below_stored_anchor(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
) -> None:
    primary, verifier = chains
    seeded = CliRunner().invoke(app, acquire_arguments(tmp_path, primary, verifier))
    assert seeded.exit_code == 0, seeded.output
    primary.finalized = 29

    result = CliRunner().invoke(app, ["verify", str(tmp_path / "corpora" / CORPUS_ID), "--rpc-url", primary.url])

    assert result.exit_code == 1
    assert "does not cover the stored finalized anchor" in result.stderr


def test_rpc_verification_rejects_broken_target_to_anchor_ancestry(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
) -> None:
    primary, verifier = chains
    seeded = CliRunner().invoke(app, acquire_arguments(tmp_path, primary, verifier))
    assert seeded.exit_code == 0, seeded.output
    primary.changes[15] = {"parentHash": f"0x{999:064x}"}

    result = CliRunner().invoke(app, ["verify", str(tmp_path / "corpora" / CORPUS_ID), "--rpc-url", primary.url])

    assert result.exit_code == 1
    assert "Parent link mismatch at block 15" in result.stderr


def test_rpc_bisects_repeatedly_failing_batches(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
) -> None:
    primary, verifier = chains
    primary.finalized = verifier.finalized = 11
    primary.reject_batches_larger_than = 1

    result = CliRunner().invoke(app, acquire_arguments(tmp_path, primary, verifier, last=11, batch_size=20))

    assert result.exit_code == 0, result.output
    assert any(len(batch) == 1 for batch in primary.requests)


def test_rpc_response_id_mismatch_fails_without_leaking_url(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
) -> None:
    primary, verifier = chains
    primary.wrong_id_once = True

    result = CliRunner().invoke(app, acquire_arguments(tmp_path, primary, verifier, last=11))

    assert result.exit_code == 1
    assert "response ID mismatch" in result.stderr
    assert primary.url not in result.output


def test_rpc_rejects_corrupt_success_before_retrying_missing_sibling(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary, verifier = chains
    primary.omit = {10}
    primary.changes[11] = {"hash": "invalid"}
    monkeypatch.setattr(random, "uniform", lambda *_args: 0.0)

    result = CliRunner().invoke(app, acquire_arguments(tmp_path, primary, verifier, last=11))

    assert result.exit_code == 1
    assert "Invalid block hash" in result.stderr


def test_rpc_error_requires_integer_code_and_string_message(
    tmp_path: Path,
    chains: tuple[ChainServer, ChainServer],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary, verifier = chains
    primary.errors[10] = {"code": -32000}
    monkeypatch.setattr(random, "uniform", lambda *_args: 0.0)

    result = CliRunner().invoke(app, acquire_arguments(tmp_path, primary, verifier, last=10))

    assert result.exit_code == 1
    assert "Invalid JSON-RPC error shape" in result.stderr
