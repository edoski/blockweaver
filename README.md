# Blockweaver

Acquire and verify immutable EVM block corpora.

Blockweaver is a standalone CLI for producing the exact two-file Corpus format consumed by FABLE. It talks directly to EVM JSON-RPC endpoints with `aiohttp`; it does not import FABLE, Web3.py, provider SDKs, or a provider registry.

## Install

Python 3.11 or newer is required.

```console
uv tool install blockweaver
```

For development from a checkout:

```console
uv sync --locked --dev
uv run blockweaver --help
```

## Acquire

Use independent primary and verifier endpoints. Environment variables keep credentials out of the command line.

```console
export BLOCKWEAVER_RPC_URL='https://primary.example.invalid'
export BLOCKWEAVER_VERIFY_RPC_URL='https://verifier.example.invalid'

blockweaver acquire \
  --storage-root ./data \
  --corpus-id 11111111-1111-4111-8111-111111111111 \
  --chain-id 1 \
  --first-block 19000000 \
  --last-block 19000999
```

`--rpc-url` and `--verify-rpc-url` override the environment. `--batch-size` defaults to 20 and `--concurrency` to 6. The caller must supply a UUID4; Blockweaver never mints one.

Block headers supply the existing block facts. Batched `eth_feeHistory(blockCount, newestBlock, [50])` calls supply `effective_priority_fee_per_gas_p50`. Blockweaver requires the requested `oldestBlock` and one P50 reward value for every block. It does not use the response's `baseFeePerGas` length.

Acquisition checkpoints complete deterministic ranges under exactly:

```text
ROOT/corpora/.<UUID>/
```

Running the same command resumes those checkpoints. A request with different range, chain, operation, or extension source is rejected. Publication validates the complete candidate and atomically renames its ready directory to `ROOT/corpora/<UUID>/`. Existing destinations are never overwritten.

The destination rename plus parent-directory sync is the commit boundary. A SIGINT during that short publication transition is deferred so a committed command can emit its receipt.

## Enrich

`enrich` is the only command that accepts the former exact seven-column Blockweaver Corpus. It validates that source, copies its seven facts without block RPC reads, acquires P50 for the same inclusive range from independent primary and verifier endpoints, and publishes an eight-column Corpus under a new UUID. The source is only read and remains unchanged.

```console
blockweaver enrich ./data/corpora/11111111-1111-4111-8111-111111111111 \
  --storage-root ./data \
  --corpus-id 22222222-2222-4222-8222-222222222222 \
  --rpc-url "$BLOCKWEAVER_RPC_URL" \
  --verify-rpc-url "$BLOCKWEAVER_VERIFY_RPC_URL"
```

Enrichment is a one-shot operation. It keeps no checkpoints or recovery state and uses the existing atomic publication boundary. Normal verification, loading, and extension reject the seven-column shape.

## Extend

Extension fully validates the source, copies its rows into a new Corpus, and acquires only the suffix. It never mutates, renames, deletes, or hard-links the source.

```console
blockweaver extend ./data/corpora/11111111-1111-4111-8111-111111111111 \
  --storage-root ./data \
  --corpus-id 22222222-2222-4222-8222-222222222222 \
  --last-block 19001999
```

Before publication, both endpoints must agree with the source boundary, the first suffix block must link to it, and the source pair hashes must remain unchanged.

## Verify

Every verification performs full local validation, including exact filenames, JSON shape, Parquet schema, row count, order, domains, and timestamps.

Existing Corpus directories that match the durable eight-column contract below can be verified directly. Verification reads the pair in place and needs no FABLE imports or acquisition code.

```console
blockweaver verify ./data/corpora/11111111-1111-4111-8111-111111111111
blockweaver verify ./data/corpora/11111111-1111-4111-8111-111111111111 --rpc-url "$BLOCKWEAVER_RPC_URL"
blockweaver verify ./data/corpora/11111111-1111-4111-8111-111111111111 --rpc-url "$BLOCKWEAVER_RPC_URL" --full-rpc
```

RPC verification checks deterministic samples and the finalized anchor. `--full-rpc` compares every row.

## Durable contract

A published Corpus contains exactly:

```text
corpus.json
blocks.parquet
```

`corpus.json` has only the request and finalized anchor:

```json
{"finalized_anchor":{"block_hash":"0000000000000000000000000000000000000000000000000000000000000000","block_number":19001000},"request":{"corpus_id":"11111111-1111-4111-8111-111111111111","definition":{"chain_id":1,"first_block":19000000,"last_block":19000999}}}
```

`blocks.parquet` has exactly eight ordered, non-null `Int64` columns:

| Column | Rule |
| --- | --- |
| `block_number` | Contiguous requested range |
| `timestamp` | Nonnegative, nondecreasing seconds |
| `chain_id` | Requested chain |
| `base_fee_per_gas` | Positive wei/gas |
| `gas_used` | Between zero and `gas_limit` |
| `gas_limit` | Positive gas |
| `tx_count` | Nonnegative transaction count |
| `effective_priority_fee_per_gas_p50` | Nonnegative gas-used-weighted P50 among included transactions, in wei/gas |

## Output and trust

Progress is JSON Lines on stderr. One final receipt is JSON on stdout. The receipt records the operation, range, row counts, finalized anchor, SHA-256 hashes for the pair, deterministic sample facts, and verifier facts. It is not stored beside the Corpus.

```console
blockweaver verify ./data/corpora/11111111-1111-4111-8111-111111111111 \
  >receipt.json 2>progress.jsonl
```

Store the receipt in your own audit system. Blockweaver excludes RPC URLs and credentials from durable files and intentional output.

Providers must support archival reads for every requested historical block and the `finalized` tag. Blockweaver proves numbered ancestry from the target to a freshly read finalized block, then rereads the tagged anchor by number. This detects endpoint disagreement and many provider faults; it does not make a provider trustless. Use independently operated endpoints when possible.

Historical reads, finality ancestry, retries, and `--full-rpc` can consume substantial provider quota or incur charges. Estimate the range and provider pricing first. Tune batching and concurrency to provider limits.

Licensed under the [MIT License](LICENSE).
