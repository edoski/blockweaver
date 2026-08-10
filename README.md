# Blockweaver

Blockweaver downloads and verifies immutable, feature-selected EVM block datasets. Chains and JSON-RPC providers are configuration, not code. Each successful request publishes one data file and one canonical manifest.

Python 3.11 or newer is required.

```console
uv tool install blockweaver
blockweaver init
```

`init` writes the platform user config path, or the path selected by `--config` or `BLOCKWEAVER_CONFIG`. It never overwrites a file. The generated TOML uses environment-backed URLs and a local example chain:

```toml
[defaults]
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
```

Configuration is strict: unknown keys, unknown profiles, invalid URLs, ambiguous `url`/`url_env` pairs, and non-finite or greater-than-one-hour timeouts fail before network access. Chain profiles may override `provider` and `verifier`. CLI values override the selected chain and provider profiles, which override global defaults.

Inspect configuration and the closed feature catalog without exposing endpoints. `chains` reports `bigquery` only for chains with a dataset and billing configuration; `features` reports each source supported by the tool and configured for the selected chain.

```console
blockweaver chains
blockweaver features --chain local
```

## Download

Bounds are inclusive. Supply exactly one complete range form:

```console
blockweaver download --from-block 19000000 --to-block 19000999

blockweaver download \
  --from-time 2026-01-01T10:30+01:00 \
  --to-time 2026-01-01T10:45:30+01:00 \
  --feature timestamp \
  --feature block_hash \
  --feature effective_priority_fee_per_gas_p50 \
  --format csv
```

Dates mean the full UTC day. Datetimes accept timezone-aware hour, minute, or second precision; reduced-precision end bounds include the final second of that period. Blockweaver resolves time bounds against the finalized chain and rejects pre-genesis, empty, future, and partly unfinalized requests instead of clipping them.

`block_number` is always the first column. Other columns are selected explicitly or inherited from `defaults.features`. Header features share `eth_getBlockByNumber`; selected priority-fee percentiles share one `eth_feeHistory` request per acquisition chunk with only the requested percentiles.

An omitted `--id` mints a UUID4 and emits it on stderr before acquisition. Reusing an explicit `--id` resumes only an exact binding of chain, requested and resolved range, features, format, source, and provider profile names. Batch size, concurrency, timeout, and endpoint credentials may change between attempts.

The output is exactly:

```text
ROOT/<chain>-<resolved-start-UTC>-<uuid4>/
  manifest.json
  blocks.parquet | blocks.csv
```

Parquet is the typed default. CSV uses canonical decimal integers and UTF-8 strings; `manifest.json` is its type authority. The version-1 manifest records the request, resolved range, ordered schema and units, acquisition plan, chain identity, non-secret provider profile names, finality proof, verification samples, byte size, and SHA-256 digest. JSON is sorted, compact, UTF-8, and newline-terminated.

Work is checkpointed under a hidden directory. Complete chunks are digest-bound and validated before reuse. The fully assembled candidate is validated, synced, and atomically renamed without replacement; existing destinations are never overwritten.

## Verify

Local verification is strict and needs no provider:

```console
blockweaver verify ./downloads/ethereum-20260101T000000Z-11111111-1111-4111-8111-111111111111
```

RPC verification uses a configured profile or a direct URL. It checks deterministic samples and refreshes the stored finality proof. `--full-rpc` checks every row in bounded chunks, including ancestry across chunk boundaries.

```console
blockweaver verify DATASET --provider verifier
blockweaver verify DATASET --rpc-url http://127.0.0.1:8545 --full-rpc
```

Progress and errors are JSON Lines on stderr. Errors include stable `code` and `message` fields. Success is one JSON receipt on stdout. URLs and environment values are excluded from manifests, receipts, and intentional logs.

Providers must implement EVM JSON-RPC batch requests, historical `eth_getBlockByNumber`, the configured `finalized` or `safe` tag, and `eth_feeHistory` when priority-fee features are selected. Independent verification consumes quota. Blockweaver checks provider agreement, numbered ancestry to the tagged anchor, a numbered anchor reread, and deterministic row samples; this is strong operational verification, not a trustless consensus client.

## BigQuery source

Google Blockchain Analytics is an optional acquisition route for history that an RPC provider cannot serve. Install it explicitly; ordinary RPC installs do not include Google libraries:

```console
uv tool install 'blockweaver[bigquery]'
```

Configure a strictly validated `project.dataset` identifier on the chain and one billing project or environment reference. The byte cap is mandatory.

```toml
[chains.my_chain]
chain_id = 12345
finality_tag = "finalized"
bigquery_dataset = "project.dataset"

[bigquery]
project_env = "GOOGLE_CLOUD_PROJECT"
maximum_bytes_billed = 1000000000
```

Select it globally with `defaults.source = "bigquery"` or per request:

```console
blockweaver download --source bigquery --from-block 1000000 --to-block 1000999
```

The same ranges, features, formats, resume state, receipts, and two-file artifacts apply to both sources. Time ranges are resolved against the configured verifier RPC before BigQuery planning. Blockweaver reads the required `blocks`, `transactions`, and `receipts` schemas, rejects unavailable selected features, then performs a dry run and checks its result schema and estimated bytes. Only then does it execute the fixed whitelisted query with `maximum_bytes_billed` enforced again by BigQuery. Results stream through bounded pages into the normal checkpoints.

BigQuery rows are not trusted as chain truth. The verifier RPC checks chain ID, resolved edges, target hash, deterministic row samples, numbered ancestry, and a reread finalized or safe anchor before publication. The manifest records the dataset identifier and verifier profile, never the billing project, environment value, credentials, or SQL text. The configured dataset must expose the recognized common Google Blockchain Analytics schema; arbitrary SQL and field mappings are not supported.

For development:

```console
uv sync --locked --dev
uv run pytest
uv run --extra bigquery python -c 'from google.cloud import bigquery'
uv run ruff check src tests
uv run ruff format --check src tests
uv run pyright
uv run vulture src tests --min-confidence 80
```

Licensed under the [MIT License](LICENSE).
