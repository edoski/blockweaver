# Dataset contract

This page states what a Blockweaver dataset is, what each column holds, and what its checks prove. The README covers installation and commands.

## A dataset is two files in a directory named by its UUID

```text
ROOT/<uuid4>/
  manifest.json
  blocks.parquet | blocks.csv
```

`open_dataset(path)` and `blockweaver verify PATH` accept exactly this shape. The directory name must equal the manifest's `dataset_id`, and no third file may be present. A published directory is never modified.

Rows are one per block, contiguous and ascending. `block_number` is the first column. No value is null.

## The manifest has 14 keys and no version field

The manifest is canonical JSON: sorted keys, compact separators, UTF-8, one trailing newline. A manifest that parses but is not canonical is rejected.

| Key | Meaning |
| --- | --- |
| `tool_version` | The Blockweaver version that wrote the dataset. |
| `dataset_id` | The UUID4 that names the directory. |
| `completed_at` | UTC time at which the data file was assembled. |
| `chain` | `name` and `chain_id` of the configured chain. Every provider reported this chain ID. |
| `source` | `rpc` with the `provider` and `verifier` profile names, or `bigquery` with the `dataset` identifier and the `verifier` profile name. Never a URL, a credential or a billing project. |
| `requested_range` | The block or time range as the user wrote it. A time range also records its normalized UTC bounds. |
| `resolved_range` | First and last block number, and their timestamps. |
| `schema` | The ordered columns with `name`, `type` (`Int64` or `UTF-8`) and `unit`. |
| `acquisition_plan` | How the rows were acquired: the JSON-RPC methods and header keys, or the BigQuery tables and fields. |
| `row_count` | Number of rows. |
| `output` | `filename`, `format`, `bytes` and `sha256` of the data file. |
| `target_hash` | Block hash of the last row. The source and the verifier both reported it. |
| `finalized_anchor` | The verifier's finality head when the dataset was proven: `block_number`, `block_hash` and `tag`. |
| `verification` | Chain IDs seen, `target_agreement`, the `sampled_blocks`, and for BigQuery the `dry_run_bytes`. |

The reader requires `schema` to equal what the feature catalog defines for those column names. It checks only the shape of `acquisition_plan`, because the plan records what an earlier version did. A dataset written by an older Blockweaver therefore keeps opening after the planner changes.

## Typed columns come from a closed catalog

`blockweaver features` prints this catalog. Both sources provide every typed column.

| Column | Type | Unit | Domain |
| --- | --- | --- | --- |
| `block_number` | Int64 | block | always present, contiguous |
| `timestamp` | Int64 | unix_second | nonnegative, nondecreasing |
| `block_hash` | UTF-8 | hex | lowercase 0x-prefixed 32-byte hash |
| `parent_hash` | UTF-8 | hex | lowercase 0x-prefixed 32-byte hash |
| `base_fee_per_gas` | Int64 | wei/gas | positive |
| `gas_used` | Int64 | gas | nonnegative and at most `gas_limit` |
| `gas_limit` | Int64 | gas | positive |
| `tx_count` | Int64 | transaction | nonnegative |
| `effective_priority_fee_per_gas_p50` | Int64 | wei/gas | nonnegative |
| `effective_priority_fee_per_gas_p90` | Int64 | wei/gas | nonnegative |

Every integer must fit a signed 64-bit value. A source value outside its domain fails the download and names the block and the column. When only one of `gas_used` and `gas_limit` is selected, the other is still read so the bound between them can be checked.

Columns hold what the chain's header says. Blockweaver does not reinterpret them per chain. On Avalanche C-Chain, for example, recent headers report `gasUsed` as the sum of the transactions' gas limits, and the `gas_used` column carries that number.

## Raw header columns carry any header key as hex

`header.<key>` selects one `eth_getBlockByNumber` key, for example `header.extraData` or `header.settledExcess`. The key is a letter followed by up to 63 letters or digits.

- The column has the same name, type UTF-8 and unit hex.
- The value is the node's lowercase `0x` hex string. It is never decoded, so a value wider than 64 bits is exact.
- A key whose value is a list, an object or null is rejected.
- Raw columns follow the typed columns, in name order.
- Raw columns come from RPC only. BigQuery refuses them before any network call.
- A key that a block lacks fails the download with `SOURCE_FEATURE_UNAVAILABLE` and the first such block. Split the range at the fork that introduced the key.

Decoding belongs to the consumer. A chain can change what a key means, or move its fee state to new keys, without a Blockweaver release.

## Priority-fee percentiles follow the execution client's rule

For percentile `p`, the value is the effective priority fee of the first transaction, in ascending fee order with ties in transaction order, whose running gas reaches `floor(gas_used * p / 100)`. A block with no transactions has value 0.

From RPC this is one `eth_feeHistory` call per chunk of up to 1,024 blocks. From BigQuery the query applies the same rule to receipts, with each transaction weighted by its receipt gas.

## BigQuery values need a second witness

The BigQuery query is fixed. It reads `blocks`, and `transactions` and `receipts` when a selected column needs them. A block fails the download, instead of receiving a value, in these cases.

- A percentile is selected and the gas of its receipts does not add up to the block's gas used.
- `tx_count` is selected and the block's transaction rows and receipt rows differ in number, or the block reports gas used and has no transaction rows.
- A percentile is selected and the block's base fee or one receipt's price is missing.
- The result does not cover the resolved range exactly.

The first rule makes percentiles unavailable from BigQuery on a chain whose header gas used is not the sum of receipt gas. Avalanche C-Chain is one since its header change of September 2026.

A dry run precedes every billable query and every manifest. It checks the table schemas, the result schema and the byte estimate against `maximum_bytes_billed`.

## What the checks prove

A download publishes only when all of these hold.

1. Every provider reports the configured chain ID.
2. The range ends at or below the finality head of every provider involved.
3. Consecutive rows are hash-linked: each block's parent hash is the previous block's hash.
4. The verifier holds the same hash for the last block as the source.
5. The verifier's finality head is at or past the last block. That head becomes `finalized_anchor`.
6. Up to five deterministic rows match the verifier on every column: the first, the last and three adjacent rows whose position is derived from the dataset UUID.
7. The assembled pair passes the same reader that `open_dataset` uses.

`verify PATH` repeats step 7 offline. `verify PATH --provider NAME` adds steps 1, 4 and 6 against that provider, and requires its finality head to be at or past the recorded anchor and the anchor block to keep its hash. `--full-rpc` compares every row instead of the samples.

These checks cost a fixed number of requests, however far the chain head is from the range. They are operational checks by two independent providers. Blockweaver does not recompute block hashes and does not read the headers between the range and the finality head. Two providers that return the same wrong data are not detected. Rows other than the samples rest on the source and on the hash links, unless `--full-rpc` is run.

## Work directories are private and tied to one version

An interrupted download keeps checkpoints under `ROOT/.blockweaver-<uuid>/`. Repeating the command with the same `--id` resumes it when chain, source, profile names, range, features and format are unchanged. Checkpoint files are an internal format. A work directory started by one Blockweaver version is not guaranteed to resume under another, so finish or delete it before upgrading.

## Error codes are stable

Errors are one JSON line on stderr with `code` and `message`. The message may change between versions. The code does not. During a BigQuery download, a verifier answer outside the protocol or a column's domain is reported as `BIGQUERY_INVALID`, and an unexpected error as `BIGQUERY_FAILED`.

| Code | Meaning |
| --- | --- |
| `CLI_USAGE`, `REQUEST_INVALID`, `RANGE_INVALID`, `FEATURE_INVALID`, `VERIFY_INVALID`, `SOURCE_OPTION_INVALID`, `PROVIDER_INVALID` | The request is malformed. Nothing was contacted. |
| `CONFIG_NOT_FOUND`, `CONFIG_INVALID`, `CONFIG_EXISTS`, `CONFIG_ENV_MISSING` | The configuration cannot be used. |
| `SOURCE_UNAVAILABLE`, `SOURCE_DEPENDENCY_MISSING`, `SOURCE_FEATURE_UNAVAILABLE` | The chosen source is not configured, not installed, or cannot provide a selected column. |
| `RANGE_UNFINALIZED`, `RANGE_PRE_GENESIS`, `RANGE_EMPTY` | The range cannot be resolved on the finalized chain. |
| `RPC_FAILED`, `RPC_INVALID`, `RPC_CHAIN_MISMATCH`, `RPC_MISMATCH` | A provider failed, answered outside the protocol or a column's domain, serves another chain, or disagrees with the data. |
| `BIGQUERY_FAILED`, `BIGQUERY_INVALID`, `BIGQUERY_SCHEMA_INVALID`, `BIGQUERY_COST_LIMIT` | BigQuery failed, returned rows that fail a check, has an unexpected schema, or the dry run exceeds the byte cap. |
| `RESUME_MISMATCH`, `RESUME_INVALID`, `DESTINATION_EXISTS`, `PUBLICATION_FAILED` | The work directory or destination does not allow this run to continue. |
| `ARTIFACT_INVALID` | A dataset fails local validation. |
| `IO_FAILED`, `INTERRUPTED`, `INTERNAL_ERROR` | The filesystem failed, the run was interrupted, or an unexpected error occurred. |
