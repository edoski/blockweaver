# Generic download implementation ledger

## Run

- Pre-run branch: `main`
- Pre-run head: `ed8ef0bc76e531ffea7303e4397af96521e7e4b5`
- Pre-run status: clean; `main` is three commits ahead of `origin/main`
- Pre-run worktrees: only `/Users/edo/dev/python/blockweaver`
- Run-owned branches/worktrees: none
- Checkout policy: direct work on `main`, one writer at a time
- Baseline checks: 33 tests passed; Ruff lint and format, Pyright, Vulture, and `git diff --check` passed

## Confirmed decisions

- Make a clean break. Do not retain legacy commands, schemas, shims, or transition tests.
- Keep the product scoped to EVM-compatible JSON-RPC chains. Users define named chains and providers; no chain or provider is tied to the repository owner.
- Initial deletion proposal superseded after inspection: the existing BigQuery path is Avalanche-specific but not tied to the repository owner, and it covers a demonstrated RPC-history gap.
- Refactor BigQuery into the generic alternative to RPC: `source = "rpc" | "bigquery"`. Chain configuration supplies an optional dataset identifier; code contains no Avalanche constants or dedicated command.
- Build BigQuery queries only from whitelisted feature definitions and the common Google Blockchain Analytics fields. Reject unavailable features before submitting a billable query. Keep an RPC provider for chain, target, and finality verification. Make the Google dependency optional.
- Keep strict external-boundary validation, bounded concurrency, resumable work, atomic publication, secret redaction, finality checks, and fake-RPC-only tests.
- Use a plain TOML configuration file for settings that persist across downloads. One-off CLI values override configuration.
- Resolve requests from either inclusive block numbers or human-readable UTC date/time bounds.
- Let selected features determine the JSON-RPC methods and parameters used. Make the available feature catalog discoverable.
- Publish one UUID-identified directory named from the chain and resolved start time. Store one data file plus one stable adjacent JSON manifest.
- Support Parquet and CSV. Avoid speculative format plugins.

## Design status

Four alternatives were compared: minimal orchestration, public extension protocols, default-first CLI, and ports/adapters. The chosen design is default-first with closed internal registries and adapters. It keeps the public interface small without blocking local extension.

### Product interface

The clean-break commands are:

```text
blockweaver init
blockweaver chains
blockweaver features
blockweaver download
blockweaver verify
```

`download` and `verify` are the external deep-module interface. The other commands create or inspect configuration and the feature catalog. Remove `acquire`, `acquire-bigquery`, and `extend` completely.

Blockweaver is generic across EVM-compatible chains, not arbitrary blockchain protocols. Chain and provider profiles are data. Source, feature, range, verification, recovery, and publication mechanics stay hidden.

### Configuration

Load one strict TOML file. Resolution order is explicit `--config`, `BLOCKWEAVER_CONFIG`, then the platform user config path. CLI overrides beat the selected chain/provider profiles, which beat global defaults. Reject unknown keys.

Configuration owns:

- default chain, source, providers, output root, format, and features;
- named chains with chain ID, safe finality tag, and optional BigQuery dataset;
- named JSON-RPC providers with exactly one of `url` or `url_env`, plus batch size, concurrency, and timeout;
- BigQuery billing project and mandatory maximum bytes billed.

The generated example uses environment-backed provider URLs and contains no personal chain, project, endpoint, or dataset values. Direct URLs remain possible for public/local endpoints and CLI overrides. All resolved URLs and environment values are secrets for redaction.

### Range contract

Exactly one complete range form is accepted:

```text
--from-block N --to-block N
--from-time VALUE --to-time VALUE
```

Block bounds are inclusive. Time bounds accept ISO 8601 dates or timezone-aware hour, minute, or second values. Normalize offsets to UTC. A reduced-precision start means the beginning of that period; a reduced-precision end means its final second. Resolve to the first block with timestamp at or after the start and the last block with timestamp at or before the end. Reject mixed, reversed, empty, pre-genesis, future, or partly unfinalized ranges; never silently clip them. Record requested and resolved bounds in the manifest.

### Feature contract

`block_number` is always the first row key. Users select every other column. Initial features are:

```text
timestamp
block_hash
parent_hash
base_fee_per_gas
gas_used
gas_limit
tx_count
effective_priority_fee_per_gas_p50
effective_priority_fee_per_gas_p90
```

The closed registry owns each feature's name, ordered output position, Int64 or UTF-8 type, unit, domain rule, source support, hidden dependencies, and acquisition recipe. The planner unions requirements and coalesces work. Header features share block reads; selected priority-fee percentiles share one `eth_feeHistory` call containing only those percentiles. Integrity and range-resolution calls remain mandatory even when their fields are not exported. Unknown or source-incompatible features fail before acquisition or a billable query.

`features` reports type, unit, source support, acquisition family, and configured availability. `chains` reports configured chain profiles without secrets.

### Source contract

`source` is `rpc` or `bigquery`.

- RPC uses a primary and a distinct verifier profile. Both must report the configured chain ID. Preserve bounded retries, batch splitting, concurrency, target agreement, numbered ancestry to the finalized anchor, tagged-anchor reread, and deterministic sample verification.
- BigQuery uses the configured dataset and a generic whitelisted query builder over common Google Blockchain Analytics fields. No raw SQL, dataset constants, or chain-specific top-level commands. Dry-run/schema validation and the hard byte cap happen before the billable query. An RPC provider verifies chain identity, resolved boundaries, target hash, samples, ancestry, and finality.

Keep BigQuery behind the optional `blockweaver[bigquery]` dependency and import it only when selected. Missing support returns a precise source-dependency error.

### Artifact contract

Publish exactly:

```text
ROOT/<chain>-<resolved-start-utc>-<uuid4>/
  manifest.json
  blocks.parquet | blocks.csv
```

Parquet is the default. CSV contains canonical decimal integers and UTF-8 strings; the manifest is its type authority. Both formats represent the same ordered logical schema.

Manifest version 1 is canonical sorted UTF-8 JSON with a final newline. It records dataset/tool versions, UUID, completion time, chain name and verified ID, source and non-secret profile names, requested and resolved range, ordered feature schema and units, normalized acquisition plan, row count, output filename/format/bytes/SHA-256, target hash, finalized anchor, and verification facts. It contains no URLs, credentials, environment values, raw SQL, or transient paths.

Generate a UUID4 when omitted and emit it before acquisition. An explicit UUID resumes only an exact immutable binding of chain, range, features, format, source, and profiles. Operational retry/concurrency values may change. Keep partial state under a hidden work directory. Validate complete deterministic chunks, assemble and fully validate the candidate, fsync files/directories, then atomically rename. Never overwrite a destination.

Progress and errors remain JSON Lines on stderr; success is one JSON receipt on stdout. Add stable error codes while keeping actionable redacted messages.

### Accepted tradeoffs and non-goals

- Parquet stays the typed production default; CSV trades type embedding and efficiency for interoperability.
- Independent verification consumes quota but remains part of the production trust model.
- Time resolution costs logarithmic finalized-chain header reads.
- BigQuery supports only registry features backed by the configured dataset's recognized common schema.
- Do not add public plugins, arbitrary configuration bags, provider SDK adapters, arbitrary SQL, non-EVM drivers, extension semantics, legacy readers, migration commands, or transition tests.
- Do not contact public RPC or BigQuery services in tests or acceptance. Live archival coverage, provider quota, billed-query behavior, and cross-provider production operation remain explicit external gates.
- No GitHub issue is opened: the user directly initiated this approved spec, and external tracker mutation is not authorized.

## Slice ledger

### Slice 1: Generic RPC download product

- Status: green
- Baseline: `888e76c620e2048a5e9d8058e152e159f05a40e6`
- Final head: `b020dcef01e76559d6e416e8f9419f86bed36962`
- Implementer: `/root/slice1_rpc`
- Reviewer: `/root/slice1_review`
- Review result: `GREEN LIGHT` after two correction rounds
- Dependencies: none
- External gates: no public RPC calls

Scope:

- Replace the legacy CLI, fixed request/schema, corpus naming, and extension flow with the configuration, discovery, range, feature-planning, download, manifest, Parquet/CSV, resume, publication, and verification contracts above.
- Implement the RPC source only.
- Delete the dedicated BigQuery implementation and make the core package independent of the Google client until Slice 2.
- Rewrite tests through the CLI, fake JSON-RPC seam, and published artifact. Keep them lean and below the repository limit.
- Update README and security guidance for the new public contract.

Non-goals:

- BigQuery acquisition; public plugins; non-EVM protocols; extension compatibility; live-provider validation.

Protected behavior:

- Strict RPC parsing and signed-Int64 bounds; secret redaction; bounded retries/concurrency and safe sibling cancellation; resume binding; full local artifact validation; target/finality proof; atomic durable publication.

Expected outcome:

Users can configure any EVM chain and independent RPC providers once, discover available features, request an exact block or UTC-time range, download only the chosen feature columns to Parquet or CSV, resume interruption safely, and verify the resulting standard two-file artifact. No legacy command or fixed FABLE schema remains.

Checks:

- Focused fake-RPC CLI tests for config precedence, block/time resolution, call coalescing, unsupported features, both formats, manifest stability, resume, redaction, finality disagreement, and interruption at publication.
- `uv run pytest`
- Ruff lint and format, Pyright, Vulture, `git diff --check`, locked dependency check, residue search for removed commands/schema.

Execution:

- Initial implementation `a44326fd79796182bbd178c02b988ea1144822e7`: 14 tests and all static/lock/residue gates passed. Fixed-range review rejected it with six Standards and six Spec findings, including one duplicate across axes.
- Original findings: noncanonical manifest acceptance; malformed URLs and non-finite timeouts; overwrite race at publication; README/init mismatch; unused parameter; unbounded full RPC verification; unproved time-range edges; checkpoint proof/export divergence; incomplete recovery binding validation; and an early completion timestamp.
- Correction 1 `1bebe8ed0bea80f32fceef567386e6791ba5d2a5`: closed all original findings and passed 30 tests plus all gates. Delta review found three correction-introduced findings: a private-helper publication test, brittle README byte coupling, and rejection of valid Basic-auth RPC URLs.
- Correction 2 `b020dcef01e76559d6e416e8f9419f86bed36962`: moved the race test through the CLI seam, removed Markdown byte coupling, and accepted/redacted valid credentialed RPC URLs. It passed 31 tests plus Ruff lint/format, Pyright, Vulture, `uv lock --check`, and `git diff --check`.
- Final proportional integration: 31 tests passed; CLI exposes only `init`, `chains`, `features`, `download`, and `verify`; legacy/fixed-schema/Avalanche/Google implementation residue search was empty; checkout was clean.
- Final limits: five implementation modules, three core runtime dependencies, 752 test lines.
- Not run: public RPC, archival-provider, quota, or real cross-provider validation.

### Slice 2: Generic BigQuery source

- Status: pending
- Baseline: to be pinned after the Slice 1 ledger update commit
- Implementer: pending
- Reviewer: pending
- Dependencies: Slice 1
- External gates: no public or billable BigQuery query; no public RPC calls

Scope:

- Add the generic BigQuery source behind the same `download` interface.
- Read the configured dataset and billing controls, compile selected features through whitelisted common-field expressions, dry-run before execution, enforce the hard byte cap, stream bounded result pages, and normalize recognized schema differences internally.
- Verify BigQuery output through configured RPC facts and publish the exact Slice 1 artifact contract.
- Move `google-cloud-bigquery` to the optional `bigquery` extra and import it lazily.
- Document source selection, availability limits, installation, billing controls, and verification.

Non-goals:

- Guaranteeing every Google blockchain dataset supports every feature; arbitrary SQL/mappings; provider-specific SDKs; live billing or dataset validation.

Protected behavior:

- Slice 1 CLI/config/artifact contracts; pre-acquisition capability failure; secret redaction; bounded memory; RPC target/finality verification; atomic publication.

Expected outcome:

Users can switch a configured chain from RPC to BigQuery without changing the requested range, features, output format, or artifact consumer. Dataset-specific limits are discovered before billing, and no Avalanche or owner-specific constant remains in code or documentation.

Checks:

- Focused fake-client tests for lazy dependency failure, dataset identifier validation, dry-run and byte cap, selected-field query planning, unsupported feature rejection before execution, streamed acquisition, RPC verification, and artifact parity with RPC.
- Full Slice 1 checks plus `uv lock --check` and BigQuery/Avalanche/personal-value residue searches.
