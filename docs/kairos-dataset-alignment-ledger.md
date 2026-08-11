# KAIROS dataset alignment ledger

## Run

- Status: plan complete; implementation and output migration are not authorized in this phase.
- Authoritative spec: this ledger plus the user-approved decisions below.
- Blockweaver planning baseline: `c8ecdd17c79633903b79c782c378fd10dc4e1fef`, clean `main`, equal to `origin/main`.
- KAIROS pre-run planning baseline: `f7fcac53069a67b27caee8bb6ee8319fb8c6caa6`, `main` sixteen commits ahead of `origin/main`.
- KAIROS plan-close head: concurrent user-owned work advanced `main` to `3c8af509070ab4fa2c67ac1ebc6cacad7d99e368`, seventeen commits ahead of `origin/main`. This run owns neither commit.
- Protected KAIROS dirt: modified `app/package.json`; untracked `docs/research/thesis-epigraph-candidates.md`, `docs/research/thesis-epigraph-canonical-epics-pass.md`, `docs/research/thesis-epigraph-finalists.md`, and `docs/research/thesis-epigraph-fresh-pass.md`.
- Pre-run worktrees: one normal worktree per repository; no run-owned branch or worktree exists.
- Execution checkout policy: use isolated `codex/` branches and worktrees, one writer at a time. Integrate only after each repository slice is green. Never include protected dirt.
- Planning authorization: this ledger only. Do not change product code, either repository's `outputs/`, external providers, GitHub, releases, or PyPI.

## Confirmed decisions

- Make a clean break. Do not add legacy readers, dual paths, compatibility shims, or transition tests.
- Blockweaver owns blockchain dataset acquisition, materialization provenance, verification, hashing, schema declaration, and immutable publication.
- KAIROS owns scientific interpretation through `CorpusDefinition` and `BlockFrame`, but not a second corpus manifest.
- Blockweaver publishes under the KAIROS storage root at exactly `outputs/datasets/<dataset_id>/manifest.json` plus `blocks.parquet`. Its generic address is `ROOT/<dataset_id>/`; KAIROS supplies `outputs/datasets` as `ROOT`.
- A Blockweaver dataset UUID remains the KAIROS `corpus_id`. Existing Study, artifact, evaluation, and experiment associations remain UUID-based and unchanged.
- KAIROS accepts Parquet only and requires its exact ordered eight-column projection: `block_number`, `timestamp`, `base_fee_per_gas`, `gas_used`, `gas_limit`, `tx_count`, `effective_priority_fee_per_gas_p50`, and `effective_priority_fee_per_gas_p90`.
- Remove row-level `chain_id`; the single-chain `CorpusDefinition` owns it. KAIROS computations already use the definition rather than the row column.
- Remove KAIROS `corpus.json`, `CorpusRequest`, and corpus-address ownership. Promote one small public read-only Blockweaver dataset API so KAIROS does not duplicate manifest validation.
- Do not hardcode a KAIROS feature profile in Blockweaver. KAIROS configuration requests the seven non-key features; `block_number` remains automatic.
- Only `outputs/corpora/` is eligible for migration. Studies, trials, artifacts, evaluations, experiments, figures, and every other KAIROS output remain KAIROS-owned and unchanged.
- Do not regenerate the three existing corpora. Preserve their UUIDs and logical row facts.
- Do not add a permanent import command, import source, migration API, or migration module to either product. Migration is a one-time operation, not product scope.
- File schema, row count, range, timestamps, output digest, target hash, finalized anchor, and verification samples are generated from the files and provider verification during migration.
- Record Ethereum and Polygon acquisition as PublicNode RPC and Avalanche acquisition as BigQuery through `bigquery-public-data.goog_blockchain_avalanche_contract_chain_us`. Do not add an evidence-status distinction. No endpoint URL, credential, billing project, or secret enters the manifest.
- Use one unversioned clean-break durable contract for UUID-only addressing and migrated source metadata. Remove `manifest_version` and `dataset_version`; the loader accepts only the exact current shape and retains no legacy branch.
- Existing `outputs/corpora/` directories are not deleted by migration. Deletion requires separate approval after all new datasets and KAIROS consumers pass acceptance.

## Architecture consequences

- KAIROS ADR 0006 must be superseded: Corpus authority moves from `corpora/<uuid>/corpus.json` to the Blockweaver manifest at `datasets/<uuid>/manifest.json`; all other durable object addresses and authority remain unchanged.
- `BlockFrame` becomes an eight-column value. `CorpusDefinition` remains because chain identity and range affect scientific feature construction.
- `STORAGE_ROOT` remains KAIROS's single root. Corpus loading resolves `STORAGE_ROOT/datasets/<corpus_id>`; no second environment variable or repository-specific absolute path is introduced.
- KAIROS takes a runtime dependency on the compatible Blockweaver release and uses only its public artifact API.
- KAIROS should lose roughly 15–30 production lines by deleting `CorpusRequest`, three corpus address helpers, JSON parsing, and row `chain_id`, net of the thin dataset-to-`BlockFrame` mapping. The main simplification is one metadata authority, not a large LOC reduction.
- Blockweaver should remain near LOC-neutral: UUID addressing and a public reader replace existing private/path logic rather than add another workflow.

## Gates before implementation

- Blockweaver `CONTRIBUTING.md` requires a GitHub issue before broadening the CLI, durable format, or provider model. Issue creation is not authorized in this planning phase. Implementation remains blocked until the user authorizes the issue or explicitly waives that repository rule.
- Pin fresh baselines and status immediately before every slice.
- Do not begin a later slice until the current implementation has a committed head and a distinct reviewer returns zero Standards and zero Spec findings.
- No public RPC, BigQuery, output, GitHub, release, push, or PyPI action is implied by implementation authorization.

## Slice 1: Blockweaver dataset contract

- Status: pending and externally gated by the repository issue requirement.
- Repository: Blockweaver.
- Planned baseline: the ledger commit; repin before execution.
- Dependencies: none.

### Scope

- Replace chain/date/UUID directory naming with `ROOT/<uuid>/` and enforce agreement between directory UUID and manifest UUID.
- Replace the old manifest with the one exact unversioned clean-break shape and remove artifact-version tags.
- Promote a small public immutable dataset value/loader from the existing strict local loader.
- Keep `download` and `verify` behavior source-independent and update documentation.
- Consolidate tests as needed to remain within five implementation modules, five runtime dependencies including extras, and 900 test lines.

### Non-goals

- Local import; KAIROS integration; output migration; KAIROS-specific features; v1 compatibility; live provider calls; release or publication.

### Protected behavior

- Exact two-file artifacts; canonical JSON; digest and schema validation; resume binding; redaction; bounded work; finality and verification; atomic no-replace publication; RPC and BigQuery parity.

### Expected outcome

Every newly downloaded dataset has one durable UUID address independent of chain and timestamp spelling, and any Python consumer can open and validate it through one supported Blockweaver API.

### Checks

- Focused CLI/public-reader tests for UUID addressing, manifest binding, both native sources, both formats, invalid artifacts, and no-clobber publication.
- Full Pytest, Ruff lint/format, Pyright, Vulture, lock check, `git diff --check`, CLI smoke, residue search, module/dependency/test-line limits.
- Explicitly not run: public RPC, live BigQuery, KAIROS, real outputs.

## Slice 2: KAIROS direct dataset consumption

- Status: pending.
- Repository: KAIROS.
- Planned baseline: `3c8af509070ab4fa2c67ac1ebc6cacad7d99e368`; repin before execution and preserve recorded dirt and later user-owned commits.
- Dependencies: Blockweaver Slice 1 green with a released or otherwise reproducibly pinned package artifact.

### Scope

- Add the compatible Blockweaver dependency.
- Resolve corpora at `STORAGE_ROOT/datasets/<corpus_id>` through Blockweaver's public loader.
- Derive `CorpusDefinition` from dataset ID, chain ID, and resolved range.
- Require Parquet and the exact KAIROS feature order; create the eight-column `BlockFrame`.
- Delete `CorpusRequest`, `corpus.json` parsing, corpus address helpers, and row `chain_id`.
- Update callers, focused tests, glossary, KAIROS documentation, and supersede ADR 0006 without changing other durable object contracts.

### Non-goals

- Output migration; compatibility with `outputs/corpora`; changes to Study/artifact/evaluation/experiment schemas; permissive feature supersets; CSV; Blockweaver configuration inside KAIROS.

### Protected behavior

- Existing `corpus_id` associations; temporal geometry and feature formulas; chain-specific feature behavior through `CorpusDefinition`; strict scientific schema; training/evaluation/model semantics; atomic KAIROS object publication; remote execution contract; protected user dirt.

### Expected outcome

KAIROS consumes one verified Blockweaver dataset directly from its own `outputs/datasets/<uuid>` address, while every downstream scientific request continues to identify the same corpus UUID and no KAIROS-owned corpus manifest remains.

### Checks

- Focused corpus tests using temporary Blockweaver datasets; feature/history/model/evaluation tests affected by the eight-column frame; mobile-export and experiment caller tests.
- Full Pytest, Ruff lint/format, Pyright, Vulture, lock check, `git diff --check`, documentation/ADR residue checks, and full repository-prescribed integration gates.
- Prove no production code reads `outputs/corpora`, `corpus.json`, `CorpusRequest`, or row-level `chain_id`.
- Explicitly not run: real outputs, remote/Slurm, GPU, app device, push, release.

## Externally authorized migration gate

- Status: blocked; not part of this planning phase or any implementation slice.
- Preconditions: both slices green; compatible Blockweaver artifact available; KAIROS integration checks green; exact source and destination paths resolved read-only; sufficient disk space; explicit user authorization to modify `outputs/` and contact PublicNode and BigQuery for verification.
- Migration implementation: a temporary script in a temporary git repository. Exercise it against synthetic fixtures and copies, commit it only in that temporary repository, review its fixed diff through the same independent Standards/Spec gate, record its commit and SHA-256, then delete the repository and script after successful migration. Do not commit migration code to Blockweaver or KAIROS and do not rely on conversational memory for destructive logic.
- Inputs: the three existing `outputs/corpora/<uuid>/blocks.parquet` files. The old directories remain untouched.
- Destinations: matching `outputs/datasets/<uuid>/` directories, published without replacement.
- Source metadata: PublicNode RPC for Ethereum and Polygon; Google Blockchain Analytics dataset for Avalanche. Generate source/output digests and current verification metadata during migration without a claimed/proven distinction.
- Acceptance: UUID/chain/range agreement; exact eight-column row equality after removing constant `chain_id`; canonical manifest; byte/hash/schema/domain checks; provider verification sufficient for the normal source contract; KAIROS load smoke for all three; no changes outside the three new dataset directories.
- Not authorized: deleting `outputs/corpora`, rewriting any of the 213 downstream JSON records, regenerating datasets, modifying other outputs, pushing, or publishing packages. Provider verification may read existing source facts but must not rebuild dataset rows.

## Execution ledger

No slice worker, reviewer, branch, worktree, implementation commit, review range, finding, correction, provider call, migration script, or output mutation exists yet.
