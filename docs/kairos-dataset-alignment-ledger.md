# KAIROS dataset alignment ledger

## Run

- Status: Blockweaver consolidation Slices 1A–1C are independently green and integrated. Release `v0.3.2` at `9572ad743b56c17e11a313a7ec5ecfc75991f2cd` is published on GitHub and PyPI, and all three additive local/research datasets are prepared and verified. Legacy KAIROS HPO `dfd33e91-702e-46c5-8cb1-3c510af4c048` is closed at 216/216; the KAIROS clean-break loader slice is authorized and active.
- Authoritative spec: this ledger plus the user-approved decisions below.
- Blockweaver clean execution baseline before this authorization update: `39116c8e65090da6dc181ebbd17f69237167c842`, clean `main`, four plan commits ahead of `origin/main`.
- KAIROS dataset-preparation pin: `bfaf9f662b24e9680e60e090e110dac9da51525d`, clean `main`, 17 user-owned commits ahead of `origin/main`; no KAIROS code commit was created by preparation.
- Servatus state: execution/lifecycle extraction is complete, independently green, and synchronized at `2ccf749e2a4c3f5ad7ca572ee34fe78e5b1bb78f` (`v0.4.1`). This plan requires no Servatus code, API, release, or data change.
- KAIROS working tree is clean. The approved `fsevents` allowance is committed at `7cca6fcb`; coherent K-study/HPO figure work is committed at `c0021cb9`; the four discarded epigraph notes are absent.
- Pre-run worktrees: one normal worktree per repository. Slice 1 used `/Users/edo/dev/python/blockweaver-dataset-contract` on `codex/dataset-contract-clean-break`; both the worktree and its integrated branch were removed after the execution record was committed.
- Execution checkout policy: use isolated `codex/` branches and worktrees, one writer at a time. Integrate only after each repository slice is green. Never include protected dirt.
- Current authority: consolidation, publication, and additive dataset preparation are complete. KAIROS code integration, exact legacy-corpus cleanup after its acceptance gate, research image build, `apptainer test`, configuration handoff, and repository push are authorized. Campaign creation, configuration, submission, and GPU smoke remain excluded.

## Confirmed decisions

- Make a clean break. Do not add legacy readers, dual paths, compatibility shims, or transition tests.
- Blockweaver owns blockchain dataset acquisition, materialization provenance, verification, hashing, schema declaration, and immutable publication.
- KAIROS owns scientific interpretation through `CorpusDefinition` and `BlockFrame`, but not a second corpus manifest.
- Servatus continues to own only generic work/submission/publication mechanics. It treats destinations as opaque paths and remains independent of Blockweaver datasets.
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
- Remove every arbitrary constant `"version": 1` tag from private work state and public machine envelopes during the consolidation clean break. Keep the package semantic version, manifest `tool_version`, UUID4 validation, and JSON-RPC's required `"jsonrpc": "2.0"` protocol member because those express release provenance, identifier facts, or external protocol framing rather than Blockweaver contract branching.
- Preserve the five command names and the public `BlockweaverError`, `Dataset`, and `open_dataset` symbols. Make `Dataset` loader-controlled so ordinary direct construction cannot forge a value described as strictly validated; keep its documented immutable properties and runtime type identity.
- Keep the product closed to exactly RPC and BigQuery. Do not add source plugins, filesystem ports, configuration bags, or speculative extension machinery.
- Treat Linux and macOS as the supported writer platforms; Python documents [`fcntl`](https://docs.python.org/3/library/fcntl.html) as Unix-only, and current CI covers those two systems. Remove false Windows writer/configuration branches and add no platform dependency. Genuine Windows durability is a separate future feature requiring locking, directory-sync, atomic publication, and Windows CI; do not imply it here.
- Existing local and research `outputs/corpora/` directories are not deleted by migration. Their later cleanup is authorized only after the active legacy HPO and every old-image consumer have closed, all six new dataset directories pass acceptance, KAIROS's new loader is proven, and a fresh read-only dependency inventory finds no remaining consumer. If any condition is uncertain, defer deletion.
- The same additive dataset conversion is required under the research storage root before any new KAIROS image uses the Blockweaver loader. Old local and research `corpora/` directories remain available to the active legacy image.
- Do not run a GPU smoke for this alignment. Repository tests and, if separately authorized later, `apptainer test` are sufficient image checks.
- Do not start, submit, or configure a new campaign in this run. Stop at a verified code/data/image handoff; campaign ownership remains in the user's other task.

## Architecture consequences

- Add a new Blockweaver dataset-authority ADR that supersedes only ADR 0006's Corpus clause. ADR 0006 remains authoritative for Study, artifact, and evaluation objects; ADR 0008 and the completed Servatus boundary remain unchanged.
- `BlockFrame` becomes an eight-column value. `CorpusDefinition` remains because chain identity and range affect scientific feature construction.
- `STORAGE_ROOT` remains KAIROS's single root. Corpus loading resolves `STORAGE_ROOT/datasets/<corpus_id>`; no second environment variable or repository-specific absolute path is introduced.
- KAIROS takes a runtime dependency on the compatible Blockweaver release and uses only its public artifact interface.
- KAIROS should lose roughly 15–30 production lines by deleting `CorpusRequest`, three corpus address helpers, JSON parsing, and row `chain_id`, net of the thin dataset-to-`BlockFrame` mapping. The main simplification is one metadata authority, not a large LOC reduction.
- Slice 1 remained near LOC-neutral. The consolidation should remove caller knowledge and repeated work, with a directional production-code deletion target of 100–220 lines. Tests should consolidate around public and fake-service behavior. Architecture and preserved behavior decide acceptance.

## Consolidation design

- External flow remains: CLI values and TOML resolve to one trusted request; one of two closed source adapters acquires normalized chunks and proves them; one deep artifact-work module recovers, assembles, validates, and publishes; `open_dataset` independently validates raw artifact bytes.
- `_contract.py` owns raw configuration, feature/range values, and a closed source-complete resolved request. `_sources.py` owns the RPC and BigQuery adapters and their external failures. `_build.py` owns source-neutral download/verification coordination. `_corpus.py` owns artifact identity, strict loading, resumable work, checkpoints, durability, and publication. `cli.py` owns Typer translation, machine I/O, redaction, and signal behavior. Remain at five implementation modules.
- The source seam is real because RPC and BigQuery vary. Replace the reflective 15-field `SourceDefinition`, dotted attribute paths, runner strings, runtime-requirement strings, `_DOWNLOADERS`, and nullable impossible request states with closed typed RPC/BigQuery request variants and two concrete adapters. Do not introduce a public protocol or registry.
- The artifact-work module exposes one narrow one-shot workflow to coordination; it does not expose a caller-driven transaction whose methods can be misordered. Hidden names, locks, binding, checkpoints, candidate states, receipts, fsync order, atomic rename, and cleanup remain implementation details.
- One normalized artifact identity/state derives private binding, manifest/receipt projections, anchor, target, plan, fingerprints, and verification views. The strict reader remains independently byte-driven and must not trust producer objects.
- RPC and BigQuery are true-external dependencies with production and fake adapters. Filesystem work uses real temporary directories and has no abstract adapter. Polars and standard-library computation remain direct in-process dependencies.
- Validate strict raw TOML/CLI/environment inputs once after resolution. Continue to validate every raw provider response, recovered checkpoint, candidate, published artifact, and publication transition. Do not delete checks merely because similar facts cross different trust seams.
- Split implementation into three slices despite shared files: source/provider behavior, filesystem/durable correctness, and public client/platform truth are distinct failure domains. Their final ownership is pinned here to avoid temporary interfaces. Reject the four-module mega-module alternative because it would combine external acquisition and use-case coordination into one roughly 900-line module.

## Gates before implementation

- Blockweaver `CONTRIBUTING.md` requires a GitHub issue before broadening the CLI or durable format. The user authorized issue creation, and [issue #2](https://github.com/edoski/blockweaver/issues/2) is the execution issue for Slice 1.
- Gate satisfied by [issue #3](https://github.com/edoski/blockweaver/issues/3), opened before implementation. Issue #2 remains the completed UUID/unversioned-artifact issue; it does not silently expand to cover this refactor and its deliberate machine-interface changes.
- Legacy HPO `dfd33e91-702e-46c5-8cb1-3c510af4c048` closed normally at 62/62 allocations and 216/216 methods. Its 24 Studies and 456 files were checksum-equal and strict-loaded; manifest-only closure and hidden-scratch removal were verified. It no longer requires the old corpus layout.
- Pin fresh baselines and status immediately before every slice.
- Do not begin a later slice until the current implementation has a committed head and a distinct reviewer returns zero Standards and zero Spec findings.
- Before Slice 1B, inventory local work roots read-only for active `.blockweaver-<uuid>` state. Finish active work on the current binary or obtain explicit abandonment authority; the clean break adds no private-work compatibility reader.
- Public RPC and BigQuery reads were authorized only for the completed migration verification. Repository pushes, exact corpus cleanup, and the documented image build are authorized for the post-HPO handoff. Campaign actions remain unauthorized.

## Slice 1: Blockweaver dataset contract

- Status: complete and integrated at `f676eda38bbc66fb1cac012dd7e9baf0be2135a7`; Standards 0 and Spec 0.
- Repository: Blockweaver.
- Planned baseline: the realignment ledger commit; repin before execution.
- Dependencies: none.

### Scope

- Replace chain/date/UUID directory naming with `ROOT/<uuid>/` and enforce agreement between directory UUID and manifest UUID.
- Replace the old manifest with the one exact unversioned clean-break shape and remove artifact-version tags.
- Promote a small public immutable dataset value/loader from the existing strict local loader.
- Keep `download` and `verify` behavior source-independent and update documentation.
- Consolidate tests around behavior while remaining within five implementation modules and five runtime dependencies including extras.

### Non-goals

- Local import; KAIROS integration; output migration; KAIROS-specific features; v1 compatibility; live provider calls; release or publication.

### Protected behavior

- Exact two-file artifacts; canonical JSON; digest and schema validation; resume binding; redaction; bounded work; finality and verification; atomic no-replace publication; RPC and BigQuery parity.

### Expected outcome

Every newly downloaded dataset has one durable UUID address independent of chain and timestamp spelling, and any Python consumer can open and validate it through one supported Blockweaver API.

### Checks

- Focused CLI/public-reader tests for UUID addressing, manifest binding, both native sources, both formats, invalid artifacts, and no-clobber publication.
- Full Pytest, Ruff lint/format, Pyright, Vulture, lock check, `git diff --check`, CLI smoke, residue search, and module/dependency limits.
- Explicitly not run: public RPC, live BigQuery, KAIROS, real outputs.

## Slice 1A: trusted request and deep source acquisition

- Status: green and integrated on `main` at `78893379edfc0d7750104ee810bddd99cc52e695`; Standards 0 and Spec 0.
- Repository: Blockweaver.
- Baseline: `c4c8da1ee8c95d76ea6444f6ffab6e2b2b1dacc7`.
- Dependencies: completed Slice 1.

### Scope

- Resolve raw CLI overrides, TOML defaults, environment values, range, features, output, and source selection into one trusted source-complete request. Invalid source runtime combinations must be unrepresentable after this seam.
- Replace `SourceDefinition`, reflective field names and paths, runtime-requirement strings, runner strings, `_DOWNLOADERS`, and nullable `primary`/`bigquery` states with a closed RPC-or-BigQuery request union and two concrete adapters.
- Keep discovery, manifest-source facts, acquisition-plan facts, and persisted verification-fact parsing source-aware but dependency-free. Opening a local BigQuery-origin dataset must not import Google libraries, read credentials, or require configuration.
- Make `_build.py` source-neutral: it coordinates one normalized chunk stream and proof result without source switches or source-specific exception blocks.
- Give `_sources.py` locality for external acquisition: RPC/BigQuery initialization, range resolution, source-specific planning, normalized chunks, target/sample/ancestry/finality proof, and recovered-candidate revalidation.
- Allow a BigQuery-only configuration without an unused primary provider; its verifier remains mandatory. RPC continues to require an independent primary and verifier.
- Classify JSON-RPC errors using numeric codes from [JSON-RPC 2.0](https://www.jsonrpc.org/specification) and [EIP-1474](https://eips.ethereum.org/EIPS/eip-1474): known invalid/unsupported errors fail immediately; explicit limit errors split multi-call batches and fail singletons; known transient and unknown server errors remain bounded retries. Never expose provider error text. Preserve original request-ID order through retries and output, concurrency, sibling cancellation, validated and capped `Retry-After`, bounded backoff, and jitter for genuinely transient failures.
- Make acquisition feature-minimal: do not create fee-history work or empty per-block fee dictionaries without selected percentiles; preserve one coalesced fee-history call when selected; require the transaction list for `tx_count` cardinality without regex-validating unused transaction hashes; keep every integrity/hash/domain fact actually consumed.
- Remove repeated provider/project/source validation after the trusted request is constructed. Raw TOML literals, environment values, and direct CLI overrides still validate at their real input seam.

### Non-goals

- Artifact lifecycle or manifest shape changes; public machine-envelope cleanup; platform changes; plugins; a third source; new dependencies; live providers; outputs; KAIROS; release or push.

### Protected behavior

- Exact RPC/BigQuery manifest-source and acquisition-plan documents; lazy optional Google import; no arbitrary SQL; BigQuery schema modes, dry run, byte cap, fork-safe receipt join, bounded pages, and verifier RPC.
- RPC provider independence; strict JSON-RPC IDs/shapes/quantities; chain identity; exact time edges and ranges; target, deterministic samples, ancestry, finality, and full verification.
- Secret exclusion/redaction; invalid configuration, feature, range, or source options failing before network access; same five commands and artifact bytes.

### Expected outcome

The CLI hands acquisition one trusted request. RPC and BigQuery each hide their complete external behavior behind the same small closed seam, while `_build.py` contains no reflective dispatch, impossible source state, or source-specific workflow.

### Checks

- Compact request-resolution matrix for defaults/overrides, RPC/BigQuery completeness, source-inapplicable options, BigQuery-only configuration, environment validation, independence, and redaction.
- Fake RPC tests for fatal/transient/limit/unknown error dispositions, bounded attempts, split behavior, pending-only retries, stable output order, concurrency/cancellation, `Retry-After`, and no provider-message leakage.
- Prove header-only work emits no fee-history call or allocation path; selected percentiles still coalesce; `tx_count` requests no extra RPC method.
- Fake BigQuery proves only required fields/tables, every schema/dry-run/cost gate before execution, bounded pages, optional dependency behavior, and RPC truth proof.
- RPC/BigQuery logical artifact parity and unchanged canonical manifests for both formats.
- Full Pytest, Ruff lint/format, Pyright, Vulture, lock and diff checks, optional BigQuery import, CLI smoke, source/registry residue search, five modules, no new dependency, and focused behavior-level tests.
- Explicitly not run: public RPC, live/billable BigQuery, ADC, real outputs, KAIROS, push, release.

## Slice 1B: artifact lifecycle, identity, and durability

- Status: green and integrated on `main` at `5d53fa6fcbd92c392f44b471bbcff3e4693d9b2f`; Standards 0 and Spec 0.
- Repository: Blockweaver.
- Planned baseline: integrated Slice 1A head; repin immediately before execution.
- Dependencies: Slice 1A.

### Scope

- Deepen `_corpus.py` so one narrow artifact-work interface owns exclusive locking, binding, recovery classification, checkpoint admission/write, candidate assembly, receipt persistence, fsync order, atomic no-replace publication, and cleanup. `_build.py` must not know hidden filenames, checkpoint paths, ready states, or publication ordering.
- Retain one normalized validated artifact state and identity. Derive binding comparison, anchor, target, feature plan, source facts, receipt fields, and pair fingerprints without repeated manifest decoding or manually synchronized field maps.
- Keep `Dataset` exported, flat, immutable, and usable for normal runtime type checks, but make ordinary direct construction fail; `open_dataset(path)` is the only supported constructor and preserves every documented property.
- Carry a bounded metadata-only validated checkpoint set. Admit each recovered checkpoint with one digest plus semantic/proof/prefix/link validation, re-seal it immediately before assembly after long work, assemble once, and retain one independent strict final-candidate scan. Do not retain full frames or all headers in memory.
- Bind provider proof to published bytes: retain externally verified sample facts, strict-open the assembled candidate, and require candidate samples to equal those facts before commit.
- Capture normalized manifest/data fingerprints during strict-open. After any RPC wait during verification or staged-candidate recovery, rehash and compare before success or publication.
- Always live-revalidate an unpublished staged candidate before rename, even when a valid receipt exists. Treat a corrupt or mismatching receipt as absent, revalidate the candidate, and recreate the per-attempt receipt; binding mismatches remain fatal.
- Recover a valid committed destination after partial hidden-work cleanup even when binding/chunks are incomplete. Keep the unavoidable terminal case where publication succeeded but stdout was lost; the pre-emitted UUID remains the recovery handle.
- Correct cross-directory durability: after rename, sync the destination parent and then the hidden source parent; after hidden cleanup, sync the root. Before publication, use `F_FULLFSYNC` for final artifact files on macOS as documented by [Apple's `fsync(2)` manual](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/fsync.2.html), and retain `fsync` on Linux. Keep fail-closed Darwin/Linux no-replace primitives and do not add check-then-rename fallbacks.
- Remove private binding/receipt version tags only after the pre-slice inventory proves no active hidden work needs them. No compatibility parser is added.

### Non-goals

- Changing the published manifest or data schema; public machine-envelope cleanup; source redesign; filesystem abstraction; network-filesystem guarantees; Windows writers; live providers; real outputs; KAIROS; release or push.

### Protected behavior

- Exact UUID/unversioned two-file artifact; strict independent byte-driven loader; canonical JSON; exact file set/schema/order/range/domains; output size/digest; target/anchor/verification facts.
- Exact resume binding; deterministic complete checkpoint prefix; proof/export agreement; corruption and ancestry detection; bounded memory; interruption recovery; no-clobber races; redaction and stable failure codes.
- Producer construction never replaces independent final validation. The cooperative lock does not claim protection from a privileged or noncooperating same-owner process.

### Expected outcome

Filesystem safety has one locality and one small interface. Every durable state is recoverable or safely terminal, provider proof is tied to the candidate bytes actually published, and redundant full checkpoint/artifact reads disappear without merging distinct trust transitions.

### Checks

- State-table tests through the deep interface for empty, incomplete setup, checkpointing, provisional candidate, receipt-only, staged with/without receipt, committed-dirty, and committed states.
- Preserve corruption, gap, duplicate, proof/export, rebinding, ancestry, interruption, racing-destination, ready recovery, published recovery, and RPC/BigQuery recovery parity outcomes.
- Add focused outcomes for partial hidden cleanup, staged live revalidation, corrupt-receipt regeneration, verified-sample/candidate equality, post-network fingerprint sealing, both-parent/root sync ordering, and unsupported publication capability failing before destination mutation.
- Direct `Dataset(...)` fails; `open_dataset` retains every documented property and rejects malformed JSON, UUID/path mismatch, extra files, digest/schema/range/domain/target/verification corruption.
- Record one temporary before/after multi-chunk fresh/resume hash/read measurement. Target: fresh and resumed checkpoints each receive at most admission plus preassembly digest passes, one assembly scan, and one independent candidate scan. Do not retain a brittle call-count or architecture-transition test.
- Full repository gates and limits from Slice 1A, plus filesystem residue checks. Explicitly no NFS/SMB durability claim, Windows writer claim, provider call, output mutation, KAIROS, push, or release.

## Slice 1C: client and platform truth

- Status: green and integrated on `main` at `29dbf93a5973b5a1a981cc8849aadc5bd2c0660c`; Standards 0 and Spec 0.
- Repository: Blockweaver.
- Planned baseline: integrated Slice 1B head; repin immediately before execution.
- Dependencies: Slice 1B.

### Scope

- Remove every arbitrary constant `"version": 1` field from `init`, `chains`, `features`, download/verify receipts, and any remaining private work document. Keep `operation`, package semantic version, manifest `tool_version`, UUID4 validation, and the required JSON-RPC 2.0 protocol member.
- Make the verify interface exact: local verification accepts no RPC-only tuning; `--rpc-url` selects direct RPC; `--provider [--config]` selects a profile; `--rpc-url` may override a named profile; `--full-rpc`, batch size, concurrency, and timeout require a provider or URL; explicit `--config` requires `--provider`. Reject invalid combinations before reading the dataset or contacting a provider.
- Normalize discovery vocabulary once: chains expose `available_sources`; the features document exposes top-level `available_sources`; each feature exposes `supported_sources`; remove repeated per-feature configured-source arrays.
- Normalize the optional-dependency error code to `SOURCE_DEPENDENCY_MISSING` and convert unknown configured default sources to `CONFIG_INVALID` at the configuration seam.
- Keep the generated TOML as the single full example. Shorten README duplication to the `init` workflow, precedence, and a small user-specific override excerpt; add no resource loader or synchronization test.
- Declare Linux and macOS writer support in package metadata and documentation. Remove misleading Windows configuration/publication branches. Add no Windows dependency or mocked support; a real Windows writer requires a separate issue, locking/publication design, and Windows CI.
- Remove remaining proven dead machinery after its owning deep module exists: `format_utc(..., filename=...)`, `_validate_eager_frame(..., resolved)`, `plan_features_for_header`, a still-trivial `dataset_path`, and the one-use receipt-operation parameter. Remove stale names and residue. Do not expose a helper merely to deduplicate a few filesystem lines.

### Non-goals

- New commands/options/providers/formats/dependencies; manifest or dataset-property changes; Windows implementation; compatibility envelopes; live providers; outputs; KAIROS; release or push.

### Protected behavior

- All previously valid meaningful download/verify operations; JSON Lines stderr; stable machine error `code` and `message`; secret redaction; mode-0600 non-overwriting config initialization.
- Exact artifact and manifest; source parity; strict loader; provider and publication safety from Slices 1A–1B.

### Expected outcome

The client-facing interface says exactly what Blockweaver supports: no inert version tags, no ignored options, one discovery vocabulary, one configuration example, consistent error codes, and truthful Linux/macOS writer support.

### Checks

- Exact success/error payloads for all five commands prove version tags absent, operation discriminators retained where useful, discovery names consistent, and secrets absent.
- Table-driven verify option matrix proves invalid combinations fail before disk/network and every supported invocation remains valid.
- BigQuery-only and RPC configurations remain strict; optional-extra error is uppercase; unknown configured sources are configuration failures.
- `blockweaver init` creates loadable authoritative TOML; README contains no full duplicate; platform metadata/docs/workflow agree and false Windows branches are absent.
- Full repository gates and limits. No transition tests for old machine envelopes; no Windows mock; no provider/output/KAIROS/push/release action.

## Slice 2: KAIROS direct dataset consumption

- Status: green and integrated on KAIROS `main` at `6a8f22c2e518b4bc6885b5cc6e3d807333e3053b`; Standards 0 and Spec 0.
- Repository: KAIROS.
- Baseline: `bfaf9f662b24e9680e60e090e110dac9da51525d`; KAIROS `main` was clean and 17 user-owned commits ahead of `origin/main` when this slice began.
- Dependencies: Blockweaver Slices 1 and 1A–1C green with a released or otherwise reproducibly pinned package artifact; Servatus remains pinned at its accepted version and requires no change.

### Scope

- Add the compatible Blockweaver dependency.
- Resolve corpora at `STORAGE_ROOT/datasets/<corpus_id>` through Blockweaver's public loader.
- Derive `CorpusDefinition` from dataset ID, chain ID, and resolved range.
- Require Parquet and the exact KAIROS feature order; create the eight-column `BlockFrame`.
- Delete `CorpusRequest`, `corpus.json` parsing, corpus address helpers, and row `chain_id`.
- Update callers, focused tests, glossary, KAIROS documentation, and add the narrow dataset-authority ADR without changing other durable object contracts or ADR 0008.

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

## Externally authorized dataset preparation gate

- Status: complete on 2026-08-11. All three local and research destinations strict-load with Blockweaver `0.3.2`; the six transferred files are byte-identical; every legacy corpus remains untouched.
- Timing: after the final consolidated Blockweaver head is green and reproducibly pinned, and before KAIROS Slice 2 is integrated or deployed. The active legacy HPO may continue because this gate is strictly additive and leaves every old corpus path untouched.
- Preconditions: compatible final consolidated Blockweaver artifact available; exact local and research source/destination paths resolved read-only; sufficient space on both filesystems; current HPO state checked; no destination collision; no write to a legacy path. The user authorized local/research dataset writes and PublicNode/BigQuery verification reads subject to these checks.
- Migration implementation: temporary repository commit `925d8c4c0370ab65c850c82548c3594eb646118e`, script SHA-256 `7d0aa25bb7de516b296c487e325716abc01f4bef3faeca79103b227eca95fb34`. Its synthetic exact-conversion test, Ruff, Pyright, and diff checks passed. No independent agent review was run in this continuation; acceptance instead revalidated every final artifact through the installed public release locally and remotely. The temporary repository is deleted after this ledger record is pushed.
- Local inputs: the three existing `outputs/corpora/<uuid>/blocks.parquet` files. Research inputs: the matching canonical corpora under the configured research storage root. All old directories remain untouched.
- Destinations: matching local and research `datasets/<uuid>/` directories, published without replacement. Prefer one verified conversion plus checksum-proven transfer when source files agree; never perform two uncorrelated conversions under one UUID.
- Source metadata: PublicNode RPC for Ethereum and Polygon; Google Blockchain Analytics dataset for Avalanche. Generate source/output digests and current verification metadata during migration without a claimed/proven distinction.
- Acceptance: complete. UUID/chain/range agreement, streamed exact eight-column row equality after removing only constant `chain_id`, canonical unversioned manifests, byte/hash/schema/domain checks, provider proof, matching local/research checksums, and installed-release strict loader checks passed for all three. No destination was overwritten and no hidden transfer or verification state remains.
- Not authorized in this gate: deleting old `corpora`, rewriting any of the 213 downstream JSON records, regenerating datasets, modifying other outputs, changing jobs, or publishing packages. Provider verification may read existing source facts but must not rebuild dataset rows.

## Legacy corpus cleanup gate

- Status: conditionally authorized. HPO closure and dataset acceptance are satisfied; KAIROS loader acceptance and the final consumer inventory remain.
- Preconditions: the active legacy HPO is fully closed; no queued/running job, old image, draft, scratch bundle, or automation still resolves an old corpus path; all local and research datasets pass strict acceptance; KAIROS's new loader has passed its full gate; exact cleanup targets are inventoried immediately before deletion.
- Action: delete only the three superseded `outputs/corpora/<uuid>/` directories locally and their three matching canonical research directories. Do not touch any Study, trial, artifact, evaluation, experiment, figure, job, log, scratch, or unrelated output.
- If any precondition is unproven, leave every old corpus directory untouched and report the remaining dependency.

## Deployment and clean-break gate

- Status: authorized after green KAIROS Slice 2 and the final legacy-corpus inventory.
- Integrate the reviewed KAIROS head only after all three datasets strict-load locally and any later user-owned work is resolved.
- Synchronize the accepted KAIROS main change into the existing compact-CUDA branch without altering its reviewed CUDA-only behavior, then independently review that merge before image work.
- Build a new immutable KAIROS image from the exact accepted compact-CUDA SHA through the documented `sbuild` procedure and run `apptainer test`. Do not run a GPU smoke.
- Keep `REMOTE.toml` on the old image until the replacement passes `apptainer test`; then review and publish the one-line image cutover before deleting legacy corpora.
- Preserve the old image and old `corpora/` paths until the legacy cleanup gate passes. Any later campaign using the aligned code must use Servatus, the new image, and `datasets/<uuid>` exclusively, but this run does not start or configure that campaign.
- Do not mix loaders, images, campaign ledgers, or corpus paths within one campaign. No compatibility branch is added.
- Pushes, research configuration changes, and image deployment each require their declared external authority. Campaign creation and launch are excluded from this run.

## Execution ledger

GitHub issue [#2](https://github.com/edoski/blockweaver/issues/2) records the authorized Blockweaver contract change.

- Slice 1 baseline: `38fda515f2f71a104e19461e386176868a1d2d74`.
- Implementer: `/root/dataset_contract_impl`; worktree `/Users/edo/dev/python/blockweaver-dataset-contract`; branch `codex/dataset-contract-clean-break`.
- Initial implementation: `ac256693804579f2632da5019cbc4b3bad8882b4` (`feature(dataset): adopt UUID-only artifact contract`).
- Reviewer: `/root/dataset_contract_review`, with separate Standards and Spec lanes. Initial result: Standards rejected with three findings; Spec green with zero findings.
- Corrections: `f676eda38bbc66fb1cac012dd7e9baf0be2135a7` (`fix(dataset): close contract review findings`) closed stale API documentation, a coupled private test oracle, and the broad output-format type.
- Final review range: `38fda515f2f71a104e19461e386176868a1d2d74..f676eda38bbc66fb1cac012dd7e9baf0be2135a7`; Standards 0, Spec 0, overall green.
- Integration: clean fast-forward to Blockweaver `main`. Main rerun passed 39 tests, Ruff lint/format, Pyright, Vulture, lock check, and diff check. The result has five implementation modules and four runtime dependencies including extras.
- Excluded and untouched: public RPC, live BigQuery, KAIROS, all real outputs, jobs, campaigns, releases, pushes, and PyPI.

- Consolidation design baseline: clean `cd8018212388e4549ed994dc90877c95a6eeda3c`, with 2,983 production lines, five implementation modules, and four runtime dependencies including the optional extra.
- Read-only architecture evidence: `/root/architecture_structure_audit` and `/root/api_validation_audit`; both confirmed the external interface is already small and the cleanup belongs behind it.
- Design-It-Twice lanes: `/root/consolidation_min_interface`, `/root/consolidation_default_caller`, `/root/consolidation_flexible_sources`, and `/root/consolidation_safety_ports`. They compared four-module, caller-first, source-flexible, and safety-first seam placements.
- Specialist lanes: `/root/checkpoint_correctness_research` produced the durable-state/trust model and found six publication/recovery correctness gaps; `/root/validation_platform_research` mapped validation ownership and platform/client cleanup; `/root/consolidation_slice_planner` resolved ordering and review gates.
- Selected result: retain five modules and use three ordered slices. Reject a four-module acquisition mega-module, public plugins, an exposed caller-driven artifact transaction, a filesystem adapter, compatibility code, and partial Windows writers.
- The architecture report was generated outside the repository as an ephemeral research artifact. No product, provider, output, KAIROS, job, campaign, release, push, or PyPI mutation occurred during research.
- Consolidation execution issue: [#3](https://github.com/edoski/blockweaver/issues/3). Pre-run checkout: clean `main` at `edea482ef777a4a5005928e5483fd170d28cdf69`, nine commits ahead of `origin/main`, with only the normal `/Users/edo/dev/python/blockweaver` worktree and no run-owned branches.
- Slice 1A baseline: `c4c8da1ee8c95d76ea6444f6ffab6e2b2b1dacc7`. Implementer: `/root/consolidation_1a_impl`; worktree `/Users/edo/dev/python/blockweaver-slice-1a`; branch `codex/consolidation-slice-1a`.
- Slice 1A implementation head: `b1c2c67de472cdb7c7d1fa9f131a8b572af746e8` (`refactor(source): consolidate acquisition boundary`). Worker reported 51 passing tests plus green Ruff lint/format, Pyright, Vulture, lock, diff, CLI, lazy/optional-import, residue, module, and dependency checks. Orchestrator verified the clean head and nonempty fixed diff before review.
- User correction during Slice 1A: tests are judged by behavior and seam quality. All conflicting source-size language was deleted from repository standards, historical specs, this ledger, and issue #3.
- Slice 1A reviewer: `/root/consolidation_1a_review`, with parallel Standards and Spec lanes over fixed range `c4c8da1ee8c95d76ea6444f6ffab6e2b2b1dacc7...b1c2c67de472cdb7c7d1fa9f131a8b572af746e8`. Initial result: rejected. Standards found one P2 stale testing-size phrase. Spec found four issues: P1 retry IDs lost order through a set; P1 new paired provider calls lacked sibling cancellation; P2 header-only rows still allocated empty fee dictionaries; P2 the same stale phrase contradicted the user correction.
- Slice 1A correction: `7e3e009058ffda0cad58ee320b0d199c0614a5ec` (`fix(source): preserve acquisition invariants`). It removes the remaining stale testing sentence, preserves pending/request order across retries, restores cancellation-and-await for paired provider work, and keeps header-only fee state absent. Worker reported 53 passing tests and every static, lock, import, residue, module, and dependency gate green. Re-review range is `b1c2c67de472cdb7c7d1fa9f131a8b572af746e8...7e3e009058ffda0cad58ee320b0d199c0614a5ec`.
- Slice 1A first correction review: Spec 0, Standards 1. All original functional findings closed. The remaining P2 finding is a correction-introduced private `Header`/`Header.row` monkeypatch and sentinel assertion in `tests/test_cli.py`; replace it with observable CLI/artifact and no-fee-history evidence rather than testing an internal transition.
- Slice 1A second correction: `f30e90737e5250ede10087688f5f3421fe8e596f` (`test(source): keep header proof at boundary`). It removes 19 private-oracle test lines while retaining CLI/artifact output and fake-RPC no-fee-history evidence. No product code changed; worker reported 53 passing tests and all gates green. Re-review range is `7e3e009058ffda0cad58ee320b0d199c0614a5ec...f30e90737e5250ede10087688f5f3421fe8e596f`.
- Slice 1A final re-review: Standards 0, Spec 0, overall green. The last fixed range contained only the 19 test deletions. Integration merge: `78893379edfc0d7750104ee810bddd99cc52e695` (`merge(source): integrate consolidation slice 1a`). Main integration passed 53 tests, Ruff lint/format, Pyright, Vulture, lock, CLI, core/optional imports, diff, residue, module, dependency, and clean-status checks. No provider, output, KAIROS, job, campaign, GPU/image, push, release, or PyPI action occurred.
- Slice 1A cleanup: removed run-owned worktree `/Users/edo/dev/python/blockweaver-slice-1a` and branch `codex/consolidation-slice-1a` after integration. Only the normal `main` worktree remains.
- Slice 1B hidden-work inventory: the platform config path `/Users/edo/Library/Application Support/blockweaver/config.toml` is absent, and a bounded read-only scan under `/Users/edo/dev/python` found no `.blockweaver-<uuid>` directory. No work state needs completion, compatibility, or abandonment before the clean break.
- Slice 1B baseline: `b0edf0a5417c8ff6247d58b3db4a8e731e0005b6`. Implementer: `/root/consolidation_1b_impl`; worktree `/Users/edo/dev/python/blockweaver-slice-1b`; branch `codex/consolidation-slice-1b`.
- Slice 1B implementation head: `3627b583bb6a99ffe19a1799821d9fb239f9e363` (`refactor(artifact): centralize durable lifecycle`). Worker reported 72 passing tests and every static, lock, import, CLI, residue, module, dependency, and diff gate green. Temporary three-chunk measurement reduced fresh/resume checkpoint hashes from `9/10` to `6/6` and semantic reads from `6/7` to `3/3`, while retaining one assembly and one independent strict candidate scan. macOS publication and `F_FULLFSYNC` were exercised; Linux was statically/type checked but not natively run. Orchestrator verified the clean head and nonempty fixed diff before review.
- Slice 1B reviewer: `/root/consolidation_1b_review`, with parallel Standards and Spec lanes over `b0edf0a5417c8ff6247d58b3db4a8e731e0005b6...3627b583bb6a99ffe19a1799821d9fb239f9e363`. Initial result: rejected. Standards found one P3 duplicated source protocol across `_corpus.py` and `_sources.py`. Spec found two P2 issues: staged recovery admitted obsolete checkpoints before validating an already staged candidate, and a same-UUID waiter could acquire a lock on an unlinked hidden-directory generation then fail cleanup with `ENOENT`. Required correction: one lifecycle-owned protocol, staged-first validation, and descriptor/path-generation verification with cleanup only by the owning lock.
- Slice 1B correction: `04bb8c227c916b4ca12739094e7a6a37f924f676` (`fix(artifact): harden staged and concurrent recovery`). It deletes the duplicate source protocol, validates and live-reseals staged candidates before considering obsolete checkpoints, and binds cleanup to the locked directory device/inode generation with retry for stale generations. Worker reported 76 passing tests and all gates green. Four same-UUID subprocess races each produced one valid publication and one `DESTINATION_EXISTS`, never `ENOENT`/`IO_FAILED`, with no hidden residue. Re-review range is `3627b583bb6a99ffe19a1799821d9fb239f9e363...04bb8c227c916b4ca12739094e7a6a37f924f676`.
- Slice 1B first correction review: Spec 0, Standards 1. All original findings closed. The remaining P3 is correction-introduced unsupported-platform generality: `getattr(os, "O_DIRECTORY", 0)` silently weakens directory-only opening. The supported Linux/macOS clean break requires direct `os.O_DIRECTORY` and fail-closed import/runtime behavior.
- Slice 1B second correction: `fcffca52de56f8aaf39da625cd96bdca8a9cfbf2` (`fix(artifact): require directory lock support`). One line now uses `os.O_DIRECTORY` directly with no unsupported-platform fallback. Worker reported the focused staged/concurrent tests, all 76 tests, and every gate green. Re-review range is `04bb8c227c916b4ca12739094e7a6a37f924f676...fcffca52de56f8aaf39da625cd96bdca8a9cfbf2`.
- Slice 1B final re-review: Standards 0, Spec 0, overall green. Integration merge: `5d53fa6fcbd92c392f44b471bbcff3e4693d9b2f` (`merge(artifact): integrate consolidation slice 1b`). Main integration passed all 76 tests plus Ruff lint/format, Pyright, Vulture, lock, CLI, core/optional imports, diff, residue, module, dependency, and clean-status checks. No provider, output, KAIROS, job, campaign, GPU/image, push, release, or PyPI action occurred.
- Slice 1B cleanup: removed run-owned worktree `/Users/edo/dev/python/blockweaver-slice-1b` and branch `codex/consolidation-slice-1b` after integration. Only the normal `main` worktree remains.
- Slice 1C baseline: `b792faebeba76b78f92c5dde73d1d9b387695bcb`. Implementer: `/root/consolidation_1c_impl`; worktree `/Users/edo/dev/python/blockweaver-slice-1c`; branch `codex/consolidation-slice-1c`.
- Slice 1C implementation head: `d4096944b06e44d1680bbff754f09b89700f2a48` (`refactor(client): align CLI and platform truth`). Worker reported 90 passing tests and every static, lock, diff, CLI, import, residue, metadata, module, and dependency gate green. Inert version fields are absent; verify combinations validate before artifact/network access; discovery/error vocabulary is unified; generated TOML is authoritative; README duplication is shortened; Linux/macOS support is declared; false Windows configuration and proven dead arguments are removed. Orchestrator verified the clean head and nonempty fixed diff before review.
- Slice 1C reviewer: `/root/consolidation_1c_review`, with parallel Standards and Spec lanes over `b792faebeba76b78f92c5dde73d1d9b387695bcb...d4096944b06e44d1680bbff754f09b89700f2a48`. Initial result: Standards 0, Spec 1. The sole P3 finding is partial `chains` success-envelope coverage: the product payload is correct, but the test extracts only the nested list and would not reject a reintroduced top-level field. Required correction: compare the complete document with the exact expected envelope.
- Slice 1C correction: `8d8e22fbf5caf6bf7e3aeb3ace82dfce027002bb` (`test(cli): assert complete chains envelope`). The `chains` test now compares the complete parsed document; the other four command envelopes already had exact coverage. No product code changed. Worker reported four focused payload tests, all 90 tests, and every gate green. Re-review range is `d4096944b06e44d1680bbff754f09b89700f2a48...8d8e22fbf5caf6bf7e3aeb3ace82dfce027002bb`.
- Slice 1C final re-review: Standards 0, Spec 0, overall green. Integration merge: `29dbf93a5973b5a1a981cc8849aadc5bd2c0660c` (`merge(client): integrate consolidation slice 1c`). Main integration passed all 90 tests plus Ruff lint/format, Pyright, Vulture, lock, five-command CLI, core/optional imports, metadata, diff, residue, module, dependency, and clean-status checks. No provider, output, KAIROS, job, campaign, GPU/image, push, release, or PyPI action occurred.
- Slice 1C cleanup: removed run-owned worktree `/Users/edo/dev/python/blockweaver-slice-1c` and branch `codex/consolidation-slice-1c` after integration. Final worktree/branch state matches the pre-run state: one normal worktree on `main`, no run-owned branch.

- Release `v0.3.0` published from `f59f5e35311b576c382c472a8c3cf15ce6062d16`; workflow `31493837406` passed. A public-install smoke then exposed the stale hardcoded runtime `__version__` and prevented that release from becoming the handoff pin.
- Release `v0.3.1` published from `a5a02be5cec4b2fb21e6a6b890f97a18d88baa80`; workflow `31494385389` passed and runtime/package metadata agreed. Live Google schema inspection then exposed two fake-boundary gaps before migration: Avalanche transactions/receipts key blocks by `block_hash`, and a dry run must omit an unset `maximum_bytes_billed` field.
- Final release `v0.3.2` published from `9572ad743b56c17e11a313a7ec5ecfc75991f2cd`; workflow `31495305709` passed. The fix uses Google Blockchain Analytics' actual block-hash joins and omits the null dry-run billing field. Live Avalanche schema checks, a full-range dry run of `94,599,003,752` bytes, and five bounded one-block executions passed before release. Public PyPI install reported runtime and metadata version `0.3.2`; CLI help passed.
- Provider samples matched exact legacy rows: Polygon `[83756500, 83888319, 83888320, 83888321, 90582952]`; Avalanche `[75190713, 76425219, 76425220, 76425221, 90987393]`; Ethereum `[23935694, 25101687, 25101688, 25101689, 25590229]`. Target hashes, stored anchor hashes, finalized-head coverage, and full target-to-stored-anchor ancestry passed. Ethereum used PublicNode as the recorded source and dRPC as the archival verifier because PublicNode now requires a personal token for archival `eth_feeHistory`; Avalanche rows used BigQuery and PublicNode verified its target and anchor.
- The first migration attempt failed safely before any write when Polygon proof traffic exhausted retries. Temporary-tool correction `925d8c4c0370ab65c850c82548c3594eb646118e` increased proof batch size and reduced concurrency; the rerun verified every source before creating `outputs/datasets` and then published each candidate atomically without replacement.
- Local dataset hashes: Polygon Parquet `3dd9097bd12f9004d654eef0acb66b216e7d1cd341172e1a30da3f1361cdc3fa`, manifest `e1a5693ec70ef9c3c7355175648920eb466004ca43f20969a76a3a1645fd2c0f`; Avalanche Parquet `2f813186f67ee2f3d16886c5ce061330164786dd563e5a458adbdad410cc9efb`, manifest `e62c3b0735ebf15e216e969d0e2aa0cb8ce6d286b7aad5b7f47b904ba5b7fa43`; Ethereum Parquet `4a8482552e3730ad68243a042eaa92b112450b36411f11a18947e9682d2e62e0`, manifest `512863ef6ad1bd8848a7431f7f4ae9864f9b52276139682ff77326fc298acb0c`.
- Research transfer staged under hidden sibling directories, verified all six hashes, and published through exclusive empty-directory reservations plus same-filesystem atomic rename because the research filesystem returned `EINVAL` for Linux `RENAME_NOREPLACE`. The reservation pattern cannot replace a pre-existing or nonempty destination. Public Blockweaver `0.3.2` strict-opened all three remote datasets in a temporary Python environment using Polars `rtcompat`; the environment and all hidden transfer paths were removed.
- Final preparation audit: local and research dataset copies are byte-identical and contain exactly six files; KAIROS remains clean at `bfaf9f662b24e9680e60e090e110dac9da51525d`; Blockweaver `main` and `origin/main` match the final release head before this ledger-only record; all six old corpus directories and their original hashes remain; the scheduler still reported three running and 28 pending legacy jobs. No KAIROS code, downstream JSON, job, campaign, image, GPU, Servatus, or legacy corpus was changed.

- Post-HPO continuation: source task `019fb1c5-42ec-72c1-ac17-33a8ebe9c8e8` reported legacy HPO `dfd33e91-702e-46c5-8cb1-3c510af4c048` closed at 62/62 allocations and 216/216 methods, with canonical evidence, checksum equality, strict loading, manifest-only closure, and hidden-scratch removal. A fresh local check found only the 24-entry closure manifest; the research scheduler and bounded HPO scratch search were empty.
- Continuation pins: Blockweaver clean `main` and `origin/main` at `19abdfc175682950b9824f786488978f593ab7da`; KAIROS clean `main` at `bfaf9f662b24e9680e60e090e110dac9da51525d`, 17 user-owned commits ahead of `origin/main` at `e96c9f4d0917c35c58f23b3cd43accd61e005d61`. The pre-existing KAIROS branch `codex/compact-cuda-execution` is unrelated and protected.
- KAIROS Slice 2 will use an isolated run-owned `codex/` branch and worktree from the pinned KAIROS baseline. A distinct reviewer must return zero Standards and zero Spec findings before integration.
- Deployment preflight found `REMOTE.toml` still names `/scratch.hpc/edoardo.galli3/deployments/kairos-cuda-f49db0b.sif`, while `codex/compact-cuda-execution` retains the accepted CUDA delta. The safe order is main loader integration, reviewed compact synchronization, exact-SHA image build and `apptainer test`, reviewed configuration cutover, then legacy-corpus deletion.
- KAIROS Slice 2 execution issue: [#149](https://github.com/edoski/kairos/issues/149). Baseline `bfaf9f662b24e9680e60e090e110dac9da51525d`; implementer `/root/kairos_dataset_loader_impl`; worktree `/Users/edo/dev/python/kairos-blockweaver-alignment`; branch `codex/blockweaver-dataset-alignment`.
- Initial implementation `26ab63e52649e29751e2949db87dcbc88dace8df` (`feature(dataset): consume Blockweaver artifacts`) added the pinned public Blockweaver dependency and direct dataset boundary, removed the legacy manifest/address/row-chain path, updated callers and documentation, and added ADR 0009. Reviewer `/root/kairos_dataset_loader_review` rejected four Standards and three Spec findings: duplicate format/schema prechecks, repeated strict opens, generalized fixture/test machinery, and an out-of-scope ADR 0008 edit.
- Correction `6a8f22c2e518b4bc6885b5cc6e3d807333e3053b` (`fix(dataset): deepen consumer boundary`) trusts `open_dataset`, lets `read_parquet` and `BlockFrame` express KAIROS applicability, opens each distinct corpus once per command, removes the generalized rejection tests, simplifies the fixture, and restores ADR 0008 byte-for-byte. Re-review returned Standards 0 and Spec 0.
- KAIROS `main` fast-forward integration passed 109 root tests, 9 mobile-export tests, Ruff lint/format, Pyright, Vulture, both lock checks, and diff check. All three real local datasets opened through KAIROS with exact chain/range/row counts and the ordered eight-column frame: Polygon 6,826,453 rows; Avalanche 15,796,681; Ethereum 1,654,536. No output was changed.

Post-HPO continuation is active. The next work is the KAIROS clean-break loader slice, followed by its consumer inventory, exact legacy-corpus cleanup, immutable image build, `apptainer test`, and verified runner handoff. No campaign is created or launched here.
