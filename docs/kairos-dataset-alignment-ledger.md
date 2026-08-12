# KAIROS dataset alignment ledger

## Run

- Status: Blockweaver consolidation Slices 3A–3F are independently green and integrated. Release `v0.3.3` at `e69c02c2d72cc5250834233d3eee9a525e386eb0` is published on GitHub and PyPI. All three additive local/research datasets remain prepared and verified, legacy HPO `dfd33e91-702e-46c5-8cb1-3c510af4c048` is closed at 216/216, and the accepted Servatus 0.6.0 KAIROS handoff is complete. K1 implementation is active, but final acceptance is gated on the Blockweaver CLI hotfix below.
- Authoritative spec: this ledger plus the user-approved decisions below.
- Blockweaver clean execution baseline before this authorization update: `39116c8e65090da6dc181ebbd17f69237167c842`, clean `main`, four plan commits ahead of `origin/main`.
- KAIROS dataset-preparation pin: `bfaf9f662b24e9680e60e090e110dac9da51525d`, clean `main`, 17 user-owned commits ahead of `origin/main`; no KAIROS code commit was created by preparation.
- Servatus state: `v0.6.0` is published from independently accepted head `281c381548489c1dcf7a6ca8d045908d0b50ba3f`. Task `019fea73-abd5-7a51-9681-f0443f647884` completed the reviewed KAIROS adoption and pushed exact accepted refs to both `origin` and `research`: `main` at `56eb5498031239e50a6fd4a5f634c2777257e609` and compact CUDA at `75a36cccc3b04eb9026e3481c2362c2cb6fb1846`.
- KAIROS working tree is clean. The approved `fsevents` allowance is committed at `7cca6fcb`; coherent K-study/HPO figure work is committed at `c0021cb9`; the four discarded epigraph notes are absent.
- Pre-run worktrees: one normal worktree per repository. Slice 1 used `/Users/edo/dev/python/blockweaver-dataset-contract` on `codex/dataset-contract-clean-break`; both the worktree and its integrated branch were removed after the execution record was committed.
- Execution checkout policy: use isolated `codex/` branches and worktrees, one writer at a time. Integrate only after each repository slice is green. Never include protected dirt.
- Current authority: Slices 3A–3F, the compatible Blockweaver patch release, and K1 are authorized through independent implementation/review loops. Completed reviewed code and dataset preparation may then proceed through the authorized combined-image, configuration, and exact cleanup gates after the Servatus handoff. Campaign creation, configuration, submission, and GPU smoke remain excluded.

## Confirmed decisions

- Make a clean break. Do not add legacy readers, dual paths, compatibility shims, or transition tests.
- Blockweaver owns blockchain dataset acquisition, materialization provenance, verification, hashing, schema declaration, and immutable publication.
- KAIROS owns scientific interpretation through `BlockFrame`, but not a second corpus manifest. Slice 2 introduced `CorpusDefinition` as a temporary three-field projection; proposed K1 deletes it and lets metadata callers use Blockweaver `Dataset` facts directly.
- Servatus continues to own only generic work/submission/publication mechanics. It treats destinations as opaque paths and remains independent of Blockweaver datasets.
- Blockweaver publishes under the KAIROS storage root at exactly `outputs/datasets/<dataset_id>/manifest.json` plus `blocks.parquet`. Its generic address is `ROOT/<dataset_id>/`; KAIROS supplies `outputs/datasets` as `ROOT`.
- A Blockweaver dataset UUID remains the KAIROS `corpus_id`. Existing Study, artifact, evaluation, and experiment associations remain UUID-based and unchanged.
- KAIROS accepts Parquet only and requires its exact ordered eight-column projection: `block_number`, `timestamp`, `base_fee_per_gas`, `gas_used`, `gas_limit`, `tx_count`, `effective_priority_fee_per_gas_p50`, and `effective_priority_fee_per_gas_p90`.
- Remove row-level `chain_id`; after proposed K1, each single-chain `BlockFrame` retains one chain-ID scalar while its first/last extent derives from its actual rows.
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
- `BlockFrame` is an eight-column single-chain value. Proposed K1 removes `CorpusDefinition`: the frame retains chain identity and derives its current range from its rows, while metadata-only callers read generic range facts from Blockweaver `Dataset`.
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

## Post-integration consolidation audit

- Status: read-only audit complete at clean Blockweaver `4fd70dcebcc8c29a0c9a5c168eccb35704948b06` and accepted KAIROS dataset head `6a8f22c2e518b4bc6885b5cc6e3d807333e3053b`. No product, test, provider, output, remote, scheduler, image, or campaign state changed.
- Purpose: consolidate only demonstrated duplicate ownership, durable shadow state, repeated external work, and layered tests. Complexity does not qualify merely because it is low-level or large.
- Lanes: `/root/bw_structure_audit`, `/root/bw_validation_audit`, `/root/bw_sources_audit`, `/root/bw_artifact_audit`, `/root/bw_cli_config_audit`, `/root/kairos_corpus_ownership_audit`, `/root/kairos_corpus_test_audit`, `/root/bw_kairos_interface_audit`, and `/root/bw_kairos_data_path_audit`. `/root/rejected_findings_adjudication` then challenged every rejection from first principles, confirmed K1, rescued `_build.py` deletion as 3F, kept two items measurement-gated, and upheld the remaining rejections.
- Shared conclusion: keep the public five-command plus `BlockweaverError`/`Dataset`/`open_dataset` interface and the real RPC/BigQuery `ArtifactSource` seam. Proposed Slice 3F tests whether the shallow `_build.py` pass-through can disappear, reducing five implementation modules to four without creating the previously rejected source mega-module. The Blockweaver–KAIROS production seam is otherwise already narrow; no KAIROS feature profile, scientific frame, temporal logic, or model meaning moves into Blockweaver.
- Execution gate: [issue #4](https://github.com/edoski/blockweaver/issues/4) records Slices 3A–3F and the release gate. Repin clean `main`, inventory active hidden work before 3C, and use the implementation/review loop. Before K1, wait for the separate Servatus task to finish and repin both KAIROS branches. User approval for every proposed slice and release gate was recorded on 2026-08-12.

## Proposed Slice 3A: trusted configuration and validation ownership

- Status: green and integrated on `main` at `293d738fd0b81af2d506c1c940691575fa7aaea8`; Standards 0 and Spec 0.
- Repository: Blockweaver.
- Planned baseline: repin clean Blockweaver `main`; audit evidence was gathered at `4fd70dcebcc8c29a0c9a5c168eccb35704948b06`.
- Dependencies: completed Slices 1A–1C. No external provider or output gate.

### Scope

- Remove unused `ProviderSpec.name` and `Config.path` state.
- Let resolved `Provider` construction own URL, batch-size, concurrency, and timeout validation exactly once; `Config.provider` only applies precedence and constructs it.
- Compile and retain the validated default feature `Plan` at the TOML seam; plan only explicit CLI feature overrides later.
- Use `None`, not truthiness, for chain/provider/verifier/RPC-URL precedence so explicit empty overrides fail instead of silently selecting defaults.
- Make invalid TOML default features consistently `CONFIG_INVALID`; explicit CLI feature failures remain `FEATURE_INVALID`.
- Delete the unreachable lowercase check on an already normalized `UUID`; retain UUID4 validation and strict canonical UUID-string validation in `open_dataset`.
- Delete the duplicate private `verify_dataset` option guard, the consecutive public manifest-source dictionary precheck, and the staged-candidate chain comparison already guaranteed by immutable request binding. Standalone live verification retains its independent RPC chain check.

### Non-goals

- Relaxing raw TOML, environment, CLI, provider-response, manifest, UUID4, redaction, or live-chain validation; changing valid precedence; adding config objects or public helpers; changing machine envelopes.

### Protected behavior

- Strict failure before I/O; named provider and verifier independence; secret exclusion; exact error codes by input seam; BigQuery-only configuration; all five command payloads; strict public artifact loading.

### Expected outcome

Every raw value is validated once at its owning trust seam, trusted internal values are not reparsed, and explicit malformed overrides cannot masquerade as omission. The public client remains unchanged and the private configuration path becomes smaller.

### Checks

- Extend existing table-driven request-resolution tests with explicit empty overrides and invalid default features; add no new test family.
- Retain invalid config/environment/direct-provider, redaction, verify-mode, UUID4, and corrupt-manifest behavior.
- Full Pytest, Ruff lint/format, Pyright, Vulture, lock, diff, CLI/import, residue, module, dependency, and clean-status gates.
- Explicitly not run: provider calls, outputs, KAIROS, release, push, image, or campaign.

## Proposed Slice 3B: source planning and range efficiency

- Status: green and integrated on `main` at `5b79246e5cc2571beacb5184763b16282d16653a`; Standards 0 and Spec 0.
- Repository: Blockweaver.
- Dependencies: green Slice 3A.

### Scope

- Give `Plan` one private typed BigQuery requirements projection. Canonical acquisition-plan serialization and whitelisted SQL compilation consume it instead of independently deriving table/field dependencies.
- For BigQuery date/time ranges, read verifier boundaries once rather than pass the same verifier as two supposedly independent endpoints. RPC date/time ranges retain real primary/verifier agreement.
- Fetch each unique block boundary once, including a single-block range whose start equals end.
- Treat successful target agreement as control flow: make finality proof return the anchor, emit `target_agreement: true` after success, use frozen `Header` equality directly, and delete `_same_header`. Retain `_same_core`, which intentionally ignores selected feature values.

### Non-goals

- Changing canonical manifests or selected SQL meaning; a generic source registry; relaxing schema-mode, dry-run, byte-cap, fork-safe join, pagination, target, sample, ancestry, finality, cancellation, retry, ordering, or redaction behavior.

### Protected behavior

- Exact RPC/BigQuery parity; independent RPC providers; BigQuery verifier RPC; exact time edges; stable JSON-RPC ordering and bounded work; minimal selected fields/tables; lazy optional Google dependency.

### Expected outcome

Feature requirements have one owner and range/finality work performs no duplicate call against the same endpoint. Observable artifacts and errors remain identical while provider latency and quota use fall.

### Checks

- Existing exact manifest, header-minimal SQL, fork-safe receipt, schema/cost, provider-independence, target/finality, and time-range tests remain the interface proof.
- Add only focused fake-boundary evidence that BigQuery time resolution and single-block ranges do not repeat identical calls; avoid private call-graph tests.
- Full repository gates from 3A plus optional-BigQuery import and source residue checks. No live/billable call.

## Proposed Slice 3C: delete durable receipt shadow state

- Status: green and integrated on `main` at `e05cae16719bf71cb8d69a82d8b7378f51e77a5c`; Standards 0 and Spec 0.
- Repository: Blockweaver.
- Dependencies: green Slice 3B; fresh hidden-work inventory immediately before implementation.

### Scope

- Delete private durable `receipt.json`, its exact-key schema, recovery field, matching/regeneration branches, and staged persistence.
- Keep the exact public download success receipt and `_download_receipt` as the sole receipt owner.
- Define counts for the successful invocation: checkpoint rows are reused and newly fetched rows acquired; staged or committed recovery reports every row reused and zero acquired.
- Simplify the durable state machine from `chunks -> ready + receipt -> destination + receipt -> cleanup` to `chunks -> ready -> destination -> cleanup`.
- Delete the separate publication-capability preflight only if fixed-head implementation proves `_rename_no_replace` still fails before destination mutation and leaves valid staged recovery. Otherwise retain it.

### Non-goals

- Changing stdout receipt fields; preserving diagnostic counts from a previous crashed invocation; deleting immutable `binding.json`; weakening staged live revalidation, committed recovery, locks, fingerprints, fsync, no-replace publication, or cleanup ownership.

### Protected behavior

- Fresh and partial-resume counts; staged/committed recovery; exact binding; generation-safe concurrency; destination no-clobber; interruption recovery; stable machine result envelope; unavoidable terminal case where publication succeeds but stdout is lost.

### Expected outcome

The durable filesystem contains only state needed to resume or prove publication. A receipt is an invocation result, not a second durable projection of the already validated dataset.

### Checks

- Preserve fresh and partial-checkpoint count tests; staged/committed recovery must report `rows/0`.
- Delete receipt-only and receipt-corruption transition tests. Retain staged live validation, committed-dirty recovery, binding rejection, race, interruption, sync-order, and stdout-envelope tests.
- Full artifact/state matrix and repository gates. No real output or provider call.

## Proposed Slice 3D: proof pipeline deepening

- Status: green and integrated on `main` at `5f4fd8967f484d3702bd3f329a6bbbb08bab56c3`; Standards 0 and Spec 0.
- Repository: Blockweaver.
- Dependencies: green Slice 3C.

### Scope

- Keep early target agreement, anchor/finality/ancestry proof, and deterministic sampled-block numbers because the canonical manifest requires them before strict candidate opening.
- Remove early sampled-row RPC verification, retained checkpoint sample facts, `FactReader`, `VerifiedProof.samples`, and the candidate-versus-retained-samples comparison.
- Let the existing final candidate revalidation perform the only sampled-row comparison directly against strict-opened candidate bytes, followed by fingerprint resealing across the network wait.
- Keep full staged-candidate revalidation unchanged.

### Non-goals

- Assemble-first provisional manifests; a weaker candidate reader; collapsing early target/finality proof; removing final target, anchor, ancestry, finality, samples, checkpoint resealing, strict candidate open, or post-network fingerprint checks.

### Protected behavior

- Canonical manifest construction; proof-to-published-byte binding; fresh and staged sample mismatch detection; checkpoint mutation detection; provider movement during assembly; mutation during RPC waits; RPC/BigQuery parity.

### Expected outcome

Each fresh download proves sampled rows once against the exact candidate bytes that may be published, while target/finality facts still exist in time to build and independently open the manifest.

### Checks

- Retarget the existing retained-sample test to final candidate sample disagreement; preserve target/sample/ancestry/finality rejection, staged recovery, and post-network mutation cases.
- Fake-provider evidence may show one sampled verification without binding tests to private methods.
- Full repository gates; no public providers or outputs.

## Proposed Slice 3E: checkpoint and row-domain pass consolidation

- Status: green and integrated on `main` at `776f20226751b71720134aa33b69bfecd8a0fbee`; Standards 0 and Spec 0.
- Repository: Blockweaver.
- Dependencies: green Slice 3D.

### Scope

- For newly created checkpoints, construct the trusted `Checkpoint` metadata from already parsed source headers/rows after durable write and digest. Do not immediately reopen it through the recovered-byte admission path.
- Keep `_read_checkpoint` unchanged for recovered bytes and keep preassembly digest resealing for every checkpoint.
- Give selected-row numeric/gas domain validation one private owner. Remove only schema/null/contiguity/hash/timestamp work already proved by surrounding checkpoint checks in the same trust transition.
- Preserve independent full candidate validation and public `open_dataset` validation.

### Non-goals

- Removing source response parsing, checkpoint hashes, recovery admission, proof/export equality, link validation, prefix coverage, resealing, candidate assembly, or strict public validation; creating a filesystem adapter or cache.

### Protected behavior

- Recovered checkpoint corruption/gap/duplicate/link rejection; cross-chunk ancestry; exact ranges; CSV/Parquet parity; row domains; target hash; bounded memory; one assembly plus one independent candidate scan.

### Expected outcome

Fresh trusted data is not treated as recovered untrusted bytes immediately after Blockweaver writes it, and each row-domain rule has one owner without merging distinct recovery, candidate, and public trust seams.

### Checks

- Existing checkpoint digest/proof/prefix/link, artifact corruption, row-domain, CSV token, Parquet schema, and final-candidate tests remain observable proof.
- Record a temporary fresh/resume read/pass measurement and remove the measurement code before commit; add no brittle call-count test.
- Full repository and filesystem residue gates. No NFS/SMB, Windows, provider, or output claim.

## Proposed Slice 3F: remove shallow build orchestration

- Status: green and integrated on `main` at `95fd7e122c01fc608d0e02d1431a47c620a27a4d`; Standards 0 and Spec 0.
- Repository: Blockweaver.
- Dependencies: green Slice 3E.

### Scope

- Delete `_build.py`, which currently forwards source acquisition into artifact materialization and wraps local/optional-RPC verification without owning a distinct policy or trust seam.
- Keep the private `ArtifactSource` protocol and both concrete RPC/BigQuery adapters.
- Put download composition behind one private source-facing entry point that acquires the concrete adapter and invokes `materialize_artifact` with the existing progress/publication callbacks and tool version.
- Put local plus optional-RPC verification orchestration beside its actual strict-dataset and RPC owners without exposing a new public interface or teaching Typer source internals.
- Remove wrapper imports and any tests coupled only to `_build` names. Accept the slice only if the final diff removes one module and is net simpler; do not replace it with another forwarding module.

### Non-goals

- Merging `_sources` and `_corpus`; removing `ArtifactSource`; adding source switches to artifact lifecycle; making CLI own provider algorithms, durable state, or manifest construction; changing command payloads or signal/publication semantics.

### Protected behavior

- Source-neutral artifact lifecycle; two concrete adapters; exact five commands; progress JSONL; SIGINT and committed-output behavior; local/sample/full verification; post-RPC fingerprint sealing; stable errors and redaction.

### Expected outcome

Blockweaver has four deep implementation modules rather than five modules with a shallow coordination hop. Acquisition, artifact work, and CLI translation retain distinct owners even though the pass-through file disappears.

### Checks

- Existing five-command, download, verify-option, source-parity, interruption, publication, and redaction tests remain the interface proof.
- Residue proves `_build.py` and wrapper-only imports/tests are absent while `ArtifactSource`, `RpcSource`, and `BigQuerySource` remain.
- Full repository gates plus a before/after production/module/interface inventory. No provider, output, KAIROS, release, push, image, or campaign.

## Proposed Blockweaver release gate

- Status: complete. Release `v0.3.3` at `e69c02c2d72cc5250834233d3eee9a525e386eb0` is published and publicly verified.
- Independently verify the integrated Blockweaver head, choose the next compatible patch release, update package metadata/changelog only as required, push/tag/release through trusted publishing, verify GitHub/PyPI artifacts and hashes, and install from the public index in an isolated environment.
- The release must retain the same public Python and five-command interface. Private hidden-work compatibility is not added; the fresh pre-3C inventory must prove no active work was abandoned.
- KAIROS K1 and the final image must pin the new published patch if the Blockweaver consolidation is intended to ship. Do not build an image that still installs `blockweaver==0.3.2` after Slices 3A–3F are accepted.

### Blockweaver CLI compatibility hotfix

- Status: complete. Blockweaver `v0.3.4` is published from exact reviewed head `045ff7dc8dcfce01bd7b472d0c1c618cc71a1b74`; K1 may take its final public pin and proceed to review.
- K1 integration found that public `blockweaver==0.3.3` imports `ClickException` from Typer's private `typer._click` module. KAIROS's locked public Typer 0.24.1 removes that module, so `blockweaver --help` fails before command registration.
- Scope: replace the private Typer import with the supported public exception boundary, prove all five command helps and stable JSONL usage failures against the supported Typer range including 0.24.1, and publish the next patch through the same independent implementation/review and trusted-publishing gate.
- Non-goals: no KAIROS workaround, compatibility shim, CLI redesign, dependency widening, or unrelated Blockweaver change.
- Expected outcome: Blockweaver's documented five-command CLI imports and behaves correctly in KAIROS's locked environment without relying on Typer internals. K1 pins the reviewed public hotfix before acceptance.

## Proposed Slice K1: remove duplicated corpus metadata and layered fixtures

- Status: complete. K1 is integrated on KAIROS `main` at `c95ffa1c5a8fb11191524749d88e6ee6ee794570`; its reviewed compact-CUDA synchronization is `352cc968853bda69b1a9131db709d48d8f1d2043`. The later reviewed image cutover advances published main to `ff2a9e26ba67a7b4b58cc9e389a4eb4e81ff7b95` and compact-CUDA to `45f27ef9ce345c59e4c469522e2aa184f613a9e6` on both remotes.
- Repository: KAIROS.
- Baselines: KAIROS `main` `56eb5498031239e50a6fd4a5f634c2777257e609`; protected compact-CUDA `75a36cccc3b04eb9026e3481c2362c2cb6fb1846`.
- Dependencies: green Blockweaver Slice 3F, a reproducibly published compatible Blockweaver patch, and the separate Servatus KAIROS task. K1 updates the exact Blockweaver pin so the final image contains the accepted consolidation.
- Coordination update: the separate task delivered published Servatus 0.6.0 and exact accepted KAIROS refs on 2026-08-12. K1 may proceed. Image, configuration, and corpus cleanup remain sequenced after green K1 integration and compact synchronization.

### Scope

- Delete `CorpusDefinition`, `_definition`, `load_corpus_definition`, and `BlockFrame.definition`.
- Expose the existing KAIROS address adapter as `open_corpus_dataset(...) -> blockweaver.Dataset` for metadata-only callers. Held-out preparation caches dataset `last_block`; mobile export caches dataset `chain_id` once per distinct UUID.
- Let `BlockFrame` store only `chain_id`, derive `first_block` and `last_block` from its actual rows, and preserve those truthful extents through `select_range`.
- Reject an empty `BlockFrame`, which has no derivable scientific extent. Do not add contiguity or domain rescans: root frames trust Blockweaver and subframes arise by slicing.
- Reject artifact/corpus association mismatches before hydrating a multi-million-row corpus in evaluation.
- Delete the KAIROS test helper that manually authors Blockweaver manifests/acquisition plans/digests. Keep one narrow valid consumer-contract test with a small `open_dataset` stand-in plus real Parquet data; higher-level scientific tests inject their existing `BlockFrame` values through KAIROS loader seams.
- Update glossary, ADR 0009, active Servatus/Blockweaver wording, and tests. Preserve current UUID associations and output schemas.

### Non-goals

- Storing `Dataset` inside `BlockFrame`; moving KAIROS schema/features/temporal/model semantics into Blockweaver; adding `Dataset.read`, `scan`, `to_blockframe`, `require_parquet`, `expect_schema`, unchecked metadata opening, global caching, compatibility paths, or output migration.

### Protected behavior

- Exact eight-column scientific schema; chain-specific feature behavior; inclusive range selection; frame isolation; temporal geometry; training/evaluation/model outcomes; metadata-only callers avoid row hydration; one valid Blockweaver consumer-contract test; existing dataset UUID associations.

### Expected outcome

Generic chain and range facts come directly from Blockweaver at the artifact seam, while each scientific `BlockFrame` truthfully owns only its current rows and chain identity. KAIROS loses one wrapper concept, one impossible frame/definition mismatch state, and its tests stop impersonating Blockweaver.

### Checks

- Focused nonempty/schema/range/isolation `BlockFrame` behavior; corpus address and dataset-metadata mapping; held-out/mobile once-per-UUID loading; evaluation mismatch before corpus load; feature/history/model/evaluation behavior.
- Prove no `CorpusDefinition`, `load_corpus_definition`, handwritten Blockweaver manifest fixture, legacy corpus path, row-level `chain_id`, or duplicate Blockweaver validation remains.
- Full root, CUDA-focused, mobile-export, App, Ruff, Pyright, Vulture, both locks, diff, documentation/ADR, topology/parity, and clean-status gates on `main`, then reviewed compact synchronization.
- Explicitly not run: outputs, remote, Slurm, image, GPU, campaign, or corpus deletion.

## Rejected and deferred audit proposals

- Keep the current public Blockweaver dataset interface. `Dataset.scan()`, `read()`, callback projection, Arrow/Pandas adapters, `open_dataset(root, id)`, `expect_schema`, `require_parquet`, and `Dataset.to_blockframe()` add public machinery for one caller and do not remove meaningful code.
- Do not store a Blockweaver `Dataset` in `BlockFrame`. A selected subrange would retain the root dataset extent; separate view bounds would recreate `CorpusDefinition` under another name and carry irrelevant provenance/path state.
- Keep separate metadata and row-loading KAIROS functions. One eager loader would hydrate up to roughly 15.8 million rows for held-out/mobile metadata; a mode flag would be a shallower interface.
- Keep `BlockFrame`'s exact schema, inclusive range, and isolation invariants, and add the nonempty extent invariant in K1 when range becomes row-derived. They express KAIROS scientific applicability and internally constructed values, not duplicate artifact validation.
- Do not add a global or cross-process dataset cache, unchecked metadata loader, validation sidecar, or frame stored in `Dataset`. They weaken independent validation or retain roughly 0.1–1.0 GiB per corpus without useful reuse across Servatus processes.
- A future `Dataset.read_range()` is performance work, not cleanup. Consider it only after peak-RSS and warm-cache measurements prove material Polygon/Avalanche benefit and exact context/predecessor/horizon geometry remains KAIROS-owned.
- Keep the two-adapter `ArtifactSource` seam, RPC batching/retry/cancellation, BigQuery schema/dry-run/cost/page/hash-join checks, strict public `open_dataset`, candidate fingerprints, generation-safe locking, immutable binding, checkpoint resealing, CSV token validation, Parquet schema validation, fsync ordering, and atomic no-replace publication. `_build.py` is separately rescued as proposed Slice 3F because it is a shallow pass-through, not the adapter seam itself.
- Reject the full assemble-first/single-proof rewrite. The canonical manifest needs anchor and verification facts before strict candidate opening; a provisional manifest or weaker reader would add machinery and weaken the trust transition. Slice 3D accepts only the narrower sampled-row deduplication.
- Do not retype `ArtifactIdentity` solely to avoid its small source-kind validation unless implementation of 3A demonstrates a real net deletion without another synchronized projection.
- Moving `CorpusDefinition` beside `BlockFrame` as a dataclass is superseded by K1's cleaner deletion. A KAIROS feature profile or scientific schema in Blockweaver remains rejected.

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

- Status: complete. HPO closure, KAIROS loader acceptance, local/research Blockweaver 0.3.4 strict opening, byte equality, and the final no-consumer inventory passed. The three exact legacy UUID directories were removed locally and on research; the local copies are recoverable from Trash.
- Preconditions: the active legacy HPO is fully closed; no queued/running job, old image, draft, scratch bundle, or automation still resolves an old corpus path; all local and research datasets pass strict acceptance; KAIROS's new loader has passed its full gate; exact cleanup targets are inventoried immediately before deletion.
- Action: delete only the three superseded `outputs/corpora/<uuid>/` directories locally and their three matching canonical research directories. Do not touch any Study, trial, artifact, evaluation, experiment, figure, job, log, scratch, or unrelated output.
- If any precondition is unproven, leave every old corpus directory untouched and report the remaining dependency.

## Deployment and clean-break gate

- Status: complete. The reviewed image, main/compact configuration cutover, ref publication, dataset acceptance, and exact legacy cleanup all passed. The old deployment image remains preserved.
- Integrate the reviewed KAIROS head only after all three datasets strict-load locally and any later user-owned work is resolved.
- Synchronize the accepted KAIROS main change into the existing compact-CUDA branch without altering its reviewed CUDA-only behavior, then independently review that merge before image work.
- Build a new immutable KAIROS image from the exact accepted compact-CUDA SHA through the documented `sbuild` procedure and run `apptainer test`. Do not run a GPU smoke.
- Keep `REMOTE.toml` on the old image until the replacement passes `apptainer test`; then review and publish the one-line image cutover before deleting legacy corpora.
- Preserve the old image and old `corpora/` paths until the legacy cleanup gate passes. Any later campaign using the aligned code must use Servatus, the new image, and `datasets/<uuid>` exclusively, but this run does not start or configure that campaign.
- Do not mix loaders, images, campaign ledgers, or corpus paths within one campaign. No compatibility branch is added.
- Pushes, research configuration changes, and image deployment each require their declared external authority. Campaign creation and launch are excluded from this run.

## Execution ledger

GitHub issue [#4](https://github.com/edoski/blockweaver/issues/4) records approved consolidation Slices 3A–3F and the compatible release gate.

- K1 hotfix discovery: KAIROS implementation head `148f3f85b9b911491bc324930b9ec069bb4dcc3b` passed all KAIROS gates but proved public Blockweaver 0.3.3 CLI incompatible with locked Typer 0.24.1. Issue #4 was reopened; no KAIROS workaround was accepted.
- CLI hotfix baseline `fe87dbbb5c1c163ee919705fab5edd00083274e8`; implementer `/root/blockweaver_cli_hotfix_impl`; worktree `/Users/edo/dev/python/blockweaver-typer-hotfix`; branch `codex/typer-024-hotfix`.
- Initial head `64ec6f530d978a41334f84130344f0e64fd127de` removed the private import but broadly recognized structurally compatible exceptions. Reviewer `/root/blockweaver_cli_hotfix_review` rejected Standards 2 and Spec 1 because unrelated callback failures could become `CLI_USAGE`.
- First correction `10341053f413e65fab0de937dc9038b6bce54d5b` restricted recognition to exact exit code 2. Re-review rejected Standards 2 and accepted Spec 0 because an unrelated callback could still imitate that shape.
- Final correction `7a10dffade4e2826b4f59293043a00ff60bb281c` catches only the nominal usage-error superclass of public `typer.BadParameter`; static protocol typing performs no runtime classification. An exact-code-2 lookalike propagates unchanged. Final review: Standards 0, Spec 0.
- Integration merge `935db72e453a7bcccd99d09544926fa4c37a6ba1` passed 104 tests, Ruff lint/format, Pyright, Vulture, lock, and diff checks. Focused help and usage envelopes pass Typer 0.24.1 and 0.27.0. No provider, output, KAIROS data, image, GPU, campaign, or corpus action occurred.
- Release 0.3.4 baseline `d30316463560aa4e2b8131a0760dd2cd90a6347c`; implementer `/root/blockweaver_034_release_impl`; candidate `045ff7dc8dcfce01bd7b472d0c1c618cc71a1b74`; reviewer `/root/blockweaver_034_release_review`. The release-only two-line version bump was Standards 0 and Spec 0, and independent builds matched byte-for-byte.
- Branch CI run `31607853394` and tag CI run `31608107732` passed on Ubuntu and macOS. Trusted-publishing run `31608325047` passed. GitHub Release: `https://github.com/edoski/blockweaver/releases/tag/v0.3.4`.
- Public PyPI hashes match both independent candidate builds: wheel `21b0e29f4d7ce495fba26fb5cede0288155525231ab58b647f7c2704f3781842`; sdist `a39bd4973ff10c7422063bc156e9a845b5d8e163edd606cb91814929c21cb47c`. Fresh public CPython 3.11 installs passed the exact API, lazy core, Typer 0.24.1 root/five-command help and JSONL usage boundary, and BigQuery-extra import. The first seconds-old index lookup missed 0.3.4; an explicit package refresh resolved it and proved the public files. Temporary environments were moved to Trash.
- K1 baseline `56eb5498031239e50a6fd4a5f634c2777257e609`; implementer `/root/kairos_k1_impl`; implementation head `148f3f85b9b911491bc324930b9ec069bb4dcc3b`; final public-pin correction `c95ffa1c5a8fb11191524749d88e6ee6ee794570`; reviewer `/root/kairos_k1_review`. The initial 0.3.3 pin exposed the CLI defect and correctly paused acceptance; the correction changed only the exact pin and both locks to public 0.3.4.
- K1 final review: Standards 0, Spec 0. `CorpusDefinition` and handwritten Blockweaver manifest fixtures are gone; metadata callers use generic Dataset facts without duplicate format/schema validation; `BlockFrame` owns chain identity and row-derived nonempty extents; evaluation rejects mismatched associations before hydration. The slice removes 14 production Python lines and 85 test lines, net 99 lines.
- K1 gates passed: 105 root, 43 CUDA-focused, 9 mobile-export, 43 App tests, Ruff lint/format, configured Pyright, Vulture, both lock/frozen-sync gates, App typecheck/dry install, public Blockweaver API/CLI, residue, diff, and clean status. Main fast-forwarded to `c95ffa1c5a8fb11191524749d88e6ee6ee794570` and was pushed identically to `origin` and `research`; no output, corpus, remote execution, image, GPU, job, or campaign action occurred.
- K1 compact synchronization merged exact compact parent `75a36cccc3b04eb9026e3481c2362c2cb6fb1846` with exact K1 main parent `c95ffa1c5a8fb11191524749d88e6ee6ee794570` at `352cc968853bda69b1a9131db709d48d8f1d2043`. Independent review returned Standards 0 and Spec 0; the 11-file CUDA roster and its reviewed product patches remained exact. Both remotes received the accepted ref.
- Image job `45067` built and tested `/scratch.hpc/edoardo.galli3/deployments/kairos-cuda-352cc96.sif` from exact clean compact SHA `352cc968853bda69b1a9131db709d48d8f1d2043`; exit `0:0`, SHA-256 `4b56487c8ea046abd42c6e2bdd186a01b0d029997364f918779d99f101432ad5`. The prior `f49db0b` image remains untouched. CPU-only Servatus publication/retire job `45074` passed on the production filesystem. After explicit user approval, job `45075` performed only a nine-second one-GPU visibility and matrix-multiply probe on an L40S; it created no campaign or scientific output.
- C7A changed only `REMOTE.toml` at `ff2a9e26ba67a7b4b58cc9e389a4eb4e81ff7b95`; independent review returned Standards 0 and Spec 0. C7B merged compact `352cc968853bda69b1a9131db709d48d8f1d2043` first and main `ff2a9e26ba67a7b4b58cc9e389a4eb4e81ff7b95` second at `45f27ef9ce345c59e4c469522e2aa184f613a9e6`; review returned Standards 0 and Spec 0 with all 12 compact-only commits and the 11-file CUDA delta preserved. GitHub and research refs match those exact heads.
- Final dataset acceptance strict-opened all three artifacts locally and through the accepted image with public Blockweaver 0.3.4. Local and research hashes match for every manifest and Parquet file. Scheduler, campaign/symlink/lifecycle-name, code fallback, and cron inventories found no legacy consumer. The three exact local `outputs/corpora/<uuid>` directories moved to `/Users/edo/.Trash/kairos-legacy-corpora-20260812/`; the matching three research directories were permanently deleted. Datasets and all Studies, trials, artifacts, evaluations, experiments, figures, jobs, logs, scratch, and deployment images were untouched.

- Slice 3A baseline: clean pushed `main` at `66f2f119ed15e7d5f1a6d067b07a0579f7b8d693`. Implementer: `/root/consolidation_3a_impl`; worktree `/Users/edo/dev/python/blockweaver-slice-3a`; branch `codex/consolidation-slice-3a`.
- Initial implementation: `b431c0e3262a0bac4849ce3fddfb4ab5669810d4` (`refactor(config): consolidate validation ownership`). Initial review by `/root/consolidation_3a_review`: Standards 0; Spec rejected one P2 because raw provider parsing and resolved `Provider` still duplicated URL/tuning domain validation.
- First correction: `6dda4b938bf6dc36857f9f8bbd3d3e7aa44d735e` (`fix(config): validate provider domains once`). Re-review closed the original P2 and found one correction-edge P2: a huge TOML timeout overflowed during raw float coercion instead of producing `CONFIG_INVALID`.
- Second correction: `d9fc95fdc5380490ed72e0633b87c66d2a65df84` (`fix(config): bound timeout normalization`). Raw numeric parsing is type-only; resolved `Provider` alone normalizes and validates timeout, including huge integers. Final re-review returned Standards 0 and Spec 0.
- Integration: merge `293d738fd0b81af2d506c1c940691575fa7aaea8` (`merge(config): integrate consolidation slice 3a`). Main passed all 96 tests, Ruff lint/format, Pyright, Vulture, lock and diff checks, five-command CLI help, lazy core import, and optional BigQuery import. No provider, output, KAIROS, image, campaign, release, or package publication occurred.
- Slice 3B baseline: clean pushed `main` at `d9611b773d7e2cd878ddc673c980f6fe9f001c07`. Implementer: `/root/consolidation_3b_impl`; worktree `/Users/edo/dev/python/blockweaver-slice-3b`; branch `codex/consolidation-slice-3b`.
- Slice 3B implementation: `027272e35f2a927024cee869ac9ec3a89a03681b` (`refactor(source): consolidate planning and ranges`). `Plan` owns the immutable typed BigQuery requirements projection; duplicate verifier and boundary calls are removed; target agreement is successful control flow; frozen `Header` equality replaces `_same_header` while `_same_core` remains. Reviewer `/root/consolidation_3b_review` returned Standards 0 and Spec 0. The reviewer explicitly challenged the net +34 production lines and accepted them as one deeper requirements owner rather than synchronized shadow state.
- Slice 3B integration: merge `5b79246e5cc2571beacb5184763b16282d16653a` (`merge(source): integrate consolidation slice 3b`). Main passed all 97 tests, Ruff lint/format, Pyright, Vulture, lock and diff checks, five-command CLI help, lazy core import, and optional BigQuery import. No live RPC, ADC, BigQuery job, output, KAIROS, image, campaign, release, or publication occurred.
- Pre-3C hidden-work inventory: the platform config `/Users/edo/Library/Application Support/blockweaver/config.toml` is absent; a bounded read-only scan under `/Users/edo/dev/python` found no `.blockweaver-<uuid>` directory; the process table contained no running Blockweaver process. No active private receipt state needs completion, compatibility, or abandonment before the clean break.
- Slice 3C baseline: clean pushed `main` at `5b4d051241cdc6b29ea73207bd97d88473ebf722`. Implementer: `/root/consolidation_3c_impl`; worktree `/Users/edo/dev/python/blockweaver-slice-3c`; branch `codex/consolidation-slice-3c`.
- Slice 3C implementation: `76d9574fad523993ab445f44953453d8502a79bd` (`refactor(artifact): delete durable receipt shadow state`). It deletes durable `receipt.json`, its schema/recovery/persistence branches, and the redundant publication preflight while preserving public invocation receipts, staged/committed recovery, and no-mutation failure before unsupported atomic rename. Initial review by `/root/consolidation_3c_review`: Standards 0; Spec rejected one test-only P2 because the fresh receipt test asserted keys but not exact fresh counts.
- Slice 3C correction: `85dea4b87e9c3819f874bffc955128cae31386f3` (`test(artifact): assert fresh download counts`). The existing public receipt test now proves zero reused rows and all rows acquired. Re-review returned Standards 0 and Spec 0. Whole slice is net 70 deleted lines.
- Slice 3C integration: merge `e05cae16719bf71cb8d69a82d8b7378f51e77a5c` (`merge(artifact): integrate consolidation slice 3c`). Main passed all 96 tests, Ruff lint/format, Pyright, Vulture, lock and diff checks, CLI/import smokes, and receipt/preflight residue checks. No live provider, output, KAIROS, image, campaign, release, or publication occurred.
- Slice 3D baseline: clean pushed `main` at `4036f5e37ae07fc013a434caf1a5ebb47742e179`. Implementer: `/root/consolidation_3d_impl`; worktree `/Users/edo/dev/python/blockweaver-slice-3d`; branch `codex/consolidation-slice-3d`.
- Slice 3D implementation: `522a903a3932c7a375c3ceec770debad2e81f1ae` (`refactor(proof): verify candidate samples once`). It deletes retained checkpoint sample facts and the early sample provider pass; final strict-candidate revalidation is the sole sampled-row comparison while early target/finality/ancestry and deterministic sample-number facts remain. Reviewer `/root/consolidation_3d_review` returned Standards 0 and Spec 0. The slice deletes a net 19 lines.
- Slice 3D integration: merge `5f4fd8967f484d3702bd3f329a6bbbb08bab56c3` (`merge(proof): integrate consolidation slice 3d`). Main passed all 96 tests, Ruff lint/format, Pyright, Vulture, lock and diff checks, CLI/import smokes, and deleted-symbol residue checks. No live provider, output, KAIROS, image, campaign, release, or publication occurred.
- Slice 3E baseline: clean pushed `main` at `97f2855d637c40881d1f3bf04f781c90aed42daa`. Implementer: `/root/consolidation_3e_impl`; worktree `/Users/edo/dev/python/blockweaver-slice-3e`; branch `codex/consolidation-slice-3e`.
- Slice 3E implementation: `2afd178a5619179ebac2c1d6fca1ac8926f7f49d` (`refactor(artifact): consolidate checkpoint validation passes`). Fresh checkpoints retain trusted metadata after durable write and digest instead of immediately entering recovered-byte admission; recovered checkpoints remain strict and every checkpoint keeps preassembly resealing. Reviewer `/root/consolidation_3e_review` returned Standards 0 and Spec 0.
- Slice 3E measurement: five blocks across three checkpoints. Baseline fresh/resume semantic checkpoint reads were `3/3`; head fresh/resume reads are `0/1`. Baseline and head both perform six digest passes: three durable-write hashes and three preassembly reseals. All temporary instrumentation and detached worktrees were removed.
- Slice 3E integration: merge `776f20226751b71720134aa33b69bfecd8a0fbee` (`merge(artifact): integrate consolidation slice 3e`). Main passed all 96 tests, Ruff lint/format, Pyright, Vulture, lock and diff checks, CLI/import smokes, and instrumentation-residue checks. No live provider, output, KAIROS, image, campaign, release, or publication occurred.
- Slice 3F baseline: clean pushed `main` at `9a8ad767acb12b878ce22c72eb3f24c2441d1f93`. Implementer: `/root/consolidation_3f_impl`; worktree `/Users/edo/dev/python/blockweaver-slice-3f`; branch `codex/consolidation-slice-3f`.
- Slice 3F implementation: `85ffd3adee6f0ba16bb7b3654c7e716bba66cbf8` (`refactor(sources): remove shallow build module`). `_build.py` is deleted; source acquisition/materialization and local plus optional-RPC verification compose in `_sources`, while CLI remains translation-only. Reviewer `/root/consolidation_3f_review` returned Standards 0 and Spec 0. Production drops 14 lines and implementation modules drop from five to four; tests and public interfaces are unchanged.
- Slice 3F integration: merge `95fd7e122c01fc608d0e02d1431a47c620a27a4d` (`merge(sources): integrate consolidation slice 3f`). Main passed all 97 tests, Ruff lint/format, Pyright, Vulture, lock and diff checks, exact public/CLI inventory, optional-import, deleted-module, and adapter-presence checks. No live provider, output, KAIROS, image, campaign, release, or publication occurred.
- Release preparation: `e69c02c2d72cc5250834233d3eee9a525e386eb0` (`chore(release): prepare version 0.3.3`) changes only `pyproject.toml` and `uv.lock`. Final cumulative reviewer `/root/blockweaver_033_release_review` audited `66f2f119...e69c02c` and returned Standards 0 and Spec 0. All 97 tests and local/static/archive/install gates passed.
- Release `v0.3.3`: reviewed head pushed to `main`; branch CI run `31592998520` and tag CI run `31593176767` passed on Ubuntu and macOS. The [GitHub Release](https://github.com/edoski/blockweaver/releases/tag/v0.3.3) triggered Trusted Publishing run `31593360467`; release-commit/tag checks, full gates, build, and PyPI publication passed.
- Public artifact verification: PyPI reports wheel SHA-256 `42f368fb94daab2fdf12f8d4763be82917f4f0b37255a16be98d735c3443ec0b` and sdist SHA-256 `a5f8c55f6310e8766c23b2adb6a5000159a36bf9adb26de24027eb816b8dd9c1`, exactly matching both independent local builds. A fresh public-index core install reported package/runtime `0.3.3`, loaded no Google module, and passed all five command helps; a fresh `blockweaver[bigquery]==0.3.3` install imported the optional adapter. Initial `uv` resolution saw a seconds-old index cache; explicit package refresh succeeded without product change.

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

Post-HPO continuation is complete. KAIROS consumes the three strict Blockweaver datasets, published main and compact-CUDA select the accepted immutable image, and the six superseded legacy corpus directories are gone. No campaign was created or launched here; the runner task owns subsequent scientific execution.
