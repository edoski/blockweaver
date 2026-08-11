# Security policy

Security fixes target the latest released version. Report vulnerabilities through [GitHub private vulnerability reporting](https://github.com/edoski/blockweaver/security/advisories/new). Do not open a public issue for an unpatched vulnerability.

RPC URLs often contain credentials. Prefer provider `url_env` settings, restrict access to the process environment and the mode-0600 configuration created by `blockweaver init`, and avoid direct URL CLI overrides on shared systems. Blockweaver excludes resolved URLs and environment values from manifests, receipts, and intentional logs, and redacts them from expected failures. Host telemetry, provider logs, shell history, crash tooling, and storage permissions remain the operator's responsibility.

Use independently operated primary and verifier providers. Confirm that both support archival reads, batch JSON-RPC, the configured finality tag, and any requested `eth_feeHistory` range. Verification consumes provider quota and can incur charges. Blockweaver's agreement, ancestry, anchor-reread, and sample checks detect many endpoint faults but do not replace a consensus client.

Published directories are immutable by contract. The public `open_dataset` loader requires the directory UUID to match the manifest UUID and strictly validates the canonical manifest, exact file set, schema, range, and data digest. Treat any local verification failure, digest mismatch, unexpected file, or RPC disagreement as corruption; create a new UUID-backed download rather than modifying the artifact.

BigQuery uses Google Application Default Credentials. Keep credential files and billing-project environment values out of the TOML file when possible, and grant only the permissions needed to inspect schemas and run queries. Blockweaver never accepts user SQL: configured dataset identifiers are strictly validated and selected features compile through a closed query builder.

`maximum_bytes_billed` is enforced on the billable query and checked against a preceding dry-run estimate. It limits one query, not total account spending, and schema inspection, retries, provider verification, or other Google activity may have separate costs. Use project quotas and billing alerts as independent controls. BigQuery data remains externally supplied; publication still requires RPC chain, boundary, sample, ancestry, and finality proof.
