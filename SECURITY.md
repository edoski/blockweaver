# Security policy

Security fixes target the latest released version. Report vulnerabilities through [GitHub private vulnerability reporting](https://github.com/edoski/blockweaver/security/advisories/new). Do not open a public issue for an unpatched vulnerability.

RPC URLs often contain credentials. Prefer provider `url_env` settings, restrict access to the process environment and the mode-0600 configuration created by `blockweaver init`, and avoid direct URL CLI overrides on shared systems. Blockweaver excludes resolved URLs and environment values from manifests, receipts, and intentional logs, and redacts them from expected failures. Host telemetry, provider logs, shell history, crash tooling, and storage permissions remain the operator's responsibility.

Use independently operated primary and verifier providers. Confirm that both support archival reads, batch JSON-RPC, the configured finality tag, and any requested `eth_feeHistory` range. Verification consumes provider quota and can incur charges. Blockweaver's agreement, ancestry, anchor-reread, and sample checks detect many endpoint faults but do not replace a consensus client.

Published directories are immutable by contract. Treat any local verification failure, digest mismatch, unexpected file, or RPC disagreement as corruption; create a new UUID-backed download rather than modifying the artifact.
