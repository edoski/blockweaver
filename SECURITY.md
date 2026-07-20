# Security policy

Security fixes target the latest released version.

Report vulnerabilities through [GitHub private vulnerability reporting](https://github.com/edoski/blockweaver/security/advisories/new). Do not open a public issue for an unpatched vulnerability.

RPC URLs often contain credentials. Pass them through `BLOCKWEAVER_RPC_URL` and `BLOCKWEAVER_VERIFY_RPC_URL` where possible, keep receipts separate from logs, and restrict access to process environments and shell history. Blockweaver does not persist or intentionally print RPC URLs, but operators remain responsible for provider logs, host telemetry, and storage permissions.
