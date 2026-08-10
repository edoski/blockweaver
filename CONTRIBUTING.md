# Contributing

Blockweaver requires Python 3.11 or newer and uses `uv` for reproducible development.

```console
uv sync --locked --dev
uv run pytest
uv run ruff check src tests
uv run ruff format --check src tests
uv run pyright
uv run vulture src tests --min-confidence 80
```

Tests must use local fake JSON-RPC and BigQuery clients. Never add a test that contacts a public provider, submits a BigQuery job, or requires credentials. Test behavior through the CLI, published dataset pair, or fake external-service boundary. Keep changes within five implementation modules, five runtime dependencies including extras, and 900 test code lines.

Open an issue before broadening the CLI, durable format, dependency set, or provider model. Submit focused changes with documentation for user-visible behavior.

## Release

Update the project version and lockfile on a green `main`, then publish a GitHub Release tagged `v<version>`. The publish workflow repeats every quality gate, requires the tag to match the project version, builds the wheel and source archive, and uploads them through PyPI Trusted Publishing. It stores no release credential or build artifact.
