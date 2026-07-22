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

Tests must use local fake JSON-RPC servers. Never add a test that contacts a public provider or requires credentials. Test behavior through the CLI, durable Corpus pair, or fake external JSON-RPC boundary. Keep changes within five implementation modules, five runtime dependencies, and 900 test code lines.

Open an issue before broadening the CLI, durable format, dependency set, or provider model. Submit focused changes with documentation for user-visible behavior.
