# Project: sqlalchemy-odata

SQLAlchemy dialect + Shillelagh adapter for querying OData v4 APIs with SQL.

## Commit conventions

All commits must use [Conventional Commits](https://www.conventionalcommits.org/) format. PRs are merged via rebase, so individual commit messages drive releases (PR titles are not enforced).

- `feat: ...` — new feature (minor version bump)
- `fix: ...` — bug fix (patch version bump)
- `feat!: ...` or `fix!: ...` — breaking change (major version bump)
- `docs:`, `chore:`, `ci:`, `test:`, `refactor:` — no release triggered

## Development

```bash
pip install -e ".[dev]"
pytest
ruff check src/ tests/
ruff format src/ tests/
```

## Architecture

- `src/shillelagh_odata/adapter.py` — Shillelagh adapter: fetches OData data, parses $metadata EDMX
- `src/shillelagh_odata/dialect.py` — SQLAlchemy dialect: `odata://` connection strings, table discovery
- `src/shillelagh_odata/engine_spec.py` — Superset engine spec: registers OData in Superset UI
- Entry point name `odataapi` in pyproject.toml must match `ADAPTER_NAME` in dialect.py

## Branch workflow

- `main` is protected — all changes go through PRs
- CI must pass (lint, format, tests on Python 3.9-3.13, build)
- Release-please automates versioning, changelogs, and PyPI publishing
- Version is tracked in both `pyproject.toml` and `src/shillelagh_odata/__init__.py` (release-please updates both)
