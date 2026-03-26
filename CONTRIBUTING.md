# Contributing to sqlalchemy-odata

Thanks for your interest in contributing!

## Development setup

```bash
git clone https://github.com/brandonjjon/sqlalchemy-odata.git
cd sqlalchemy-odata
pip install -e ".[dev]"
```

## Running tests

```bash
pytest
```

With coverage:

```bash
pytest --cov=shillelagh_odata --cov-report=term-missing
```

## Code quality

This project uses [ruff](https://docs.astral.sh/ruff/) for linting and formatting.

Check for issues:

```bash
ruff check src/ tests/
ruff format --check src/ tests/
```

Auto-fix issues:

```bash
ruff check --fix src/ tests/
ruff format src/ tests/
```

CI will block merges if lint or formatting checks fail, so make sure to run these before pushing.

## Commit messages

This project uses [Conventional Commits](https://www.conventionalcommits.org/) and [release-please](https://github.com/googleapis/release-please) for automated releases. Please format your commit messages like:

- `fix: handle null OData response` (patch release)
- `feat: add bearer token auth` (minor release)
- `feat!: redesign connection API` (major release)
- `docs: update README` (no release)
- `test: add pagination edge cases` (no release)
- `chore: update dependencies` (no release)

## Submitting changes

1. Fork the repository
2. Create a feature branch (`git checkout -b feat/my-feature`)
3. Make your changes with tests
4. Run all checks (`pytest`, `ruff check`, `ruff format --check`)
5. Commit using conventional commit messages
6. Push and open a pull request against `main`

CI runs automatically on pull requests. All checks must pass before merging.

## Reporting bugs

Please open an issue at https://github.com/brandonjjon/sqlalchemy-odata/issues with:

- Your Python version and OS
- The OData service you're connecting to (if possible)
- The full error traceback
- A minimal code example that reproduces the issue
