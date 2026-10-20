# AGENTS.md

Project-specific guidance for assistants working in the `ssb-timeseries` repository.
General working rules live in the user's global `AGENTS.md`.
This file only adds repository facts and conventions; it does not repeat the global rules.

## Project

`ssb-timeseries` is Statistics Norway's glue layer between the data platform and statistics production code.
It is `Dataset`-centric: a dataset is a set of equally typed series, stored through pluggable IO handlers, with built-in linear-algebra and time-algebra operations and descriptive metadata.
The package supports Python 3.11 to 3.14 (excluding 3.14.1) and is managed with Poetry.
See `README.md` for the domain model and `CONTRIBUTING.md` for environment setup.

## Layout

- `src/ssb_timeseries/` — library source.
  - `catalog.py`, `dataset.py`, `series/` — the core `Dataset` and series API.
  - `io/` — IO handlers and protocols (`protocols.py`), filesystem (`fs.py`), archiving (`archiving.py`), Parquet (`pyarrow_simple.py`, `pyarrow_hive.py`, `parquet_schema.py`), and JSON metadata (`json_metadata.py`).
  - `meta/` — metadata loaders, tags, taxonomy, and formatters.
  - `config/`, `types/`, `dataframes/`, `dates.py`, `intervals.py` — supporting subsystems.
  - `cli/` — the `ssb-timeseries` and `ts` command-line entry points.
- `tools/` — project utilities and helper scripts.
- `tests/` — the pytest suite, laid out to mirror `src/` (`tests/io`, `tests/meta`, `tests/dataset`, ...), with shared fixtures in `tests/fixtures/`.
- `docs/` — Sphinx documentation; `docs/guides/` holds marimo notebooks exported to `_assets/`; `docs/adr/` — Architectural Decision Records (versioned).
- `notes/` — gitignored working notes.

## Toolchain

- Set up with `poetry install`; run tools directly with `poetry run <tool>`, or run the full matrix with `nox`.
- Nox sessions: `lint` (ruff check and format check), `mypy`, `tests`, `coverage`, `typeguard`, `xdoctest`, `docs-build`, `docs`, and `deptry`.
- Format every edited Python file with `ruff format`, via the pre-commit hook or `poetry run ruff format <file>`.
- Pre-commit hooks must pass; re-add any file that a hook rewrites.
- Write text files LF-only, without trailing whitespace, and ending with exactly one newline; the `trailing-whitespace`, `end-of-file-fixer`, and `mixed-line-ending` pre-commit hooks enforce all three.

## Commit convention

- Use an imperative-mood summary.

## Testing

- Tests mirror the source layout and use pytest; place new tests in the directory matching the code under test.
- Set `SSB_TYPEGUARD=1` to run the suite with typeguard's import hook (`tests/conftest.py:18`); the `typeguard` nox session does this and filters `typeguard.TypeHintWarning`.
- Known xfail: the multi-repo catalog search test at `tests/test_catalog.py:402`, to re-enable once multi-repo test setup exists.

## Configuration

- The runtime configuration is a JSON or YAML file identified by the `TIMESERIES_CONFIG` environment variable (`src/ssb_timeseries/config/constants.py:40`); nox sessions write a temporary config and point the variable at it.

## CI and quality

- Workflows live in `.github/workflows/`: `tests.yml` (tests, typeguard, SonarCloud), `docs.yml`, `release.yml`, and `labeler.yml`.
- SonarCloud reads `coverage.xml`, configured in `sonar-project.properties`.
- `tests.yml` rewrites the empty `<source></source>` that `coverage xml` emits for library files (a `relative_files = true` + src-layout artifact); without it, Coverage on New Code reads 0%.

## Notes

- `notes/` holds transient working notes for in-progress ideas and designs not yet mature enough for issues or official documentation.
- It is deliberately outside version control.
- The goal is to keep them few and short-lived, not to build an archive.

## Python version policy

- Support ≥ last 3 minor Python versions.
- Focus testing/validation on 2nd newest (3.14).
- Add newest (3.15) support/testing ASAP without dropping older versions prematurely.
- Keep legacy only if it needs no extra effort; age alone isn't reason to drop.
- `pyproject.toml`: `>=3.11, <4.0, !=3.14.1`.

## Git ignore

- `notes/` and `GEMINI.md` are gitignored (`.gitignore:11-12`); this `AGENTS.md` is tracked.
- `.geminiignore` is Gemini-specific and does not apply to other tools.
