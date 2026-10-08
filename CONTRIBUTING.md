# Contributor Guide

Thank you for your interest in improving this project.
This project is open-source under the [MIT license] and welcomes contributions in the form of bug reports, feature requests, and pull requests.

Here is a list of important resources for contributors:

- [Source Code]
- [Documentation]
- [Issue Tracker]
- [Code of Conduct]

## How to report a bug

Report bugs on the [Issue Tracker].

When filing an issue, make sure to answer these questions:

- Which operating system and Python version are you using?
- Which version of this project are you using?
- What did you do?
- What did you expect to see?
- What did you see instead?

The best way to get your bug fixed is to provide a test case, and/or steps to reproduce the issue.

## How to request a feature

Request features on the [Issue Tracker].

## Project outline

The project uses a [src layout](https://packaging.python.org/en/latest/discussions/src-layout-vs-flat-layout/) with the usual `src/`, `docs/` and `tests/` directories, plus `notebooks/` and `tools/`.

| Path | Contents |
|---|---|
| `src/ssb_timeseries/` | The library published to PyPI. |
| `tests/` | The test suite, mirroring the `src/` layout. |
| `docs/` | Documentation sources, including the `docs/reference/` API reference. |
| `notebooks/` | Marimo notebooks and their configurations. |
| `tools/` | Development scripts that are not part of the package, covering notebook helpers and the export of notebook content to `docs/`. |

The guides under `docs/guides/` are exported from `notebooks/`, and the inventory of exported notebooks is `NOTEBOOK_NAMES` in `tools/export_all_guides.py`.
Edits to an exported guide are lost on the next run, so edit the notebook instead.
A guide the inventory does not list is hand-maintained.

Rebuild only the guides whose notebooks changed.
Rebuilding everything is discouraged while editing, because several notebooks generate unseeded random data and a full run produces large diffs that hide the real change:

```console
poetry run python tools/marimo_to_md.py notebooks/quickstart.py docs/guides/quickstart.md
```

Name just the notebook you changed and write it over the existing guide.
`tools/marimo_to_md.py` sets `TIMESERIES_CONFIG` to `notebooks/minimal_configuration.json` and runs from the repository root, so no environment setup is needed.
It exports only what you name instead of executing all ten notebooks, which makes it the right command for the edit-review loop.

Rebuilding is a manual step, not a Nox session.
To export the whole inventory and check that every listed notebook still exports, run the full script:

```console
poetry run python tools/export_all_guides.py
```
Use this option sparingly.

Add a notebook to `tests/notebooks/test_guides.py` to have it run as part of the test suite, without regenerating anything in `docs/guides/`.
The test only checks that the notebook runs to completion, so put the real assertions in cells named `test_...`.

## How to set up your development environment

You need Python 3.11+ (except 3.14.1) and the following tools:

- [Poetry]
- [Nox]
- [nox-poetry]

Install [pipx]:

```console
python -m pip install --user pipx
python -m pipx ensurepath
```

Install [Poetry]:

```console
pipx install poetry
```

Install [Nox] and [nox-poetry]:

```console
pipx install nox
pipx inject nox nox-poetry
```

Install the pre-commit hooks

```console
poetry run pre-commit install --hook-type pre-commit --hook-type pre-push
```

Install the package with development requirements:

```console
poetry install
```

You can now run an interactive Python session, or your app:

```console
poetry run python
poetry run ssb-timeseries
```

## How to test the project

Run the full test suite:

```console
nox
```

List the available Nox sessions:

```console
nox --list-sessions
```

You can also run a specific Nox session.
For example, invoke the unit test suite like this:

```console
nox --session=tests
```

Unit tests are located in the `tests/` directory, and are written using the [pytest] testing framework.

## How to submit changes

Open a [pull request] to submit changes to this project.

Your pull request needs to meet the following guidelines for acceptance:

- The Nox test suite must pass without errors and warnings.
- Include unit tests. This project aims for high code coverage.
- If your changes add functionality, update the documentation accordingly.

Feel free to submit early, though — we can always iterate on this.

It is recommended to open an issue before starting work on anything.
This will allow a chance to talk it over with the owners and validate your approach.

We also appreciate it if you follow the [Google Python Style Guide](https://google.github.io/styleguide/pyguide.html) and use [semantic line breaks](https://sembr.org/) for Markdown and reStructured Text.

[mit license]: https://opensource.org/licenses/MIT
[source code]: https://github.com/statisticsnorway/ssb-timeseries
[documentation]: https://statisticsnorway.github.io/ssb-timeseries
[issue tracker]: https://github.com/statisticsnorway/ssb-timeseries/issues
[pipx]: https://pipx.pypa.io/
[poetry]: https://python-poetry.org/
[nox]: https://nox.thea.codes/
[nox-poetry]: https://nox-poetry.readthedocs.io/
[pytest]: https://pytest.readthedocs.io/
[pull request]: https://github.com/statisticsnorway/ssb-timeseries/pulls

<!-- github-only -->

[code of conduct]: CODE_OF_CONDUCT.md
