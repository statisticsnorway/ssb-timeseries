# SSB Timeseries

[![PyPI](https://img.shields.io/pypi/v/ssb-timeseries.svg)][pypi status]
[![Status](https://img.shields.io/pypi/status/ssb-timeseries.svg)][pypi status]
[![Python Version](https://img.shields.io/pypi/pyversions/ssb-timeseries)][pypi status]
[![License](https://img.shields.io/pypi/l/ssb-timeseries)][license]

[![Documentation](https://github.com/statisticsnorway/ssb-timeseries/actions/workflows/docs.yml/badge.svg)][documentation]
[![Tests](https://github.com/statisticsnorway/ssb-timeseries/actions/workflows/tests.yml/badge.svg)][tests]
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=statisticsnorway_ssb-timeseries&metric=coverage)][sonarcov]
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=statisticsnorway_ssb-timeseries&metric=alert_status)][sonarquality]

[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)][pre-commit]
[![Black](https://img.shields.io/badge/code%20style-black-000000.svg)][black]
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Poetry](https://img.shields.io/endpoint?url=https://python-poetry.org/badge/v0.json)][poetry]

[pypi status]: https://pypi.org/project/ssb-timeseries/
[documentation]: https://statisticsnorway.github.io/ssb-timeseries
[tests]: https://github.com/statisticsnorway/ssb-timeseries/actions?workflow=Tests
[sonarcov]: https://sonarcloud.io/summary/overall?id=statisticsnorway_ssb-timeseries
[sonarquality]: https://sonarcloud.io/summary/overall?id=statisticsnorway_ssb-timeseries
[pre-commit]: https://github.com/pre-commit/pre-commit
[black]: https://github.com/psf/black
[poetry]: https://python-poetry.org/

## Background

Statistics Norway is building a new procuction system in the cloud.

Moving towards modern architecture, development methodology and open source technologies: Python and R are replacing SAS for statistics production code. Oracle databases and ODI for ETL are being replaced by a data lake architecture relying heavily on Parquet files.

Another big issue has been time series.Time series are essential to statistics production, so the decision to phase out FAME while not having landed precisely what should replace it has left a huge gap.

A complete solution will touch several areas of functionality:

- The core is storage with performant read and write, search and filtering
- Good descriptive metadata is key to findability
- A wide selection of math and statistics libraries is key for calculations and models
- Visualisation tools play a role both in ad hoc and routine inspection and quality control
- Workflow integration with automation and process monitoring help keeping consistent quality
- Data lineage and process metadata is essential for quality control

In Statistics Norway strict requirements for transparency and data quality are mandated by law and commitment to international standards. The data itself has a wide variety, but time resolution and publishing frequencies are typically low. While volumes are some times significant, they are far from extreme. This shifts the focus from performance towards process and data control.

This project came out of a PoC to demonstrate how the key functionality may be provided with the core technologies Python and Parquet, in alignment with architecture decisions and process model requirements. Constructed to be an abstraction between the storage layer and the statistics production code, it provides a way forward while postponing some the technical choices.

- Basic functionality for read/write, calculations, time aggregation and plotting was demonstrated December 2023.
- Persisting snapshots in alignment with the process model, simple descriptive tagging and integrations with GCS buckets was added Q1 2024.

## How to get started?

See notebook files and tests, `demo.ipynb` and `tests/test_*.py` for examples of usage, and what works and in some cases what does not.

Note that

- The library is constructed to be platform independent, but top priority is making it work in a Linux environment.
- Install by way of `poetry add ssb_timeseries`.
- The library should work out of the box with default settings. Note that the defaults are for local testing, ie not be suitable for the production setting.
- To apply custom settings: The environment variable TIMESERIES_CONFIG should point to a JSON file with configurations.
- The command `poetry run timeseries-config <...>` can be run from a terminal in order to shift between defauls.
- Run `poetry run timeseries-config home` to create the environment variable and a file with default configurations in the home directory, ie `/home/jovyan` in the Jupyter environment (or the equivalent running elsewhere.
- The similar `poetry run timeseries-config gcs` will put configurations and logs in the home directory and time series data in a shared bucket `gs://ssb-prod-dapla-felles-data-delt/poc-tidsserier`.
- With the environment variable set and the configuration in place `poetry run pytest` should succeed.

While the library is in a workable state and should work both locally and in JupyterLab, it is still in an exploratory phase. There is a risk that fundamental choices are reversed and breaking changes introduced.

With that disclaimer, feel free to explore and experiment, and do not be shy about asking questions or giving feedback. At this stage, feedback is all important.

Assuming you have Python working with a standard SSB setup for git and poetry etc, the following should get you going:

```bash
# Get the poc package
git clone https://github.com/statisticsnorway/arkitektur-poc-tidsserier.git

# Run inside a poetry controlled venv:
poetry shell
## Create default config
poetry run timeseries-config home
# Run the tests to check that everything is OK:
poetry run pytest
# A couple of the test cases *are expected* fail when running for the first time in a new location.
# They should create the structures they need and should succeed in subsequent runs.
```

## Functionality overview

The core of the library is the Dataset class. This is essentially a wrapper around a DataFrame (for now Pandas, later probably Polars) in the .data attribute.

The .data attribute should comply to conventions implied by the underlying _information model_. These will start out as pure conventions and subject to evalutation. At a later stage they are likely to be enforced by Parquet schemas. Failing to obey them will cause some methods to fail.

The Dataset.io attribute connects the dataset to a helper class that takes care of reading and writing data. This structure abstracts away the IO mechanics, so that the user do not need to know about the "physical" details, only the _information model meaning_ of the choices made.

- Read and write for both versioned and unversioned data types.
- Search for sets by name, regex and (planned for later) metadata.
- Basic filtering of sets (selecting series within a selected set).
- Basic linear algebra: Datasets can be added, subtracted, multiplied and divided with each other and dataframes, matrices, vectors (untested) and scalars according to normal rules.
- Basic plotting: Dataset.plot() as shorthand for Dataset.data.plot(<and sensible defaults>).
- Basic time aggregation:
  `Dataset.groupby(<frequency>, 'sum'|'mean'|'auto')`

## The information model

### TLDR

- **Types** are defined by
- **Versioning** defines how updated versions of the truth are represented: NONE overwrites a single version, NAMED or AS_OF maintaines new "logical" versions identified by name or date.
- **Temporality** describes the "real world" valid_at or valid_from - valid_to datetime of the data. It will translate into columns, datetime or period indexes of Dataset.data.
- Value type (only scalars for now) of Dataset.data "cells".
- **Datasets** can consists of multiple series. (Later: possible extension with sets of sets.)
- All series in a set must be of the same type.
- **Series** are value columns in Datasets.data, rows identified by date(s) or index corresponding temporality.
- The combination `<Dataset.name>.<Series.name>` will serve as a globally unique series identifier.
- `<Dataset.name>` identifies a "directory", hence must be unique. (Caveat: Directories per type creates room for error.)
- `<Series.name>` (.data column name) must be unique within the set.
- Series names _should_ be related to (preferrably constructed from) codes or meta data in such a way that they can be mapped to "tags" via a format mask (and if needed a translation table).

Yes, that _was_ the short version. The long version is still pending production.

To be continued ...

### Other sources of documentation:

- https://statistics-norway.atlassian.net/wiki/spaces/Arkitektur/pages/3581313026/Statistikkproduksjon
- https://statistics-norway.atlassian.net/wiki/spaces/Arkitektur/pages/3595665419/Lagring+av+tidsserier

## API-documentation

The [documentation] is published on GitHub Pages. Se the Reference page for
API-documentation.

## Contributing

Contributions are very welcome.
To learn more, see the [Contributor Guide].

## License

Distributed under the terms of the [MIT license][license],
_SSB Timeseries_ is free and open source software.

## Issues

If you encounter any problems,
please [file an issue] along with a detailed description.

## Credits

This project was generated from [Statistics Norway]'s [SSB PyPI Template].

[statistics norway]: https://www.ssb.no/en
[pypi]: https://pypi.org/
[ssb pypi template]: https://github.com/statisticsnorway/ssb-pypitemplate
[file an issue]: https://github.com/statisticsnorway/ssb-timeseries/issues
[pip]: https://pip.pypa.io/

<!-- github-only -->

[license]: https://github.com/statisticsnorway/ssb-timeseries/blob/main/LICENSE
[contributor guide]: https://github.com/statisticsnorway/ssb-timeseries/blob/main/CONTRIBUTING.md
[reference guide]: https://statisticsnorway.github.io/ssb-timeseries/reference.html
