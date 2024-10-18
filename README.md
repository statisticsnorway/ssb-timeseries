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
[API reference]: https://statisticsnorway.github.io/ssb-timeseries/reference.html
[tests]: https://github.com/statisticsnorway/ssb-timeseries/actions?workflow=Tests
[sonarcov]: https://sonarcloud.io/summary/overall?id=statisticsnorway_ssb-timeseries
[sonarquality]: https://sonarcloud.io/summary/overall?id=statisticsnorway_ssb-timeseries
[pre-commit]: https://github.com/pre-commit/pre-commit
[black]: https://github.com/psf/black
[poetry]: https://python-poetry.org/

## Background

Statistics Norway is the national statistics agency in Norway. We are building a new production system in the cloud and moving towards a modern architecture based on open source technologies.

**Time series** play a key role in the statistics production process.

Our mission comes with strict requirements for transparency and data quality. Some are mandated by law, others stem from commitment to international standards.

The data itself has a wide variety, but time resolution and publishing frequencies are typically low. While volumes are sometimes significant, they are far from extreme. Quality and reliability is by far more important than latency. This shifts the focus towards process and data control.

This libarary came out of a PoC to demonstrate how key functionality could be provided in alignment with architecture decisions and process model requirements.

- At the core is storage with performant read and write, search and filtering of the time series data
- Good descriptive metadata is key to findability
- A wide selection of math and statistics libraries is key for calculations and models
- Visualisation tools play a role both in ad hoc and routine inspection and quality control
- Workflow integration with automation and process monitoring help keeping consistent quality
- Data lineage and process metadata is essential for quality control

It is constructed to be an abstraction between the storage and automation layers and the statistics production code. providing a way forward while postponing some technical choices.

## How to get started?

- Install by way of `poetry add ssb_timeseries`.
- The library should work out of the box with default settings. Note that the defaults are for local testing, ie not be suitable for the production setting.
- To apply custom settings: The environment variable TIMESERIES_CONFIG should point to a JSON file with configurations.
- The command `poetry run timeseries-config <...>` can be run from a terminal in order to shift between defauls.
- Run `poetry run timeseries-config home` to create the environment variable and a file with default configurations in the home directory, ie `/home/jovyan` in the SSB Jupyter environment (or the equivalent running elsewhere).
- The similar `poetry run timeseries-config gcs` will put configurations and logs in the home directory and time series data in a shared GCS bucket `gs://ssb-prod-dapla-felles-data-delt/poc-tidsserier`. Take appropriate steps to make sure you have access. The library does not attempt to facilitate that.
- With the environment variable set and the configuration in place you should be all set. See the reference https://statisticsnorway.github.io/ssb-timeseries/reference.html

**Note that** while the library is in a workable state and should work both locally and (for SSB users) in JupyterLab, it is still in early development. There is a risk that fundamental choices are reversed and breaking changes introduced.

With that disclaimer, feel free to explore and experiment, and do not be shy about asking questions or giving feedback.


## Functionality and structure overview

The core of the library is the `Dataset` class. Datasets consist of one or more series, but they should not be used for arbitrary groupings: Workflow integrations call for a stricter definition, where primary datasets consist of series of the same type originating from the same process. It is not a strict requirement that all series in a set are written at the same time, but it tends to simplify workflows a lot if they are.

The dataset is a wrapper around an Arrow table (held in the .data attribute; accessible as a Dataframe). By requiring the .data attribute to comply with an underlying _information model_ it provides consistent storage, with descriptive metadata, search. Format restrictions enables calculation features:
- Since each series is represented as a column vector in a dataset matrix, *linear algebra* is readily available. Datasets can be added, subtracted, multiplied and divided with each other and dataframes, matrices, vectors (untested) and scalars according to normal rules.
- *Time algebra* features allow up- and downsamliong that make use of the date columns. Basic time aggregation:
`Dataset.groupby(<frequency>, 'sum'|'mean'|'auto')`

- *Metadata calculations* uses the descriptions of the individual series for calculations ranging from simple things like unit conversions to using relations between entities in tag values to group series for aggregation.


The `io module` connects the dataset to helper class(es) that takes care of reading and writing data. This structure abstracts away the IO mechanics, so that the user do not need to know about implementation details, but only the _information model meaning_ of the choices made. Also, although the current implementation uses pyarrow and parquet data structures under the hood, by replacing the io-module, a database could be used instead.


- Read and write for both versioned and unversioned data types.
- Search for sets by name, regex and metadata.
- Basic filtering of sets (selecting series within a selected set).
- Basic plotting: Dataset.plot() as shorthand for Dataset.data.plot(<and sensible defaults>).

## The information model

Data type implies mandatory date columns shared by all series in the dataset. Series are represented as columns. These start out as pure conventions and subject to evalutation. At a later stage they are likely to be enforced by Parquet schemas. Failing to obey them will cause some methods to fail.

Both the datasets and the series in the set can be *tagged*ie associated with any number of key-value pairs. While the related features can benefit greatly from using controlled vocabularies or structured taxonomies, and some integrations with Statistics Norway meta data are built in, this is not a strict requirement.

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



### Internal documentation:

- https://statistics-norway.atlassian.net/wiki/spaces/Arkitektur/pages/3581313026/Statistikkproduksjon
- https://statistics-norway.atlassian.net/wiki/spaces/Arkitektur/pages/3595665419/Lagring+av+tidsserier

## API-documentation

The [documentation] is published on GitHub Pages. See the [API reference]  for API-documentation.

## Contributing

Contributions are very welcome.

For SSB internals, assuming you have Python working with a standard SSB setup for git and poetry etc, the following should get you going:

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

See the [Contributor Guide] to learn more.

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
