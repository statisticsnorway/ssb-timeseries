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

Responsibilities for maintaining the production environment is moved closer to the statistical processes owners.

Moving towards modern architecture, development methodology and open source technologies: 

 * Python and R are replacing SAS for production code.
 * Oracle databases and ODI for ETL are being replaced by a data lake architecture relying heavily on Parquet files.
 * The time series solution FAME is not moving along and a replacement has not been chosen. 

Time series are essential to statistics production, so this leaves a huge gap.  

A complete solution will need cover several areas of functionality:

 * Basics: storage with performant read and write, search and filtering 
 * Descriptive metadata 
 * Calculation support: math and statistics libraries
 * Visualisation
 * Workflow integration and process monitoring 
 * Data lineage and ad hoc inspection

This PoC aims to demonstrate how the key functionality may be provided with the core technologies Python and Parquet, in alignment with architecture decisions and process model requirements. As such, it 

 * Basic functionality for read/write, calculations, time aggregation and plotting was demonstrated December 2023.
 * Persisting snapshots in alignment with the process model, simple descriptive tagging and integrations with GCS buckets was added Q1 2024. 

## How to get started?

See notebook files and tests, `demo.ipynb` and `tests/test_*.py` for examples of usage, and what works and in some cases what does not.

Note that
 * The library *should* be platform independent, but has been developed and tested mainly in a Linux environment.
~~ * the solution relies on env vars  TIMESERIES_ROOT and LOG_LOCATION being set. They *must* be set if `/home/jovyan/sample-data` is not reachable, but *should* be set anyway.~~
* The environment variable TIMESERIES_CONFIG is expected to to point to a JSON file with configurations.
* The command `poetry run timeseries-config home` can be run from a terminal in order to create the environment variable and a file with default configurations in the home directory, ie `/home/jovyan` in the Jupyter environment (or the equivalent running locally).
* The similar `poetry run timeseries-config gcs` will put configurations and logs in the home directory and time series data in a shared bucket `gs://ssb-prod-dapla-felles-data-delt/poc-tidsserier`. 
* With the environment variable set and the configuration in place `poetry run pytest` should succeed.
* That is, if the code is on the python path. A little bit down the road, we aim to make the library available on PyPi and installable by way of `poetry add <...>`. Till then, you will need to clone this repo and make sure the code is visible to your project.


While the library is in a workable state and should work both locally and in JupyterLab, it is still in an exploratory phase. There is a risk that fundamental choices are reversed and breaking changes introduced. 

With that disclaimer, feel free to explore and experiment, and do not be shy about asking questions or giving feedback. At this stage, feedback is all important. 

Assuming you have Python working with a standard SSB setup for git and poetry etc, the following should get you going:

``` bash
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
~~ No longer needed:~~ 
~~ Create and set a location for data and log files. This could be anywhere, but separated from the code is preferrable.~~ 
~~ mkdir series~~ 
~~ export TIMESERIES_ROOT=${PWD}/series ~~ 
~~ export LOG_LOCATION=${PWD}/series ~~ 


## Functionality overview

The core of the library is the Dataset class. This is essentially a wrapper around a DataFrame (for now Pandas, later probably Polars) in the .data attribute. 

The .data attribute should comply to conventions implied by the underlying *information model*. These will start out as pure conventions and subject to evalutation. At a later stage they are likely to be enforced by Parquet schemas. Failing to obey them will cause some methods to fail. 

The Dataset.io attribute connects the dataset to a helper class that takes care of reading and writing data. This structure abstracts away the IO mechanics, so that the user do not need to know about the "physical" details, only the *information model meaning* of the choices made.

 * Read and write for both versioned and unversioned data types.
 * Search for sets by name, regex and (planned for later) metadata.
 * Basic filtering of sets (selecting series within a selected set).
 * Basic linear algebra: Datasets can be added, subtracted, multiplied and divided with each other and dataframes, matrices, vectors (untested) and scalars according to normal rules.  
 * Basic plotting: Dataset.plot() as shorthand for Dataset.data.plot(<and sensible defaults>).
 * Basic time aggregation: 
 `Dataset.groupby(<frequency>, 'sum'|'mean'|'auto')`
 * 


 ## The information model

 ### TLDR

 * **Types** are defined by
  * **Versioning** defines how updated versions of the truth are represented: NONE overwrites a single version, NAMED or AS_OF maintaines new "logical" versions identified by name or date.
  * **Temporality** describes the "real world" valid_at or valid_from - valid_to datetime of the data. It will translate into columns, datetime or period indexes of Dataset.data.
  * Value type (only scalars for now) of Dataset.data "cells".
* **Datasets** can consists of multiple series. (Later: possible extension with sets of sets.)
* All series in a set must be of the same type. 
* **Series** are value columns in Datasets.data, rows identified by date(s) or index corresponding temporality.
* The combination `<Dataset.name>.<Series.name>` will serve as a globally unique series identifier.
* `<Dataset.name>` identifies a "directory", hence must be unique. (Caveat: Directories per type creates room for error.)
* `<Series.name>` (.data column name) must be unique within the set. 
* Series names *should* be related to (preferrably constructed from) codes or meta data in such a way that they can be mapped to "tags" via a format mask (and if needed a translation table). 

Yes, that *was* the short version. The long version is still pending production.

To be continued ...

### How to contribute

More information about this will come later, but contributions are welcome. If you want to contribute, just let us know. 

### Other sources of documentation:

* https://statistics-norway.atlassian.net/wiki/spaces/Arkitektur/pages/3581313026/Statistikkproduksjon
* https://statistics-norway.atlassian.net/wiki/spaces/Arkitektur/pages/3595665419/Lagring+av+tidsserier

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
