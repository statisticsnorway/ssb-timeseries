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
[API reference]: https://statisticsnorway.github.io/ssb-timeseries/reference/index.html
[quickstart guide](https://statisticsnorway.github.io/ssb-timeseries/quickstart.html)
[tests]: https://github.com/statisticsnorway/ssb-timeseries/actions?workflow=Tests
[sonarcov]: https://sonarcloud.io/summary/overall?id=statisticsnorway_ssb-timeseries
[sonarquality]: https://sonarcloud.io/summary/overall?id=statisticsnorway_ssb-timeseries
[pre-commit]: https://github.com/pre-commit/pre-commit
[black]: https://github.com/psf/black
[poetry]: https://python-poetry.org/

[https://www.ssb.no/_/asset/mimir:00000198eb05f160/SSB_logo_black.svg]
Statistics Norway is the national statistical agency of Norway and the main producer of official statistics.
We collect, produce and communicate statistics related to the economy, population and society at national, regional and local levels and conducts extensive research and analysis activities.

Our mission comes with strict requirements for transparency and data quality.
Some are [mandated by law](https://www.ssb.no/en/omssb/ssbs-virksomhet/styringsdokumenter), others stem from commitment to international standards and best practices.

# SSB Timeseries

**Time series** play a key role in the statistics production process.

The data itself has a wide variety, but while data volumes are substantial, they are not enormous.
The data resolution and publishing frequencies are typically low: monthly, quarterly and yearly are most typical.

Quality and reliability is by far more important than latency.
This shifts the focus towards process and data control.

This libarary came out of a PoC to demonstrate how key functionality could be provided in alignment with architecture decisions and process model requirements.

- At the core is storage with performant read and write, search and filtering of the time series data
- Good descriptive metadata is key to findability
- A wide selection of math and statistics libraries is key for calculations and models
- Visualisation tools play a role both in ad hoc and routine inspection and quality control
- Workflow integration with automation and process monitoring help keeping consistent quality
- Data lineage and process metadata is essential for quality control

It is constructed to be an abstraction between the storage and automation layers and the statistics production code. providing a way forward while postponing some technical choices.

## Feature summary

The library is `Dataset` centric: One or more series form a set that is read or written at the same time.
All series in a set must be of the same type and should stem from the same process.

All series in a set being of the same type and otherwise complying with the underlying [information model](https://statisticsnorway.github.io/ssb-timeseries/info-model.html) simplifies the implementation of storage, descriptive metadata and search and enables key calculation features:

- Since each series is represented as a column vector in a dataset matrix, *linear algebra* is readily available. Datasets can be added, subtracted, multiplied and divided with each other and dataframes, matrices, vectors (untested) and scalars according to normal rules.
- *Time algebra* features allow up- and downsamliong that make use of the date columns. Basic time aggregation:
`Dataset.groupby('quarter', 'sum'|'mean'|'auto')`

- *Metadata calculations* uses the descriptions of the individual series for calculations ranging from simple things like unit conversions to using relations between entities in tag values to group series for aggregation.

It is not a strict requirement that all series in a set are written at the same time, but [managing workflows](https://statisticsnorway.github.io/ssb-timeseries/workflow.html) becomes much simpler if they are.

The `io module` connects the dataset to helper class(es) that takes care of reading and writing data.
This structure abstracts away the IO mechanics, so that the user do not need to know about implementation details, but only the _information model meaning_ of the choices made.
Also, although the current implementation uses pyarrow and parquet data structures under the hood,
by replacing the io-module, a database could be used instead.

## Documentation

Extensive [documentation](https://statisticsnorway.github.io/ssb-timeseries) is available on GitHub Pages, notably:

 * the [quickstart guide],
 * the detailed [API reference], and
 * (soon) tutorials.

In addition, you will find articles elaborating on topics like

 * [the overall design](https://statisticsnorway.github.io/ssb-timeseries/structure.html),
 * [the information model](https://statisticsnorway.github.io/ssb-timeseries/info-model.html), and
 * [workflow integration](https://statisticsnorway.github.io/ssb-timeseries/workflow.html).

## Contributing

Feedback and code contributions are welcome. See the [Contributor Guide] for how.

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
