---
title: Getting Started
marimo-version: 0.23.1
---

Getting started
===============

```{eval-rst}
.. marimo:: getting_started.py
   :height: 700px
   :width: 100%
   :click-to-load: false

```

Assuming you have installed and configured the library, you are ready to start coding.
Refer to the [Quick start guide](quickstart.md)) for setup instructions.

<!-- name: test_getting_started; fixtures: buildup_and_teardown -->
```python
from ssb_timeseries.dataset import Dataset
```

If the configuration points to a repository with existing data,
data can be retrieved simply by instantiating a `dataset` object by its name.

<!-- name: test_getting_started; case: expect_to_fail; mark: xfail(raises=AttributeError) -->
```python
pqr = Dataset("PQR")
```

By default this will collect either all the data or the data corresponding to the latest version.
If the dataset does not exist, the call will throw an error.
To create a new dataset, we can specify additional parameters, notably .

Data types
----------

Types are defined by *versioning* and *temporality*:

<!-- name: test_getting_started -->
```python
from ssb_timeseries.types import SeriesType, Versioning, Temporality

POINT_IN_TIME = SeriesType(
    Versioning.NONE,
    Temporality.AT)

PERIOD = SeriesType(
    Versioning.NONE,
    Temporality.FROM_TO)

POINT_IN_TIME_ESTIMATE = SeriesType(
    Versioning.AS_OF,
    Temporality.AT)

PERIOD_ESTIMATE = SeriesType(
    Versioning.AS_OF,
    Temporality.FROM_TO)

```

These concepts impacts key behaviours and how the data is physically stored.
Versioned data require a version marker.

<!-- name: test_getting_started -->
```python
as_of = '2026-01-01'
```

Series data
-----------

Data should be provided as a Narwhals compatible) Dataframe or Arrow table.
A dataset can contain any number of numeric series, each represented as a dataframe column,
and shared datetime columns corresponding to the data type:

- a single `valid_at` for points in time
- a pair of `valid_from` (inclusive) and `valid_to` (exclusive)

<!-- name: test_getting_started
```python
import polars as pl
df = pl.DataFrame(
    {"valid_at": ["2026-01-01 11:37","2026-01-01 13:33"],
    "a": [1, 2],
    "b": [3, 4]},
    schema=[("valid_at", pl.Datetime), ("a", pl.int64), ("b", pl.int64)])
```
-->

Example: Point in time data
---------------------------

The `temporality = 'AT'` indicates that values are instantaneous, that is valid *at* an exact point in time.


<!-- name: test_getting_started; case: completed
```python
assert True
```
-->
