---
title: Basic Usage
marimo-version: 0.24.0
width: comnpact
html_head_file: resources/custom.css
---

````python {.marimo hide_code="true"}
import marimo as mo
import testing

import inspect
import textwrap
from collections.abc import Callable
from typing import Any, TypeVar

F = TypeVar("F", bound=Callable[..., Any])

def prompt(cmd, str=''):
    """print a prompt with command before output str"""
    if cmd:
        return f"\n```\n>>> {cmd}\n\n{str}\n```\n"
    else:
        return  f"\n```\n{str}\n```\n"

from tabulate import tabulate
_tbl_format = 'simple'
_float_format=".2f"
def tbl(df, cmd=''):
    """print a str formatted table"""
    tbl_str = tabulate(
        df,
        headers = df.columns,
        tablefmt = _tbl_format,
        floatfmt =_float_format,
        showindex=False,
    )
    return prompt(cmd, tbl_str)

from ssb_timeseries.types import Versioning, Temporality
````

BEGIN
<!---->
# Basic Usage

## Introduction

The time series library was created to operate in lieu of a fully blown timeseries system.

Being a code libarary it does not provide any services, of its own, but is built to *manage* or *communicate* with a storage layer, and metadata and workflow orchestration services.

Its main responsibility is to connect an information model and analytic features to loosely coupled services that combines into a complete solution.
It is designed mainly to facilitate quantitative analysis in code, but can also be the foundation for GUI applications for charts, tables and dashboards.

That means it provides (or connects to) all the main building blocks required to build a full, but lightweight, timeseries system.

This guide focuses on the most basic operations: reading and writing `Datasets`, and some basic features and manipulations.
<!---->
Configuration depencency:
The code below assumes access to a working configuration.
See the [Quick start guide](quickstart.md) for how to prepare it.

```python {.marimo}
import ssb_timeseries as ts
```

## Datasets

The statistics production processes are largely batch oriented and encourages writing vectorised code.
The library is therefore constructed to work with *datasets* as a primary unit of analysis.
While it is possible to work on individual series or values, the library is built around the idea that datasets are matrices and series are vectors.

Practically, this means that the `Dataset` class is the very core of the library.
Reads and writes (and most other core functionality) operates on the dataset level.
<!---->
### Create a dataset

To create our first `Dataset` we need some data.
Let us generate a dataframe `df` with some random data for three series.
The library used to create the dataframe (here: Pandas) does not matter, [any Narwhals compatible library will do](compatibility.md).

```python {.marimo}
from ssb_timeseries import sample_data
df = sample_data.xyz_at()
```

```python {.marimo}

```

```python {.marimo}
mo.md(f"""
Now we have a `df` as follows {tbl(df, 'df')}
""")
```

Note the structure: one shared `valid_at` column and one column for each of the series  `x`, `y` and `z`. The single date signifies a "point in time" *temporality*, `Temporality.AT`.

The temporality can be combined with any variant of *versioning* to define a `SeriesType` for a `Dataset`
For now, let us go with `Versioning.NONE`.

```python {.marimo}
from ssb_timeseries.types import SeriesType
POINT_IN_TIME = SeriesType('NONE', 'AT')
```

```python {.marimo}
mo.md(f"""
`SeriesType('NONE', 'AT')` is a shorthand that resolves to `{repr(SeriesType(Versioning.NONE, Temporality.AT))}`.

See the [core concepts](..info-model) and [the datatypes tutorial]() for more about data types.
""")
```

This is all we need to define a `Dataset` object. Let us call it "XYZ".

```python {.marimo}
from ssb_timeseries.dataset import Dataset
xyz = Dataset("XYZ", data_type=POINT_IN_TIME, data=df,)
```

```python {.marimo hide_code="true"}
mo.md(f"""
We now have a `Dataset` object with name "XYZ" assigned to the variable `xyz`.
The object lives in memory only untill we save it.

{prompt('xyz',repr(xyz))}
""")
```

### Write a dataset
<!---->
If more than one repository is configured, we can specify which we want to write to.
If we don't, an existing set will be written to the repository it was read from and a new set will be written to the default repository.

```python {.marimo}
xyz.save()
```

### Dataset .data and .tags

```python {.marimo hide_code="true"}
mo.md(f"""
Inspect the data: {prompt('xyz.data')}
""")
```

```python {.marimo}
mo.md(f"""
{xyz.pd.to_markdown()}
""")
```

```python {.marimo hide_code="true"}
mo.md(f"""
... and the tags: {prompt('xyz.tags','')}

{mo.tree(xyz.tags)}
""")
```

For this sample set, we have only a minimal set of technical attributes.
<!---->
The metadata captures selected technical attributes, but also any number of descriptive attributes, or "tags".
Tags apply at both the `Dataset` and `Series` levels.
The technical implementation for the tags is a key-value structure in the form of a Python `dictionary`.
Some rules and conventions that apply are described in the [core concepts](info-model), and other guides go deeper into [search and filtering](), [tag maintenance]() and [calculations with metadata]().
Beyond the mandatory technical attributes, the library
<!---->
### Read a dataset
<!---->
For an existing dataset, initating a `Dataset("<name>")` will read it.
If we do not specify and interval, reading will collect either all the data (for `Versioning.NONE`) or the latest version (for `Versioning.AS_OF`).

```python {.marimo}
read_xyz_back = Dataset("XYZ")
```

Note the `std_out`.
Both reading and writing was logged.
The logging behaviour can be modified in the configuration.
This can be leveraged for orchestration: adding a queue or API logger allows even driven workflows.
<!---->
### Deriving new datasets

The library has built in funcitonality for basic arithmetics and linear algebra, calculations with time and calculations with metadata.
<!---->
Other groups have limited or no functionality at the time of writing, but may be added later:
- Logical functions
- Set functions
- Unit conversion
- Currency conversion
- Indexing

```python {.marimo}
check_equality = (xyz == read_xyz_back)
```

The calculation returns a new `Dataset` object.
Inspect the data to very that all values are equal:

```python {.marimo hide_code="true"}
mo.md(f"""
{check_equality.pd.to_markdown()}
""")
```

The boolean return type is semi-supported for now:
That is sufficient for intermediate calculations, but most functionality will fail and the storage model will require casting to numbers.

A more direct check for the test above is `Dataset.all()` to check if all the values for the series (ie. not the dates) evaluate to `True`.

```python {.marimo hide_code="true"}
mo.md(f"""
{prompt('check_equality.all()',check_equality.all())}
""")
```

The new dataset object got a new name automatically generated.
The same thing can be observed for the filter and multiplication below.

A new dataset object with a new name is the standard behaviour for all [calculations](calculations.md) performed by the library.
The new name is an important safeguard against destroying data.
The "lineage naming" that tells which operations were performed hints about the larger topic of [data lineage](lineage.md).

In real production code, the calculation of any dataset that we intend to save should be followed by a `Dataset.rename()` and updating the [descriptive metadata]() to reflect whichever calculations where performed.
Functions and guidelines for [metadata maintenance](meta-tag-maintenance) is a topic in itself.

```python {.marimo}
big_xyz = xyz['x', 'y'] *1000
big_xyz.plot()
```

Planned: Iterators
------------------

While many of the most used calculation features are implemented for the `Dataset` objects, iterating over `Series` ... --> TODO.
<!---->
END

```python {.marimo hide_code="true"}
def test_xyz_is_a_dataset():
    assert isinstance(xyz, Dataset)
```

```python {.marimo hide_code="true"}
def test_xyz_is_equal_to_itself():
    assert check_if_they_are_equal.all()
```

```python {.marimo hide_code="true"}
testing.run_and_report([test_xyz_is_a_dataset, test_xyz_is_equal_to_itself])
```

```python {.marimo hide_code="true"}

```
