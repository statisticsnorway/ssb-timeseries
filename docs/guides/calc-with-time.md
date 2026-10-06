---
title: Calc With Time
marimo-version: 0.24.2
---

Calculating with time
=====================
<!---->
Scope
-----

This guide demonstrates handling of time.

We repeat ever so briefly some examples are covered in more detail elsewhere.

- Storage implementation: UTC under the hood.
- Calculating differeneces between versions identified by `as_of`-dates is simple arithmetics after retrieving a data.
- Interval for data retrieval and simple filtering after retrieval of the data along the time axis using time aware functionality of other libararies.
- Functions along the time axis:
  - Sampling and aggregations (group by)
  - Changing types.
  - Moving average.

Planned extensions:
  - Indexing
  - Diff, shift, cumsum

Proper timeseries analysis and seasonal adjustment. (Planned integrations.)
<!---->
The quintessential time functions work along the time axis.
<!---->
## Prerequisites

``` {note}
The guide assumes that the SSB Timeseries library is installed and that a working configuration is active.
See [the quickstart guide](quickstart) for instructions to that.
```

The presented functionality relies on `dataset.Dataset`.
Other imports like`types.SeriesType` and external libraries are used only for generating the sample data.

```python {.marimo}
from ssb_timeseries.dataset import Dataset
```

```python {.marimo}
from datetime import date
from itertools import product

import numpy as np

from ssb_timeseries.sample_data import POPU06_MAIN_COUNTRIES
from ssb_timeseries.sample_data import create_df
from ssb_timeseries.sample_data import popu06
from ssb_timeseries.types import SeriesType
```

Generate some test data

```python {.marimo}
def create_popu06_versions(
    as_of_dates: list[date],
    countries: tuple[str, ...],
    noise: float = 0.002,
    seed: int = 20251001,
):
    """Save one version of the Nordic population projections for each as-of date.

    The projections themselves are static and reproducible, so a small seeded
    perturbation is applied to give each version its own values.
    Without it every version would hold identical numbers and calculating
    between two versions would yield nothing but zeros.
    """
    base = popu06(countries=countries, start_year=2027, end_year=2046)
    generator = np.random.default_rng(seed)
    for as_of in as_of_dates:
        version = base.copy()
        for country in countries:
            jitter = 1 + noise * generator.standard_normal(len(version))
            version[country] = (
                version[country].to_numpy() * jitter
            ).round().astype("int64")
        Dataset(
            name="POPU06",
            data_type=SeriesType("AS_OF", "AT"),
            as_of_tz=str(as_of),
            data=version,
            tags={
                "source": "POPU06",
                "table": "Population projections by age and sex, total",
            },
        ).save()
```

We will store the population projections published in the Nordic Statistics database table
[POPU06](https://pxweb.nordicstatistics.org), one series per country.
The same projections are stored once per `as_of`-date, so that versions can be compared.

```python {.marimo}
create_popu06_versions(
    as_of_dates=[date(*d) for d in product({2024, 2025}, range(1, 13), {1})],
    countries=POPU06_MAIN_COUNTRIES,
)
```

Element-wise arithmetic
--------------------------------
<!---->
Our dataset "POPU06" contains *population projections* for the Nordic countries.

[Basic arithmetic](calc-basic-arithmetic) may be performed on same size data:

```python {.marimo}
jul = Dataset(name="POPU06", as_of_tz="2025-07-01")
feb = Dataset(name="POPU06", as_of_tz="2025-02-01")

change_from_feb_to_july = jul - feb
```

```python {.marimo}
change_from_feb_to_july.plot()
```

<!-- @output:iLit -->

![png](calc-with-time_assets/figure-1.png)

The Numpy implementation means that element-wise calculation is the default, with [Numpy "broadcasting rules"](https://numpy.org/doc/stable/user/basics.broadcasting.html) for different size objects.
Broadcasting rules and dimensional conditions are avaluated only for the numeric parts - the math functions will ignore the date columns.
Date alignment must be performed explicitly prior to the calculation.
<!---->
Narwhals under the hood first and foremost allow the arithmetic functions support operating not only on `Dataset` objects, but on combinations of datasets with a large number of other datatypes (scalars, Numpy arrays, dataframes, Arrow tables).
Note that the "dataframe like" objects are all conflated to 'df' in the lineage tracking.

Filter by dates
---------------------------------------
<!---->
Narwhals also brings conversion of `Dataset.data` to other libraries and their functionality within short reach.
Shorthand properties `Dataset.pa`, `.nw`, `.pd`, and `.pl` return Arrow tables, and Narwhals, Pandas and Polars dataframes.

Interval support and filtering by dates is an underdeveloped area of functionality.

```python {.marimo}
import polars as pl

d_from = date(2035, 1, 1)
d_to = pl.date(2039, 12, 31)
```

```python {.marimo}
x_row = jul.pl.filter( pl.col("valid_at").is_between(d_from, d_to) )
```

```python {.marimo}
x_row
```

<!-- @output:ZBYS -->

| valid_at | Denmark | Finland | Iceland | Norway | Sweden |
| --- | --- | --- | --- | --- | --- |
| datetime[ns, UTC] | f64 | f64 | f64 | f64 | f64 |
| 2035-12-31 23:00:00 UTC | 6.134939e6 | 5.899144e6 | 468533.0 | 5.908382e6 | 1.0749026e7 |
| 2036-12-31 23:00:00 UTC | 6.140989e6 | 5.921987e6 | 476381.0 | 5.95139e6 | 1.0811669e7 |
| 2037-12-31 23:00:00 UTC | 6.130022e6 | 5.944316e6 | 479762.0 | 5.937953e6 | 1.0844831e7 |
| 2038-12-31 23:00:00 UTC | 6.157238e6 | 5.978206e6 | 487794.0 | 5.956446e6 | 1.0855639e7 |

\# bigger example - not needed?
tags = {"Var": ["price", "volume"], \
        "Product": ["milk", "eggs", "bread", "cheese", "ham"], \
        "Store": ["A", "B", "C", "D", "E"], \
        "Region": ["N", "S", "E", "W", "NE", "NW", "SE", "SW"]}

some_data = create_df(
    *[value for value in tags.values()],
    start_date="2000-12-01",
    end_date="2024-01-01",
    freq="MS",
    implementation="pandas").set_index('valid_at')
some_data.info()

Group by
--------

```python {.marimo}
jul.pl.describe().select(pl.col(["statistic", "valid_at"]))
```

<!-- @output:AjVT -->

| statistic | valid_at |
| --- | --- |
| str | str |
| "count" | "20" |
| "null_count" | "0" |
| "mean" | "2036-07-01 23:00:00+00:00" |
| "std" | null |
| "min" | "2026-12-31 23:00:00+00:00" |
| "25%" | "2031-12-31 23:00:00+00:00" |
| "50%" | "2036-12-31 23:00:00+00:00" |
| "75%" | "2040-12-31 23:00:00+00:00" |
| "max" | "2045-12-31 23:00:00+00:00" |

The projections are annual, so the data is aggregated over five year periods rather than quarters.

```python {.marimo}
jul.data = jul.pd # workaround for BUG!
```

```python {.marimo}
five_year = jul.groupby('5Y','mean')
```

```python {.marimo}
five_year.data
```

<!-- @output:TRpd -->

| Denmark | Finland | Iceland | Norway | Sweden |
| --- | --- | --- | --- | --- |
|  |  |  |  |  |
| 5999359.0 | 5672363.0 | 406313.0 | 5669951.0 | 10615636.0 |
| 5996705.0 | 5721457.0 | 412917.0 | 5684376.0 | 10625613.0 |
| 6027401.0 | 5716670.0 | 420793.0 | 5716418.0 | 10611853.0 |
| 6080182.0 | 5771599.0 | 427802.0 | 5722952.0 | 10591888.0 |
| 6050448.0 | 5766510.0 | 434670.0 | 5758831.0 | 10596849.0 |
| ... | ... | ... | ... | ... |
| 6183954.0 | 5987105.0 | 503685.0 | 6036619.0 | 10964012.0 |
| 6182232.0 | 6049960.0 | 506589.0 | 6043889.0 | 11004110.0 |
| 6208393.0 | 6045014.0 | 512525.0 | 6072128.0 | 11013343.0 |
| 6170663.0 | 6063181.0 | 519735.0 | 6085844.0 | 11114163.0 |
| 6195530.0 | 6089520.0 | 521892.0 | 6106436.0 | 11117373.0 |

```python {.marimo}

```

```python {.marimo}
five_year.pd.plot()
# sum --> strange first value because of tz conversion / and not full period
```

<!-- @output:dNNg -->

![png](calc-with-time_assets/figure-2.png)

Moving average
--------------

```python {.marimo}
rolling_5y_avg = five_year.moving_average(-4,-1)
```

```python {.marimo}
rolling_5y_avg.data
```

<!-- @output:kqZH -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">pyarrow.Table
Denmark: double
Finland: double
Iceland: double
Norway: double
Sweden: double
valid_at: extension&lt;pandas.period&lt;ArrowPeriodType&gt;&gt;
----
Denmark: &#91;&#91;nan,nan,nan,nan,6025911.75,...,6158955.75,6172438.75,6178687.25,6186581,6186310.5&#93;&#93;
Finland: &#91;&#91;nan,nan,nan,nan,5720522.25,...,5979422.25,5990119.5,6008058,6024453.5,6036315&#93;&#93;
Iceland: &#91;&#91;nan,nan,nan,nan,416956.25,...,489898.5,495879.25,500578,505516.25,510633.5&#93;&#93;
Norway: &#91;&#91;nan,nan,nan,nan,5698424.25,...,5972349.25,5997015.75,6018876.5,6038651.5,6059620&#93;&#93;
Sweden: &#91;&#91;nan,nan,nan,nan,10611247.5,...,10890970.75,10920766,10957883.75,10981308,11023907&#93;&#93;
valid_at: &#91;&#91;56,57,58,59,60,...,71,72,73,74,75&#93;&#93;</pre>

```python {.marimo}
# Observe BUG: valid_at as period_index converted to number
```

See also [Calculating with time](calc-with-time) or [Calculating with metadata](calc-with-metadata.md).
