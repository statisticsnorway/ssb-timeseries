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
  - Resampling to other frequencies.
  - Sampling and aggregations (group by).
  - Changing types.
  - Moving average.

Planned extensions:
  - Indexing
  - Diff, shift, cumsum

Proper timeseries analysis and seasonal adjustment are covered by external libraries, demonstrated later in this guide (see Timeseries analysis).
<!---->
The quintessential time functions work along the time axis.
<!---->
## Prerequisites

``` {note}
The guide assumes that the SSB Timeseries library is installed and that a working configuration is active.
See [the quickstart guide](quickstart) for instructions to that.
```

The presented functionality relies on `dataset.Dataset`.
Other imports like `types.AS_OF_AT` and external libraries are used only for generating the sample data.

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
from ssb_timeseries.types import AS_OF_AT
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
    difference between two versions would yield nothing but zeros.
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
            data_type=AS_OF_AT,
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
    as_of_dates=[date(*d) for d in product({2025, 2025}, range(1, 13), {1})],
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
Interval support and filtering by dates is an underdeveloped area of functionality.

For now, relying on the functionality of external libraries provides a workaround.
`Dataset.data` can easily be converted to the dataframe formats of other libraries:
Shorthand properties `Dataset.pa`, `.nw`, `.pd`, and `.pl` return Arrow tables, and Narwhals, Pandas and Polars dataframes.

Here with Polars:

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
| 2035-12-31 23:00:00 UTC | 6.131594e6 | 5.900607e6 | 468211.0 | 5.900501e6 | 1.074285e7 |
| 2036-12-31 23:00:00 UTC | 6.14462e6 | 5.9323e6 | 476797.0 | 5.938974e6 | 1.0800753e7 |
| 2037-12-31 23:00:00 UTC | 6.143058e6 | 5.930998e6 | 481246.0 | 5.94589e6 | 1.083013e7 |
| 2038-12-31 23:00:00 UTC | 6.157564e6 | 5.957122e6 | 487276.0 | 5.981659e6 | 1.0857581e7 |

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
<!---->
Resample
--------
<!---->
[`Dataset.resample`](../reference/ssb_timeseries.dataset) alters the frequency of the data itself.
Upsampling to a higher frequency fills in the missing periods with a fill method, `ffill` or `bfill`.
Downsampling to a lower frequency aggregates the periods with one of the simple aggregations listed in the reference
(`min`, `max`, `sum`, `mean`, `median`, `std`, `var`, `count`, `first`, `last`).

```python {.marimo}
# Check the size of our data slice:
print(jul.data.shape)
```

<!-- @output:AjVT -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">(20, 6)
</pre>

```python {.marimo}
yearly_to_daily = jul.resample("D", "ffill")
print(yearly_to_daily.data.shape)
# for the same number of series, roughly 365 times as many values
```

<!-- @output:pHFh -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">(6941, 6)
</pre>

```python {.marimo}
yearly_to_daily.data
```

<!-- @output:NCOB -->

| valid_at | Denmark | Finland | Iceland | Norway | Sweden |
| --- | --- | --- | --- | --- | --- |
| 2026-12-31 23:00:00+00:00 | 5999124.0 | 5712097.0 | 405190.0 | 5655535.0 | 10614964.0 |
| 2027-01-01 23:00:00+00:00 | 5999124.0 | 5712097.0 | 405190.0 | 5655535.0 | 10614964.0 |
| 2027-01-02 23:00:00+00:00 | 5999124.0 | 5712097.0 | 405190.0 | 5655535.0 | 10614964.0 |
| 2027-01-03 23:00:00+00:00 | 5999124.0 | 5712097.0 | 405190.0 | 5655535.0 | 10614964.0 |
| 2027-01-04 23:00:00+00:00 | 5999124.0 | 5712097.0 | 405190.0 | 5655535.0 | 10614964.0 |
| ... | ... | ... | ... | ... | ... |
| 2045-12-27 23:00:00+00:00 | 6194127.0 | 6058331.0 | 516093.0 | 6078766.0 | 11079612.0 |
| 2045-12-28 23:00:00+00:00 | 6194127.0 | 6058331.0 | 516093.0 | 6078766.0 | 11079612.0 |
| 2045-12-29 23:00:00+00:00 | 6194127.0 | 6058331.0 | 516093.0 | 6078766.0 | 11079612.0 |
| 2045-12-30 23:00:00+00:00 | 6194127.0 | 6058331.0 | 516093.0 | 6078766.0 | 11079612.0 |
| 2045-12-31 23:00:00+00:00 | 6229368.0 | 6101410.0 | 524242.0 | 6104776.0 | 11105489.0 |

Forward fill carries each annual projection forward day by day until the next annual value arrives.
Backward fill (`bfill`) instead lets the next annual value populate the days before it; it suits calendars where the time point marks the end of a period.
Downsampling the daily data again averages the values inside each new period.
It reproduces the annual levels closely, but not exactly at the boundaries, because the daily grid and the annual time points do not coincide.

```python {.marimo}
daily_to_yearly = yearly_to_daily.resample("YE", "mean")
```

```python {.marimo}
daily_to_yearly.data
```

<!-- @output:TXez -->

| valid_at | Denmark | Finland | Iceland | Norway | Sweden |
| --- | --- | --- | --- | --- | --- |
| 2027-12-30 23:00:00+00:00 | 5999124.0 | 5712097.0 | 405190.0 | 5655535.0 | 10614964.0 |
| 2028-12-30 23:00:00+00:00 | 6012748.0 | 5720839.0 | 413849.0 | 5683318.0 | 10579189.0 |
| 2029-12-30 23:00:00+00:00 | 6026108.0 | 5737104.0 | 421331.0 | 5708787.0 | 10579011.0 |
| 2030-12-30 23:00:00+00:00 | 6036643.0 | 5783166.0 | 426574.0 | 5752039.0 | 10661943.0 |
| 2031-12-30 23:00:00+00:00 | 6063536.0 | 5780553.0 | 435417.0 | 5793260.0 | 10617828.0 |
| ... | ... | ... | ... | ... | ... |
| 2042-12-30 23:00:00+00:00 | 6167750.0 | 6013029.0 | 504929.0 | 6034104.0 | 10979681.0 |
| 2043-12-30 23:00:00+00:00 | 6188418.0 | 6044526.0 | 507247.0 | 6074699.0 | 10972024.0 |
| 2044-12-30 23:00:00+00:00 | 6205124.0 | 6038939.0 | 513241.0 | 6066140.0 | 11047302.0 |
| 2045-12-30 23:00:00+00:00 | 6194127.0 | 6058331.0 | 516093.0 | 6078766.0 | 11079612.0 |
| 2046-12-30 23:00:00+00:00 | 6229368.0 | 6101410.0 | 524242.0 | 6104776.0 | 11105489.0 |

Group by
--------
<!---->
[`Dataset.group_by`](../reference/ssb_timeseries.dataset) aggregates over *calendar* periods rather than changing the frequency.
`freq` is an alias enumerated in the group_by reference, with the same meaning on all backends:
`year` (`y`/`yr`), `month` (`m`/`mth`), `quarter` (`q`), `week` (`w`/`wk`), or `raw` for already-formatted values.
`func` may be a function name or a list of function names that apply to every series, or `agg_mapping` may map functions to specific series; `tz` converts the time column before grouping, and `time_col` picks the column to group on.

```python {.marimo}
jul.pl.describe().select(pl.col(["statistic", "valid_at"]))
```

<!-- @output:wlCL -->

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

```python {.marimo}
weekly = yearly_to_daily.group_by("week", "mean")
```

```python {.marimo}
weekly.data.head(6)
```

<!-- @output:wAgl -->

| valid_at | Denmark_week_mean | Finland_week_mean | Iceland_week_mean | Norway_week_mean | Sweden_week_mean |
| --- | --- | --- | --- | --- | --- |
| 2026-53 | 5999124.0 | 5712097.0 | 405190.0 | 5655535.0 | 10614964.0 |
| 2027-01 | 5999124.0 | 5712097.0 | 405190.0 | 5655535.0 | 10614964.0 |
| 2027-02 | 5999124.0 | 5712097.0 | 405190.0 | 5655535.0 | 10614964.0 |
| 2027-03 | 5999124.0 | 5712097.0 | 405190.0 | 5655535.0 | 10614964.0 |
| 2027-04 | 5999124.0 | 5712097.0 | 405190.0 | 5655535.0 | 10614964.0 |
| 2027-05 | 5999124.0 | 5712097.0 | 405190.0 | 5655535.0 | 10614964.0 |

```python {.marimo}
quarterly = yearly_to_daily.group_by("quarter", "mean", tz="Europe/Oslo")
```

```python {.marimo}
quarterly.data.head(6)
```

<!-- @output:dGlV -->

| valid_at | Denmark_quarter_mean | Finland_quarter_mean | Iceland_quarter_mean | Norway_quarter_mean | Sweden_quarter_mean |
| --- | --- | --- | --- | --- | --- |
| 2027-Q1 | 5999124.0 | 5712097.0 | 405190.0 | 5655535.0 | 10614964.0 |
| 2027-Q2 | 5999124.0 | 5712097.0 | 405190.0 | 5655535.0 | 10614964.0 |
| 2027-Q3 | 5999124.0 | 5712097.0 | 405190.0 | 5655535.0 | 10614964.0 |
| 2027-Q4 | 5999124.0 | 5712097.0 | 405190.0 | 5655535.0 | 10614964.0 |
| 2028-Q1 | 6012748.0 | 5720839.0 | 413849.0 | 5683318.0 | 10579189.0 |
| 2028-Q2 | 6012748.0 | 5720839.0 | 413849.0 | 5683318.0 | 10579189.0 |

Moving average
--------------

```python {.marimo}
four_week_average = weekly.moving_average(-3, 0)
```

```python {.marimo}
four_week_average.pl
```

<!-- @output:yOPj -->

| valid_at | Denmark_week_mean | Finland_week_mean | Iceland_week_mean | Norway_week_mean | Sweden_week_mean |
| --- | --- | --- | --- | --- | --- |
| str | f64 | f64 | f64 | f64 | f64 |
| "2026-53" | NaN | NaN | NaN | NaN | NaN |
| "2027-01" | NaN | NaN | NaN | NaN | NaN |
| "2027-02" | NaN | NaN | NaN | NaN | NaN |
| "2027-03" | 5.999124e6 | 5.712097e6 | 405190.0 | 5.655535e6 | 1.0614964e7 |
| "2027-04" | 5.999124e6 | 5.712097e6 | 405190.0 | 5.655535e6 | 1.0614964e7 |
| … | … | … | … | … | … |
| "2045-49" | 6.194127e6 | 6.058331e6 | 516093.0 | 6.078766e6 | 1.1079612e7 |
| "2045-50" | 6.194127e6 | 6.058331e6 | 516093.0 | 6.078766e6 | 1.1079612e7 |
| "2045-51" | 6.194127e6 | 6.058331e6 | 516093.0 | 6.078766e6 | 1.1079612e7 |
| "2045-52" | 6.194127e6 | 6.058331e6 | 516093.0 | 6.078766e6 | 1.1079612e7 |
| "2046-01" | 6.2029e6 | 6.0691e6 | 518130.25 | 6085268.5 | 1.1086e7 |

```python {.marimo}
# four_week_average[{'country':'Norway'}].plot()
four_week_average['Norway_week_mean'].pd.plot()
```

<!-- @output:fwwy -->

![png](calc-with-time_assets/figure-2.png)

Note: `.pd.` above is a workaround for a missing link in sampling functionality: we got`valid_at` formatted as interval name strings rather than datetimes. Working along the time axes requires series type conversions.
<!---->
Timeseries analysis
-------------------
<!---->
Time series analysis and seasonal adjustment are large functional areas beyond the scope of SSB Timeseries.
The Python ecosystem offers several libraries that cover different parts of the territory.
[Nixtla](https://nixtlaverse.nixtla.io/statsforecast/), [Darts](https://unit8co.github.io/darts/), [statsmodels](https://www.statsmodels.org/) and [Prophet](https://facebook.github.io/prophet/) are common choices.

A key design principle is to maintain the flexibility to choose which best of breed library does the heavy lifting.
While the library does indeed provide *some* calculation functionality (clearly seen above), its main purpose is to bundles the information model with sufficient core data management and manipulation features so that the inbetween stuff becomes easy.

The external libraries make different assumptions about the data.
Available data exchange surfaces are covered in more detail in the [interoperability](interoperability) guide.

We saw a glimpse of the `Dataset.data` level adapters above.
Another class of frame adapters, apply to the [`Series`](../reference/ssb_timeseries.series) objects that can be accessed through iteration over a `Dataset`.

The following demonstrates the use of one of them, `Series.nixtla()`.

```python {.marimo}
from statsforecast import StatsForecast
from statsforecast.models import AutoARIMA
```

```python {.marimo}
norway_series = next(s for s in jul if s.name == "Norway") # to filter or not to filter?
```

```python {.marimo}
norway_series
```

<!-- @output:YWSi -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">&lt;ssb_timeseries.series.series.Series object at 0x7fca165cf490&gt;</pre>

The `Series` object contains the data and tags for a single series.

```python {.marimo}
norway_series.data.to_polars()
```

<!-- @output:tZnO -->

| valid_at | Norway |
| --- | --- |
| datetime[ns, UTC] | f64 |
| 2026-12-31 23:00:00 UTC | 5.655535e6 |
| 2027-12-31 23:00:00 UTC | 5.683318e6 |
| 2028-12-31 23:00:00 UTC | 5.708787e6 |
| 2029-12-31 23:00:00 UTC | 5.752039e6 |
| 2030-12-31 23:00:00 UTC | 5.79326e6 |
| … | … |
| 2041-12-31 23:00:00 UTC | 6.034104e6 |
| 2042-12-31 23:00:00 UTC | 6.074699e6 |
| 2043-12-31 23:00:00 UTC | 6.06614e6 |
| 2044-12-31 23:00:00 UTC | 6.078766e6 |
| 2045-12-31 23:00:00 UTC | 6.104776e6 |

Nixtla specifies its own format, requiring columns `unique_id`, `ds` and `y`. The "adapter" is just a method that provides it:

```python {.marimo}
norway_series.nixtla()
```

<!-- @output:CLip -->

| ds | y | unique_id |
| --- | --- | --- |
| 2026-12-31 23:00:00+00:00 | 5655535.0 | POPU06::Norway |
| 2027-12-31 23:00:00+00:00 | 5683318.0 | POPU06::Norway |
| 2028-12-31 23:00:00+00:00 | 5708787.0 | POPU06::Norway |
| 2029-12-31 23:00:00+00:00 | 5752039.0 | POPU06::Norway |
| 2030-12-31 23:00:00+00:00 | 5793260.0 | POPU06::Norway |
| ... | ... | ... |
| 2041-12-31 23:00:00+00:00 | 6034104.0 | POPU06::Norway |
| 2042-12-31 23:00:00+00:00 | 6074699.0 | POPU06::Norway |
| 2043-12-31 23:00:00+00:00 | 6066140.0 | POPU06::Norway |
| 2044-12-31 23:00:00+00:00 | 6078766.0 | POPU06::Norway |
| 2045-12-31 23:00:00+00:00 | 6104776.0 | POPU06::Norway |

```python {.marimo}
forecast = StatsForecast(
    models=[AutoARIMA(season_length=1)],
    freq="YE",
).forecast(df=norway_series.nixtla(), h=5)
```

`nixtla()` returns the Nixtla long format (`unique_id`, `ds`, `y`), one table per series.
Forecasts for several series can be combined by iterating the `Dataset` and calling `nixtla()` for each `Series` in turn.

```python {.marimo}
forecast
```

<!-- @output:iXej -->

| unique_id | ds | AutoARIMA |
| --- | --- | --- |
| POPU06::Norway | 2046-12-31 23:00:00+00:00 | 6.128420e+06 |
| POPU06::Norway | 2047-12-31 23:00:00+00:00 | 6.152065e+06 |
| POPU06::Norway | 2048-12-31 23:00:00+00:00 | 6.175709e+06 |
| POPU06::Norway | 2049-12-31 23:00:00+00:00 | 6.199353e+06 |
| POPU06::Norway | 2050-12-31 23:00:00+00:00 | 6.222997e+06 |

See also [Calculating with time](calc-with-time) or [Calculating with metadata](calc-with-metadata.md).
