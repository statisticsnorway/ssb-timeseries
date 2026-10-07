import marimo

__generated_with = "0.24.2"
app = marimo.App()


@app.cell(hide_code=True)
def _():
    import marimo as mo
    import testing

    return mo, testing


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Calculating with time
    =====================
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
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
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    The quintessential time functions work along the time axis.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Prerequisites

    ``` {note}
    The guide assumes that the SSB Timeseries library is installed and that a working configuration is active.
    See [the quickstart guide](quickstart) for instructions to that.
    ```

    The presented functionality relies on `dataset.Dataset`.
    Other imports like`types.SeriesType` and external libraries are used only for generating the sample data.
    """)
    return


@app.cell
def _():
    from ssb_timeseries.dataset import Dataset

    return (Dataset,)


@app.cell
def _():
    from datetime import date
    from itertools import product

    import numpy as np

    from ssb_timeseries.sample_data import POPU06_MAIN_COUNTRIES
    from ssb_timeseries.sample_data import create_df
    from ssb_timeseries.sample_data import popu06
    from ssb_timeseries.types import SeriesType

    return POPU06_MAIN_COUNTRIES, SeriesType, date, np, popu06, product


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Generate some test data
    """)
    return


@app.cell
def _(Dataset, SeriesType, date, np, popu06):
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
                data_type=SeriesType("AS_OF", "AT"),
                as_of_tz=str(as_of),
                data=version,
                tags={
                    "source": "POPU06",
                    "table": "Population projections by age and sex, total",
                },
            ).save()

    return (create_popu06_versions,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    We will store the population projections published in the Nordic Statistics database table
    [POPU06](https://pxweb.nordicstatistics.org), one series per country.
    The same projections are stored once per `as_of`-date, so that versions can be compared.
    """)
    return


@app.cell
def _(POPU06_MAIN_COUNTRIES, create_popu06_versions, date, product):
    create_popu06_versions(
        as_of_dates=[date(*d) for d in product({2025, 2025}, range(1, 13), {1})],
        countries=POPU06_MAIN_COUNTRIES,
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Element-wise arithmetic
    --------------------------------
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Our dataset "POPU06" contains *population projections* for the Nordic countries.

    [Basic arithmetic](calc-basic-arithmetic) may be performed on same size data:
    """)
    return


@app.cell
def _(Dataset):
    jul = Dataset(name="POPU06", as_of_tz="2025-07-01")
    feb = Dataset(name="POPU06", as_of_tz="2025-02-01")

    change_from_feb_to_july = jul - feb
    return change_from_feb_to_july, feb, jul


@app.cell
def _(change_from_feb_to_july):
    change_from_feb_to_july.plot()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""

    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    The Numpy implementation means that element-wise calculation is the default, with [Numpy "broadcasting rules"](https://numpy.org/doc/stable/user/basics.broadcasting.html) for different size objects.
    Broadcasting rules and dimensional conditions are avaluated only for the numeric parts - the math functions will ignore the date columns.
    Date alignment must be performed explicitly prior to the calculation.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Narwhals under the hood first and foremost allow the arithmetic functions support operating not only on `Dataset` objects, but on combinations of datasets with a large number of other datatypes (scalars, Numpy arrays, dataframes, Arrow tables).
    Note that the "dataframe like" objects are all conflated to 'df' in the lineage tracking.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""

    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(f"""

    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Filter by dates
    ---------------------------------------
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Interval support and filtering by dates is an underdeveloped area of functionality.

    For now, relying on the functionality of external libraries provides a workaround.
    `Dataset.data` can easily be converted to the dataframe formats of other libraries:
    Shorthand properties `Dataset.pa`, `.nw`, `.pd`, and `.pl` return Arrow tables, and Narwhals, Pandas and Polars dataframes.

    Here with Polars:
    """)
    return


@app.cell
def _(date):
    import polars as pl

    d_from = date(2035, 1, 1)
    d_to = pl.date(2039, 12, 31)
    return d_from, d_to, pl


@app.cell
def _(d_from, d_to, jul, pl):
    x_row = jul.pl.filter( pl.col("valid_at").is_between(d_from, d_to) )
    return (x_row,)


@app.cell
def _(x_row):
    x_row
    return


@app.cell(disabled=True, hide_code=True)
def _(mo):
    mo.md(r"""
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
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Resample
    --------
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    [`Dataset.resample`](../reference/ssb_timeseries.dataset) alters the frequency of the data itself.
    Upsampling to a higher frequency fills in the missing periods with a fill method, `ffill` or `bfill`.
    Downsampling to a lower frequency aggregates the periods with one of the simple aggregations listed in the reference
    (`min`, `max`, `sum`, `mean`, `median`, `std`, `var`, `count`, `first`, `last`).
    """)
    return


@app.cell
def _(jul):
    # Check the size of our data slice:
    print(jul.data.shape)
    return


@app.cell
def _(jul):
    yearly_to_daily = jul.resample("D", "ffill")
    print(yearly_to_daily.data.shape)
    # for the same number of series, roughly 365 times as many values
    return (yearly_to_daily,)


@app.cell
def _(yearly_to_daily):
    yearly_to_daily.data
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Forward fill carries each annual projection forward day by day until the next annual value arrives.
    Backward fill (`bfill`) instead lets the next annual value populate the days before it; it suits calendars where the time point marks the end of a period.
    Downsampling the daily data again averages the values inside each new period.
    It reproduces the annual levels closely, but not exactly at the boundaries, because the daily grid and the annual time points do not coincide.
    """)
    return


@app.cell
def _(yearly_to_daily):
    daily_to_yearly = yearly_to_daily.resample("YE", "mean")
    return (daily_to_yearly,)


@app.cell
def _(daily_to_yearly):
    daily_to_yearly.data
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Group by
    --------
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    [`Dataset.group_by`](../reference/ssb_timeseries.dataset) aggregates over *calendar* periods rather than changing the frequency.
    `freq` is an alias enumerated in the group_by reference, with the same meaning on all backends:
    `year` (`y`/`yr`), `month` (`m`/`mth`), `quarter` (`q`), `week` (`w`/`wk`), or `raw` for already-formatted values.
    `func` may be a function name or a list of function names that apply to every series, or `agg_mapping` may map functions to specific series; `tz` converts the time column before grouping, and `time_col` picks the column to group on.
    """)
    return


@app.cell
def _(jul, pl):
    jul.pl.describe().select(pl.col(["statistic", "valid_at"]))
    return


@app.cell
def _(yearly_to_daily):
    weekly = yearly_to_daily.group_by("week", "mean")
    return (weekly,)


@app.cell
def _(weekly):
    weekly.data.head(6)
    return


@app.cell
def _(yearly_to_daily):
    quarterly = yearly_to_daily.group_by("quarter", "mean", tz="Europe/Oslo")
    return (quarterly,)


@app.cell
def _(quarterly):
    quarterly.data.head(6)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Moving average
    --------------
    """)
    return


@app.cell
def _(weekly):
    four_week_average = weekly.moving_average(-3, 0)
    return (four_week_average,)


@app.cell
def _(four_week_average):
    four_week_average.pl
    return


@app.cell
def _(four_week_average):
    # four_week_average[{'country':'Norway'}].plot()
    four_week_average['Norway_week_mean'].pd.plot()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Note: `.pd.` above is a workaround for a missing link in sampling functionality: we got`valid_at` formatted as interval name strings rather than datetimes. Working along the time axes requires series type conversions.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Timeseries analysis
    -------------------
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
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
    """)
    return


@app.cell
def _():
    from statsforecast import StatsForecast
    from statsforecast.models import AutoARIMA

    return AutoARIMA, StatsForecast


@app.cell
def _(jul):
    norway_series = next(s for s in jul if s.name == "Norway") # to filter or not to filter?
    return (norway_series,)


@app.cell
def _(norway_series):
    norway_series
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    The `Series` object contains the data and tags for a single series.
    """)
    return


@app.cell
def _(norway_series):
    norway_series.data.to_polars()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Nixtla specifies its own format, requiring columns `unique_id`, `ds` and `y`. The "adapter" is just a method that provides it:
    """)
    return


@app.cell
def _(norway_series):
    norway_series.nixtla()
    return


@app.cell
def _(AutoARIMA, StatsForecast, norway_series):
    forecast = StatsForecast(
        models=[AutoARIMA(season_length=1)],
        freq="YE",
    ).forecast(df=norway_series.nixtla(), h=5)
    return (forecast,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    `nixtla()` returns the Nixtla long format (`unique_id`, `ds`, `y`), one table per series.
    Forecasts for several series can be combined by iterating the `Dataset` and calling `nixtla()` for each `Series` in turn.
    """)
    return


@app.cell
def _(forecast):
    forecast
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(f"""
    See also [Calculating with time](calc-with-time) or [Calculating with metadata](calc-with-metadata.md).
    """)
    return


@app.cell(hide_code=True)
def _(change_from_feb_to_july, feb, forecast, jul):
    # @supress

    def test_popu06_has_one_series_per_country():
        assert set(jul.series) == {"Denmark", "Finland", "Iceland", "Norway", "Sweden"}

    def test_popu06_spans_the_common_projection_period():
        assert len(jul.data["valid_at"]) == 20
        assert feb.series == jul.series

    def test_popu06_versions_are_not_identical():
        difference = change_from_feb_to_july.data["Norway"].to_pandas()
        assert difference.abs().sum() > 0

    def test_forecast_spans_five_years():
        assert len(forecast) == 5

    return (
        test_forecast_spans_five_years,
        test_popu06_has_one_series_per_country,
        test_popu06_spans_the_common_projection_period,
        test_popu06_versions_are_not_identical,
    )


@app.cell(hide_code=True)
def _(
    test_forecast_spans_five_years,
    test_popu06_has_one_series_per_country,
    test_popu06_spans_the_common_projection_period,
    test_popu06_versions_are_not_identical,
    testing,
):
    testing.run_and_report([
        test_popu06_has_one_series_per_country,
        test_popu06_spans_the_common_projection_period,
        test_popu06_versions_are_not_identical,
        test_forecast_spans_five_years,
    ])
    return


if __name__ == "__main__":
    app.run()
