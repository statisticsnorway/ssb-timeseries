import marimo

__generated_with = "0.24.0"
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
      - Sampling and aggregations (group by)
      - Changing types.
      - Moving average.

    Planned extensions:
      - Indexing
      - Diff, shift, cumsum

    Proper timeseries analysis and seasonal adjustment. (Planned integrations.)
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

    return POPU06_MAIN_COUNTRIES, SeriesType, create_df, date, np, popu06, product


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
        as_of_dates=[date(*d) for d in product({2024, 2025}, range(1, 13), {1})],
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
    return change_from_feb_to_july, jul


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
    Narwhals also brings conversion of `Dataset.data` to other libraries and their functionality within short reach.
    Shorthand properties `Dataset.pa`, `.nw`, `.pd`, and `.pl` return Arrow tables, and Narwhals, Pandas and Polars dataframes.

    Interval support and filtering by dates is an underdeveloped area of functionality.
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

    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    Group by
    --------
    """)
    return


@app.cell
def _(jul, pl):
    jul.pl.describe().select(pl.col(["statistic", "valid_at"]))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    The projections are annual, so the data is aggregated over five year periods rather than quarters.
    """)
    return


@app.cell
def _(jul):
    jul.data = jul.pd # workaround for BUG!
    return


@app.cell
def _(jul):
    five_year = jul.groupby('5Y','mean')
    return (five_year,)


@app.cell
def _(five_year):
    five_year.data
    return


@app.cell
def _():
    return


@app.cell
def _(five_year):
    five_year.pd.plot()
    # sum --> strange first value because of tz conversion / and not full period
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Moving average
    --------------
    """)
    return


@app.cell
def _(five_year):
    rolling_5y_avg = five_year.moving_average(-4,-1)
    return (rolling_5y_avg,)


@app.cell
def _(rolling_5y_avg):
    rolling_5y_avg.data
    return


@app.cell
def _():
    # Observe BUG: valid_at as period_index converted to number
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(f"""
    See also [Calculating with time](calc-with-time) or [Calculating with metadata](calc-with-meta-tags).
    """)
    return


@app.cell(hide_code=True)
def _(change_from_feb_to_july, feb, jul):
    # @supress

    def test_popu06_has_one_series_per_country():
        assert set(jul.series) == {"Denmark", "Finland", "Iceland", "Norway", "Sweden"}

    def test_popu06_spans_the_common_projection_period():
        assert len(jul.data["valid_at"]) == 20
        assert feb.series == jul.series

    def test_popu06_versions_are_not_identical():
        difference = change_from_feb_to_july.data["Norway"].to_pandas()
        assert difference.abs().sum() > 0

    return (test_popu06_has_one_series_per_country, test_popu06_spans_the_common_projection_period, test_popu06_versions_are_not_identical)


@app.cell(hide_code=True)
def _(test_popu06_has_one_series_per_country, test_popu06_spans_the_common_projection_period, test_popu06_versions_are_not_identical, testing):
    testing.run_and_report([
        test_popu06_has_one_series_per_country,
        test_popu06_spans_the_common_projection_period,
        test_popu06_versions_are_not_identical,
    ])
    return


if __name__ == "__main__":
    app.run()
