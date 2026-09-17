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
    from ssb_timeseries.types import SeriesType
    from ssb_timeseries.sample_data import create_df
    from itertools import product
    from datetime import date

    return SeriesType, create_df, date, product


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Generate some test data
    """)
    return


@app.cell
def _(Dataset, SeriesType, create_df, date):
    def create_some_example_data(
        set_name: str,
        as_of_dates: list[date],
        series_tags: dict[str,list[str]],
    ):
        """Generate and save some sample data."""
        set_tags = { "Country": "Norway" }
        for d in as_of_dates:
            df = create_df(
                *[value for value in series_tags.values()],
                temporality= 'AT',
                start_date="2025-01-01",
                end_date="2026-12-01",
                freq="D",
            )
            Dataset(
                name=set_name,
                data_type=SeriesType('AS_OF', 'AT'),
                as_of_tz=str(d),
                data=df,
                tags = set_tags,
                attributes = series_tags.keys(),
            ).save()

    return (create_some_example_data,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    We will generate random data for all permutations of some descriptive metadata,
    """)
    return


@app.cell
def _(create_some_example_data, date, product):
    create_some_example_data(
        set_name="Sample Data",
        as_of_dates = [date(*d) for d in product({2024,2025}, range(1,13), {1})],
        series_tags = {'area': ["x", "y","z"]}
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
    Our dataset "Prices and Volumes" contain *prices* and *volumes* for a number of *products*.

    [Basic arithmetic](calc-basic-arithmetic) may be performed on same size data:
    """)
    return


@app.cell
def _(Dataset):
    jul = Dataset(name="Sample Data", as_of_tz="2025-07-01")
    feb = Dataset(name="Sample Data", as_of_tz="2025-02-01")

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

    d_from = date(2024, 2, 22)
    d_to = pl.date(2024, 3, 2)
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
    Group by
    --------
    """)
    return


@app.cell
def _(jul):
    jul.data = jul.pd # workaround for BUG!
    return


@app.cell
def _(jul):
    quarterly = jul.groupby('Q','mean')
    return (quarterly,)


@app.cell
def _(quarterly):
    quarterly.data
    return


@app.cell
def _():
    return


@app.cell
def _(quarterly):
    quarterly.pd.plot()
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
def _(quarterly):
    rolling_4q_avg = quarterly.moving_average(-4,-1)
    return (rolling_4q_avg,)


@app.cell
def _(rolling_4q_avg):
    rolling_4q_avg.data
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
def _():
    # @supress
    def test_true():
        assert True

    # add better tests in this cell and add to list in cell below!
    return (test_true,)


@app.cell(hide_code=True)
def _(test_true, testing):
    testing.run_and_report([test_true])
    return


if __name__ == "__main__":
    app.run()
