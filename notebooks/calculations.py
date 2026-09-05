import marimo

__generated_with = "0.24.0"
app = marimo.App()


@app.cell(hide_code=True)
def _():
    import marimo as mo
    mo.Html(
        """
        <style>
        /* Hides the desktop sidebar table of contents */
        div[class*="marimo-toc"],
        aside[class*="sidebar"],
        [data-testid="marimo-toc"] {
            display: none !important;
        }

        /* Adjusts the main content margin to center it */
        main {
            margin-left: auto !important;
            margin-right: auto !important;
            max-width: 960px !important;
        }
        </style>
        """
    )
    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Calculations
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Scope
    -----

    This guide introduces the most basic calculation features in SSB Timeseries and explain some general principles for how calculations are supposed to work.

    See the specific guides for *calculations with time* and *metadata centric calculations*.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Prerequisites

    ``` {note}
    The SSB Timeseries library must be installed and a working configuration is active.
    See [the quickstart guide](quickstart) for instructions.
    ```

    The presented functionality relies on `dataset.Dataset`.
    Other imports like`types.SeriesType` and a few external ones are used only for generating the sample data.
    """)
    return


@app.cell
def _():
    from ssb_timeseries.dataset import Dataset
    #from ssb_timeseries.dates import date_utc, now_ut
    return (Dataset,)


@app.cell
def _():
    from ssb_timeseries.types import SeriesType
    from ssb_timeseries.sample_data import create_df
    from itertools import product
    from datetime import date

    return SeriesType, create_df, date, product


@app.cell
def _(Dataset, SeriesType, create_df, date):
    def create_some_example_data(
        set_name: str,
        as_of_dates: list[date],
        series_tags: dict[str,list[str]],
    ):
        """Generate and save some sample data."""
        set_tags = { "Country": "Norway" }
        PERIOD_ESTIMATE = SeriesType('AS_OF', 'FROM_TO')
        for d in as_of_dates:
            df = create_df(
                *[value for value in series_tags.values()],
                temporality= 'FROM_TO',
                start_date="2024-01-01",
                end_date="2026-12-01",
                freq="MS",
            )
            Dataset(
                name=set_name,
                data_type=PERIOD_ESTIMATE,
                as_of_tz=str(d),
                data=df,
                tags = set_tags,
                attributes = ["variable", "product"],
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
        set_name="Prices and Volumes",
        as_of_dates = [date(*d) for d in product({2024,2025}, range(1,13), {1})],
        series_tags = {
            "variable": ["price", "volume"],
            "product": ["milk", "eggs", "bread", "juice", "ham", "cheese"],
        }
    )
    return


@app.cell
def _():
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
    Let us retrieve it for a single version identfied by the `as_of` date, filter by metadata tags and calculate revenues:
    """)
    return


@app.cell
def _(Dataset):
    jul = Dataset(name="Prices and Volumes", as_of_tz="2023-07-01")
    jul_revenue = jul[{'variable': 'price'}] * jul[{'variable': 'volume'}]
    return jul, jul_revenue


@app.cell(hide_code=True)
def _(jul_revenue):
    from copy import copy
    rev_name = copy(jul_revenue.name)
    return (rev_name,)


@app.cell(hide_code=True)
def _(mo, rev_name):
    mo.md(f"""
    The calculation over the two slices from `jul` returns a new dataset with a long and unwieldly name:

    `{rev_name}`.

    In fact, not only the final calculation, but also the two slices created by the filter operations returned new dataset instances, albeit only in memory.
    As they were not assigned to any variables, they were just not kept.

    The copying behaviour is by design:
    The library seeks to avoid in place updates.

    The new sets created by calculations will inherit some of their attributes (or "tags") from the inputs.
    Others may be set by the functions that perform the calculations,
    but generally descriptive metadata for calculation outputs need to be updated manually:
    """)
    return


@app.cell
def _(jul_revenue):
    jul_revenue.rename("Revenues", ('price', 'revenue'))
    jul_revenue.replace_tags(({'variable':'price'}, {'variable': 'revenue'}))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Let us do the same for february.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""

    """)
    return


@app.cell
def _(Dataset):
    feb = Dataset(name="Prices and Volumes", as_of_tz="2023-02-01")
    feb_revenue = feb[{'variable': 'price'}] * feb[{'variable': 'volume'}]
    feb_revenue.rename("Revenues", ('price', 'revenue'))
    feb_revenue.replace_tags(({'variable':'price'}, {'variable': 'revenue'}))
    return feb, feb_revenue


@app.cell
def _(feb_revenue, jul_revenue):
    change_in_revenue = jul_revenue - feb_revenue
    change_in_revenue.plot()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    The above examples showed simple algebra with `*` and `-`.
    These and other *infix* operators for element-wise arithmetic and comparisons work for `Dataaset` objects because the class exposes "dunder" methods to [emulate numeric types](https://docs.python.org/3/reference/datamodel.html#emulating-numeric-types) and [rich comparisons](https://docs.python.org/3/reference/datamodel.html#basic-customization).

    The mathematical operator implementation follows a pattern: a wrapper function that uses the [interoperability]() library [Narwhals]() to standardize input and pass on the actual work to Numpy.

    This means that the arithmetic functions support operating not only on `dataset` objects, but on combinations of `Dataset` with a large number of other datatypes (scalars, Numpy arrays, dataframes, Arrow tables).
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    Numpy for the implementation means that [Numpy "broadcasting rules"](https://numpy.org/doc/stable/user/basics.broadcasting.html) apply for operations on different size objects.
    Broadcasting rules and dimensional conditions are avaluated only for the numeric parts.
    The arithmetic ignores the date columns to allow calculations with mixed temporality.
    Date alignment must be performed explicitly.

    Narwhals also brings conversion to other libraries and their functionality within short reach. Note the shorthand properties `{type(x.pd)=}` and
    and `{type(x.pl)=}`
    Note that Arrow tables and Narwhals compatible dataframes are all conflated to 'df' in the lineage tracking.
    """)
    return


@app.cell
def _(feb, jul):
    jul - feb.pd
    return


@app.cell
def _(feb, jul):
    x = jul * 10 - 123
    y = feb - 3
    x_pd = (x.pd).set_index(['valid_from', 'valid_to']) * 1.2
    return x, x_pd, y


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""

    """)
    return


@app.cell
def _(x, x_pd, y):
    x_pl = x - (x.pl *0.5) # polars
    z = x - x_pd + x_pl * y + x ** 2 - y.data + 2
    return (z,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(f"""

    """)
    return


@app.cell
def _(z):
    z.name
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    At the time of writing, interval support and filtering by dates is an underdeveloped area of functionality at the time of writing.
    """)
    return


@app.cell
def _(z):
    import polars as pl
    one_period_from_z = z.pl.filter(
        pl.col("valid_to").is_between(pl.date(2025, 5, 29), pl.date(2025, 6, 2))
    )
    one_period_from_z
    return (one_period_from_z,)


@app.cell
def _():

    return


@app.cell
def _(one_period_from_z, y):
    one_period_from_z @ y
    return


@app.cell
def _(create_df):
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
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Vectors
    """)
    return


@app.cell(hide_code=True)
def _(jul_revenue, mo):
    mo.md(f"""
    We can also get a vector (or more precisely, a Narwhals series) per series in the set. For the `jul_revenue` set from above:

    `{jul_revenue.series=}`

    Let us first record what we already have in memory:
    """)
    return


@app.cell
def _():
    variables_in_memory = set(locals())
    return (variables_in_memory,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Then do the incantation (with the right intonation, and swing the magic wand):
    """)
    return


@app.cell
def _(jul_revenue):
    jul_revenue.vectors()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(f"""
    The impact of this may not be immediately visible, but this method call will have assigned a variable for each of the series names.

    ... so if we check for new variables:
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""

    """)
    return


@app.cell
def _(variables_in_memory):
    newly_created = set(locals())-variables_in_memory
    newly_created
    return (newly_created,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Vectors accepts filter parameters. The following will behave the same as  `jul['*eggs*'].vectors()`, but will not create an intermediate dataset object.
    """)
    return


@app.cell
def _(jul):
    jul.vectors('eggs')
    return


@app.cell
def _(newly_created, variables_in_memory):
    set(locals()) - variables_in_memory - newly_created
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    The vector form of the variables may be used for calculations,
    even if the Narwhals library is intended mainly for code library development.
    (Note that `.vectors()` is an experimental feature. Its behaviour, including the return type, is up for consideration and may be changed later.)
    """)
    return


@app.cell
def _(price_eggs, volume_eggs):
    (price_eggs * volume_eggs).mean()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(f"""
    ``` {Warning}
    Be careful!
    `.vectors()` blindly assigns to variables outside its own scope.
    That can have nasty side effects if column names happen to match to variables or objects that already exist.
    ```
    """)
    return


if __name__ == "__main__":
    app.run()
