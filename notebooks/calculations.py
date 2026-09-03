import marimo

__generated_with = "0.24.0"
app = marimo.App()


@app.cell
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


@app.cell
def _():
    from ssb_timeseries.dataset import Dataset
    from ssb_timeseries.types import SeriesType, Versioning, Temporality
    from ssb_timeseries.sample_data import create_df
    from ssb_timeseries.dates import date_utc, now_utc

    return Dataset, SeriesType, create_df


@app.cell(hide_code=True)
def _():
    from itertools import product
    from datetime import date

    return date, product


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Calculations
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    This guide introduces the basic calculation features in SSB Timeseries.
    """)
    return


@app.cell
def _(SeriesType, date, product):
    set_name = "Prices and Volumes"
    series_tags = {
        "variable": ["price", "volume"],
        "product": ["milk", "eggs", "bread", "juice", "ham", "cheese"],
    }
    set_tags = { "Country": "Norway" }
    PERIOD_ESTIMATE = SeriesType('AS_OF', 'FROM_TO')

    as_of_dates = [date(*d) for d in product({2024,2025}, range(1,13), {1})]
    return PERIOD_ESTIMATE, as_of_dates, series_tags, set_name, set_tags


@app.cell
def _():
    return


@app.cell
def _(
    Dataset,
    PERIOD_ESTIMATE,
    as_of_dates,
    create_df,
    series_tags,
    set_name,
    set_tags,
):
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
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Algebra
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
    The calculation returns a new dataset with a long and unwieldly name:

    `{rev_name}`.

    In fact, not only the final calculation, but also the two filter operations returned new dataset instances.
    As they were not assigned to any variables, they were just not kept.
    The copying rather behaviour is by design:
    The library seeks to avoid in place updates.

    The new set inherits some of its metadata from the inputs,
    and some attributes may be set by the functions that performs the calculations,
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
    Their implementation uses Numpy under the hood for the actual work and ["broadcasting rules"](https://numpy.org/doc/stable/user/basics.broadcasting.html), with a wrapper function for supported objects.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### ... more algebra
    """)
    return


@app.cell
def _(feb, jul):
    _x = jul * 10 - 123
    y = feb - 3
    z = _x * y + _x ** 2 - y.data + 2
    z
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
