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

    This guide show cases support for basic arithmetic and explains some of the general principles for calculations with the SSB Timeseries library.

    More specific guides are provided for topics like *calculations with time* and *metadata centric calculations*.
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
    Let us retrieve it for a single version identfied by the `as_of` date:
    """)
    return


@app.cell
def _(Dataset):
    jul = Dataset(name="Prices and Volumes", as_of_tz="2025-07-01")
    return (jul,)


@app.cell
def _(mo):
    mo.md(r"""
    ... and filter by metadata tags to separate prices from volumes, and calculate revenues by multiplying them:
    """)
    return


@app.cell
def _(jul):
    jul_prices = jul[{'variable': 'price'}]
    jul_volumes = jul[{'variable': 'volume'}]
    jul_revenue = jul_prices * jul_volumes
    return (jul_revenue,)


@app.cell(hide_code=True)
def _(jul_revenue):
    from copy import copy
    rev_name = copy(jul_revenue.name)
    return (rev_name,)


@app.cell
def _(jul_revenue):
    jul_revenue.plot()
    return


@app.cell(hide_code=True)
def _(mo, rev_name):
    mo.md(f"""
    The calculation returns a new dataset with a long and unwieldly name:

    `{rev_name}`

    The name and tags need to be updated to make sense:
    """)
    return


@app.cell
def _(jul_revenue):
    jul_revenue.rename("Revenues", ('price', 'revenue'))
    jul_revenue.replace_tags(({'variable':'price'}, {'variable': 'revenue'}))
    return


@app.cell
def _(mo):
    mo.md(f"""
    Not only the final calculation, but also the two slices created by the filter operations are new dataset instances.
    The same holds if we do not assign the intermediate variables:
    """)
    return


@app.cell
def _(Dataset):
    feb = Dataset(name="Prices and Volumes", as_of_tz="2025-02-01")
    feb_revenue = feb[{'variable': 'price'}] * feb[{'variable': 'volume'}]
    return feb, feb_revenue


@app.cell
def _(feb_revenue):
    feb_revenue.rename("Revenues", ('price', 'revenue'))
    feb_revenue.replace_tags(({'variable':'price'}, {'variable': 'revenue'}))
    return


@app.cell
def _(mo):
    mo.md(f"""
    This copying behaviour is by design:
    The library seeks to avoid in place updates.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""

    """)
    return


@app.cell
def _(feb_revenue, jul_revenue):
    change_in_revenue = jul_revenue - feb_revenue
    change_in_revenue.plot()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    The above examples showed simple arithemetic with `*` and `-`.
    These and other *infix* operators for element-wise arithmetic and comparisons work for `Dataaset` objects because the class exposes "dunder" methods to [emulate numeric types](https://docs.python.org/3/reference/datamodel.html#emulating-numeric-types) and [rich comparisons](https://docs.python.org/3/reference/datamodel.html#basic-customization).

    The implementation of all mathematical operators follows a pattern: a wrapper function that uses the [interoperability]() library [Narwhals]() to standardize input and pass on the actual work to Numpy.

    There are several points to unpack.
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
    Narwhals also brings conversion of `Dataset.data` to other libraries and their functionality within short reach.
    Shorthand properties `Dataset.pa`, `.nw`, `.pd`, and `.pl` will return Arrow tables, and Narwhals, Pandas and Polars dataframes.
    Each of these comes with their own set of features, but the main point is interoperability.

    Some meaningless calculation examples just to illustrate possible combinations of object types and operations:
    """)
    return


@app.cell
def _(feb, jul):
    ((jul - feb.pd) / feb.pl).name
    return


@app.cell
def _(feb, jul):
    ((jul - feb.pa)/ feb.nw).name
    return


@app.cell
def _(feb):
    try:
        feb.pd**2
    except  TypeError:
        print("`__pow__`  fails for date columns")
    return


@app.cell
def _(feb):
    type(feb.pd.set_index(['valid_from','valid_to'])**2)
    return


@app.cell
def _(feb):
    feb.pd.iloc[0,:]
    #.set_index(['valid_from', 'valid_to']) * 1.2
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""

    """)
    return


@app.cell
def _(feb):
    x = feb
    x_tbl = feb.pa
    x_pd = feb.pd
    x_pl = feb.pl
    do_stuff = x**2 / x.pa + x_pd + x_pl - x**2 - 100 + x.data
    return do_stuff, x


@app.cell(hide_code=True)
def _(mo):
    mo.md(f"""

    """)
    return


@app.cell
def _(do_stuff):
    do_stuff.name
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    This means that for any functionality that is missing in SSB Timeseries, it is easy to fill in the blanks.
    For example, at the time of writing, interval support and filtering by dates is an underdeveloped area of functionality.
    """)
    return


@app.cell
def _(date):
    import polars as pl

    d_from = date(2024, 2, 22)
    d_to = pl.date(2024, 3, 2)
    return d_from, d_to, pl


@app.cell
def _(d_from, d_to, pl, x):
    x_row = x.pl.filter( pl.col("valid_to").is_between(d_from, d_to) )
    return (x_row,)


@app.cell
def _(x_row):
    x_row
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    The difference between broadcasted and element-wise:
    """)
    return


@app.cell
def _(x):
    elementwise = (x * x)
    print(elementwise.name)
    print(type(elementwise))
    print(elementwise.data.shape)
    return (elementwise,)


@app.cell
def _(x, x_row):
    broadcast = (x * x_row)
    print(broadcast.name)
    print(type(broadcast))
    print(broadcast.data.shape)
    return (broadcast,)


@app.cell
def _(broadcast, elementwise):
    (broadcast == elementwise).data
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    (For the second row, matching `x_row` all values of the comparison are `True`.)
    """)
    return


@app.cell(disabled=True, hide_code=True)
def _(x, x_row):
    matrix = (x @ x_row)
    print(matrix.name)
    print(type(matrix))
    print(matrix.data.shape)
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
    newly_created  = set(locals())-variables_in_memory - {'variables_in_mamory'}
    newly_created
    return (newly_created,)


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


@app.cell
def _(mo):
    mo.md(r"""
    The vector variables may be used for calculations directly, using Narwhals functionality:
    """)
    return


@app.cell
def _(price_eggs, volume_eggs):
    (price_eggs * volume_eggs).mean()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(rf"""
    ``` {Warning}
    Caveats:
    Note that `.vectors()` is an experimental feature and the Narwhals library is not aimed at end users.
    The behaviour of the `vectors()` and in particular Narwhals series as returntype, is up for consideration and may be changed later.
    ```
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Or, convert with `.to_list()` or `.to_numpy()`.
    """)
    return


@app.cell
def _(price_eggs):
    price_eggs.to_numpy()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(f"""
    See also [Calculating with time](calc-with-time) or [Calculating with metadata](calc-with-meta-tags).
    """)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
