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
    # Calculations with metadata
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Scope
    -----

    This guide illustrates how the role of metadata in calculations extends beyond simple filtering.
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

    The presented functionality relies on `dataset.Dataset` and `meta.taxonomy.Taxonomy`.
    Other imports like`types.SeriesType` and external libraries are used only for generating the sample data.
    """)
    return


@app.cell
def _():
    from ssb_timeseries.dataset import Dataset
    from ssb_timeseries.meta.taxonomy import Taxonomy

    return Dataset, Taxonomy


@app.cell
def _():
    return


@app.cell
def _():
    from ssb_timeseries.types import SeriesType
    from ssb_timeseries.sample_data import create_df
    from itertools import product
    from datetime import date

    return SeriesType, create_df


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Generate some test data
    """)
    return


@app.cell
def _(Dataset, SeriesType, create_df):
    def create_some_example_data(
        set_name: str,
        series_tags: dict[str,list[str]],
    ):
        """Generate and save some sample data."""
        set_tags = { "Country": "Norway" }
        df = create_df(
            *[value for value in series_tags.values()],
            temporality= 'FROM_TO',
            start_date="2024-01-01",
            end_date="2026-12-01",
            freq="MS",
        )
        Dataset(
            name=set_name,
            data_type=SeriesType('NONE', 'FROM_TO'),
            data=df,
            tags = set_tags,
            attributes = series_tags.keys(),
        ).save()

    return (create_some_example_data,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    We will generate random data for all permutations of some descriptive metadata tags.
    This time we include a real classification that we will simply name "taxonomy", and use completely out of context.
    (It just happens to have a suitable shape and size.)
    """)
    return


@app.cell
def _(Taxonomy):
    taxonomy = Taxonomy(klass_id=157)
    taxonomy.print_tree()
    return (taxonomy,)


@app.cell
def _(create_some_example_data, taxonomy):
    create_some_example_data(
        set_name="More Prices and Volumes",
        series_tags = {
            "variable": ["price", "volume"],
            "product": ["milk", "eggs", "bread", "juice", "ham", "cheese"],
            "category": taxonomy.leaf_nodes,
        }
    )
    return


@app.cell(hide_code=True)
def _(mo, taxonomy):
    mo.md(f"""
    Here we use the {len(taxonomy.leaf_nodes)} `taxonomy.leaf_nodes` to populate a `category` attribute.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Filtering datasets by tags
    --------------------------
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    The most typical use of descriptive metadata, aka `Dataset.tags`, is to extract subsets of datasets for specific purposes.
    A simple "example with Prices and Volumes" extracts *prices* and *volumes* for a number of *products* into separate variables and calculate revenues by multiplying them:
    """)
    return


@app.cell
def _(Dataset):
    prices_and_volumes = Dataset(name="More Prices and Volumes")
    prices = prices_and_volumes[{'variable': 'price'}]
    volumes = prices_and_volumes[{'variable': 'volume'}]
    revenue = prices * volumes
    return (revenue,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(f"""
    The name and tags of the returned dataset need to be updated to make sense:
    """)
    return


@app.cell
def _(revenue):
    revenue.rename("More Revenues", ('price', 'revenue'))
    revenue.replace_tags(({'variable':'price'}, {'variable': 'revenue'}))
    return


@app.cell(hide_code=True)
def _(mo, revenue, taxonomy):
    mo.md(f"""
    So from {len(taxonomy.leaf_nodes)} taxonomy entities times 6 products we get {len(revenue.series)} revenue series.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Group by behaviour
    ------------------

    Group by can be configured to run in "auto" mode: using metadata attributes to select whether to calculate sums or averages.

    (The functionality was hard coded for the PoC phase. It is now disabled, but a functionality skeleton is still there. The missing link for working properly is configuration interaction.)
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
    ## Aggregates
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Since we now happen to have a properly tagged dataset containing all the leaf nodes in such a tree, we can calculate the aggregates for the rest of the taxonomy structure, that is for the "parent" nodes of the hierarchy:
    """)
    return


@app.cell
def _(taxonomy):
    taxonomy.parent_nodes
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(f"""

    """)
    return


@app.cell
def _(revenue, taxonomy):
    list_of_functions = ['sum'] # there are more options --> see the reference

    aggregated_revenue = revenue.aggregate(
        attributes=["category"],  # lengths must match ↓
        taxonomies=[taxonomy],    # lengths must match ↑
        functions=list_of_functions
    )
    aggregated_revenue.pl.schema.to_python()
    return (aggregated_revenue,)


@app.cell
def _(aggregated_revenue):
    aggregated_revenue.tags
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Here we can observe a bug: "input" lists data, not series names.
    Including input data could be OK elsewhere, but it inflates `.tags` in a way that is not scalable.
    (Tags are included in parquet metadata fields, so including data may limit the maximum size of datasets / cause other problems.)
    """)
    return


@app.cell
def _():
    return


@app.cell
def _(klass157, leaves_157):
    aggregates_157 = leaves_157.aggregate(
        attributes=["k157"],
        taxonomies=[klass157],
        functions=["sum","mean"]
    )
    aggregates_157.data
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Planned: Canonic datasets
    -----------------------

    Calculations with canonic datasets are sets where a few specific datasets play a key role.
    The datasets can reside in local repositories or accessed through API.

    Examples:
     - Currency conversion.
     - Inflation adjustment.

    For such features to work, canonic sets and critical attributes must be specified.
    Hard coded would work, but configurations would be better.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Planned: Unit conversion
    ----------------------

    Automatic unit conversions based on tags require configurations to identify the name of the unit attribute.
    """)
    return


if __name__ == "__main__":
    app.run()
