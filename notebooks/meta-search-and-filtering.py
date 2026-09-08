import marimo

__generated_with = "0.24.0"
app = marimo.App()


@app.cell(hide_code=True)
def _():
    import marimo as mo

    from filetree import tree
    from ssb_timeseries import get_configuration

    return (mo,)


@app.cell(hide_code=True)
def _():
    from ssb_timeseries.dataset import Dataset
    from ssb_timeseries.sample_data import create_df
    from ssb_timeseries.types import SeriesType

    return Dataset, SeriesType, create_df


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Metadata search and filtering
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Scope
    -----

    This guide demonstrates use of meta for finding data and for filtering.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Prerequisites
    -------------

    ``` {note}
    The guide assumes that the SSB Timeseries library is installed and that a working configuration is active.
    See [the quickstart guide](quickstart) for instructions to that.
    ```
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    The timeseries "catalog"
    ------------------------
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(rf"""
    The catalog is the basis for search.
    A catalog can consist of one or more repositories.
    In the typical case, it is just accessed through the top level function `get_catalog` which will provide a catalog spanning all repositories in the active configuration.
    The catalog aggegates all the tags for `Dataset` and `Series` to make them searchable in a single structure.
    """)
    return


@app.cell
def _():
    from ssb_timeseries import get_catalog

    return (get_catalog,)


@app.cell
def _(get_catalog):
    timeseries_catalog = get_catalog()
    return (timeseries_catalog,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    To list the datasets in the catalog:
    """)
    return


@app.cell
def _(timeseries_catalog):
    all_sets = timeseries_catalog.datasets()
    return (all_sets,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Or the unique repositories:
    """)
    return


@app.cell
def _(all_sets):
    repositories = {s.repository_name for s in all_sets}
    repositories
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    The catalog has a `.repositories` property.
    It returns a list of the actual repository objects.
    Repositories and catalogs share the most important methods: `.datasets()`, `.series()` and `.items()` for both.
    They all work the same way, taking the same parameters. The difference between the catalog and repository variants is simply that the catalog distributes the method calls to all its repositories.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Call with no parameters to get all items:
    """)
    return


@app.cell
def _(timeseries_catalog):
    timeseries_catalog.items()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Or, to search for all `Datasets` that contain `Series` tagged with `{'product': 'coffee'}`, just return the `parent` for the `.series` search:
    """)
    return


@app.cell
def _(timeseries_catalog):
    sets_that_have_series_tagged_with = {result.parent for result in timeseries_catalog.series(tags={'product': 'coffee'})}
    return


@app.cell
def _(all_sets):
    [s.object_name for s in all_sets]
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    To get a global tag dictionary:
    """)
    return


@app.cell
def _(all_sets):
    all_sets_tag_dict = {s.object_name: s.object_tags for s in all_sets}
    all_sets_tag_dict['Sample Data']
    return (all_sets_tag_dict,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    For each item, the `object_tags` in the global dictionary is the same as `Dataset.tags`:
    """)
    return


@app.cell
def _(Dataset, all_sets_tag_dict):
    sample_data = Dataset('Sample Data')
    sample_data.tags == all_sets_tag_dict['Sample Data']
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Searches that rely only on names and tags can be performed without accessing data:
    """)
    return


@app.cell
def _(timeseries_catalog):
    series = timeseries_catalog.series(tags={'dataset': ['Sample Data', 'XYZ']})
    [f"{s.repository_name}/{s.parent}/{s.object_name}" for s in series]
    return


@app.cell
def _(our_timeseries_database):
    import pandas as pd
    everything = our_timeseries_database.items()
    pd.DataFrame(everything)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Filtering
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Initialising a variable for an existing `Dataset`, we retrieve the previously stored metadata.
    """)
    return


@app.cell
def _(Dataset):
    x = Dataset('Sample Data')
    return (x,)


@app.cell
def _(x):
    x.tags
    return


@app.cell
def _(mo):
    mo.md(r"""
    Names can be used to filter the data.
    """)
    return


@app.cell
def _(x):
    x['q','p'].plot()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    And tags as well:
    """)
    return


app._unparsable_cell(
    r"""
    x.tag_series('x','product','
    """,
    name="_"
)


@app.cell
def _():
    return


@app.cell
def _(x):
    x[{'product': 'crispbread'}].plot()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Selection by tags becomes very powerful for bigger datasets.
    """)
    return


@app.cell(hide_code=True)
def _():
    from ssb_timeseries.datasert import Dataset
    from ssb_timeseries.sample_data import create_df

    return Dataset, create_df


@app.cell
def _(create_df):
    def mock_interval_data_from_file_or_query(start, end):
        a_to_z = [chr(i) for i in range(ord('a'), ord('z') + 1)]
        variables = ['volume', 'price']
        products = ['coffee', 'tea', 'soft-drinks', 'beer', 'wine']
        regions = ['N', 'E', 'W', 'S', 'NE', 'NW', 'SE', 'SW']
        return create_df(
            a_to_z, variables, products, regions,
            start_date=start,
            end_date=end,
            freq='M',
            temporality='FROM_TO',
            #implementation='polars'
        )

    return (mock_interval_data_from_file_or_query,)


@app.cell
def _(mock_interval_data_from_file_or_query):
    bigger_data = mock_interval_data_from_file_or_query(start='2025-01-01', end='2025-06-01')
    return (bigger_data,)


@app.cell
def _(Dataset, SeriesType, bigger_data):
    az = Dataset(
        name = 'AZ_drinks',
        data_type = SeriesType('NONE', 'FROM_TO'),
        data = bigger_data,
        attributes=['store','variable','product', 'region'],
    )
    az.save()
    return (az,)


@app.cell(hide_code=True)
def _(az, mo):
    mo.md(f"""
    The `az` set has {len(az.series)} series.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Eksempel: momentane data, *med* versjonering
    """)
    return


@app.cell
def _(SeriesType):
    estimated_point_in_time = SeriesType('AS_OF', 'AT')
    return (estimated_point_in_time,)


@app.cell
def _(create_df, ensure_datetime, timedelta):
    def data_for_n_days_prior(as_of, n):
        start = ensure_datetime(as_of) - timedelta(days=n)
        end = ensure_datetime(as_of) - timedelta(days=1)
        return create_df(['x','y','z'], start_date=start,end_date=end, freq='D')

    return (data_for_n_days_prior,)


@app.cell
def _(data_for_n_days_prior):
    n = 7
    data_for_n_days_prior('2024-03-15', n)
    return (n,)


@app.cell
def _():
    as_of_dates = ['2025-05-01','2025-06-01','2025-08-03','2025-08-04','2025-08-05','2025-08-06','2025-08-07']
    return (as_of_dates,)


@app.cell
def _(
    Dataset,
    as_of_dates,
    data_for_n_days_prior,
    date_utc,
    estimated_point_in_time,
    n,
):
    # update the data for several as of dates
    # --> simulates running the production process for several periods
    for as_of in as_of_dates:
        xyz_df = data_for_n_days_prior(as_of,n)
        Dataset(
            name = 'XYZ',
            data_type = estimated_point_in_time,
            as_of_tz=date_utc(as_of),
            data = xyz_df,
        ).save()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ...
    """)
    return


if __name__ == "__main__":
    app.run()
