import marimo

__generated_with = "0.24.0"
app = marimo.App()


@app.cell(hide_code=True)
def _():
    import marimo as mo

    from filetree import tree
    from ssb_timeseries import get_configuration
    from profiling import profile_call

    CONFIG = get_configuration()

    def repository_tree():
        repositories = CONFIG.repositories
        root_dir = repositories['tutorials']['directory']['options']['path']
        print(tree(root_dir))

    return mo, profile_call, repository_tree


@app.cell(disabled=True, hide_code=True)
def _(repository_tree):
    repository_tree()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Metadata fundamentals
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Scope
    -----

    This guide explains how metadata works in SSB Timeseries:

    - key technical concepts:
      - repository -> dataset -> series
      - the type system
      - tags inheritance from `Dataset` to `Series` objects

    Some topics will be covered in more depth in dedicated guides:

    - [search and filtering](meta-search-and-filtering) with tags
    - [tag maintenance](meta-tag-maintenance)
    - consuming taxonomies
    - calculations with metadata
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
    Technical metadata - the type system
    ------------------------------------
    """)
    return


@app.cell(hide_code=True)
def _(SeriesType, Temporality, Versioning, mo):
    mo.md(f"""
    {SeriesType.__doc__}

    - {Versioning.__doc__}
    - {Temporality.__doc__}
    """)
    return


@app.cell
def _():
    from ssb_timeseries.dataset import Dataset
    from ssb_timeseries.types import SeriesType, Versioning, Temporality

    return Dataset, SeriesType, Temporality, Versioning


@app.cell(hide_code=True)
def _():
    from datetime import timedelta

    from ssb_timeseries.sample_data import create_df,date_ranges
    from ssb_timeseries.dates import ensure_datetime, date_utc

    import polars as pl
    from datetime import datetime

    return (create_df,)


@app.cell(hide_code=True)
def _(create_df):
    def dataframe_like_data_from_file_or_query(
        start='2020-01-01',
        end='2025-06-01',
    ):
        """create some sample data"""
        return create_df(
            ['p','q','r'],
            start_date=start,
            end_date=end,
            freq='D'
        )

    return (dataframe_like_data_from_file_or_query,)


@app.cell
def _(dataframe_like_data_from_file_or_query):
    some_data = dataframe_like_data_from_file_or_query()
    return (some_data,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    When creating a `Dataset` for the first time, a `name`, a `type` and some data are required.

    Specifying a `repository` is optional.
    If not specified, a default will be applied based on configurations.
    Names are assumed to be unique identifiers:
    The dataset name must be unique within the repository.
    Series names must be unique within the dataset.
    """)
    return


@app.cell
def _(Dataset, SeriesType, Temporality, Versioning, some_data):
    sample_set = Dataset(
        name = 'Sample Data',
        data_type = SeriesType(Versioning.NONE, Temporality.AT),
        data = some_data,
    )
    sample_set.save()
    return (sample_set,)


@app.cell
def _(sample_set):
    print(repr(sample_set))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(f"""
    These attributes are technically significant.
    If any of them are changed, it changes *where* or *how* the data is stored, and how it may be used.

    Dataset creation will apply a minimal amount of mandatory metadata as `Dataset.tags`.
    The technical attributes are both technical `Dataset` properties and reflected in its `.tags`:
    """)
    return


@app.cell
def _(sample_set):
    sample_set.tags
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(f"""
    Note how `Dataset.name` becomes `Series.dataset` in the tags, while the technical properties are inherited directly.
    The datatype dimensions are reflected in both in `.versioning` and the single `valid_at` column in `.data`:
    """)
    return


@app.cell
def _(sample_set):
    sample_set.data
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    To apply more than the minimal set of technical tags, we need to "tag" the dataset and series.
    """)
    return


@app.cell
def _(sample_set):
    sample_set.tag_dataset(tags={'variable': 'price','product group': 'essential'})

    sample_set.tag_series('p',tags={'product': 'coffee'})
    sample_set.tag_series('q',tags={'product': 'crispbread'})
    sample_set.tag_series('r',tags={'product': 'brown cheese'})

    sample_set.save()
    sample_set.tags
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


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Filtering

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


@app.cell
def _(az):
    az.tags
    return


@app.cell
def _(az):
    az.tags["series"]["a_price_coffee_NW"]
    return


@app.cell
def _(az, profile_call):
    prices = profile_call(func=az.select, tags={'variable': 'price'})
    return (prices,)


@app.cell
def _(az, profile_call):
    profile_call(az.select, regex='a_price_*_E')
    return


@app.cell
def _(az):
    volumes = az[{'variable':'volume'}]
    return (volumes,)


@app.cell
def _(prices, volumes):
    revenue = prices * volumes
    return (revenue,)


@app.cell
def _(revenue):
    revenue.rename('AZ_drinks', ('prices', 'volumes'))
    revenue.replace_tags(({'variable':'price'},{'variable':'revenue'}))
    revenue.plot()
    return


if __name__ == "__main__":
    app.run()
