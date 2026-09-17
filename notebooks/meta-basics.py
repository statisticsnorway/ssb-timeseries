import marimo

__generated_with = "0.24.0"
app = marimo.App()


@app.cell(hide_code=True)
def _():
    import marimo as mo
    import testing

    from filetree import tree
    from ssb_timeseries import get_configuration
    from profiling import profile_call

    CONFIG = get_configuration()

    def repository_tree():
        repositories = CONFIG.repositories
        root_dir = repositories['tutorials']['directory']['options']['path']
        print(tree(root_dir))

    return mo, repository_tree, testing


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

    This guide explains how metadata works in SSB Timeseries.
    It covers key concepts like:

    - [Repositories, Datasets and Series](#)
    - the type system
    - tag inheritance from `Dataset` to `Series` objects

    It also touches ever so lightly some topics that deserve being covered in more depth:

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


@app.cell
def _():
    from ssb_timeseries.config import Config

    Config.active().is_valid
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Repositories, Datasets and Series
    ---------------------------------

    Repositories, Datasets and Series are the building blocks of a hierarchy.
    `Repositories` are unique within the universe held within a [configuration](..configuring-io).
    Repositories contain `Datasets`.
    Datasets must be uniquely identified within their repository.
    Similarly, `Series` must be uniquely identified within the Datasets they are part of.

    Their *names* are unique identifiers within the scope of their parent.
    That means that it is possible to have:

    ```
    Repository A
        Dataset PQR
          Series P
          Series Q
          Series R
        Dataset XYZ-1
            Series X
            Series Y
            Series Z
        Dataset XYZ-2
            Series X
            Series Y
            Series Z
    Repository B
        Dataset PQR
            Series P
            Series Q
            Series R
    ```

    This scoping provides flexibility.
    It allows the same logic for different datasets.
    Creating a new dataset with *almost* identical content makes sense and allows easy transitions and comparisons in cases of changing methodologies or classifications.
    It also creates a potential for confusion.

    `Datasets` and `Series` are also associated with both technical and purely descriptive metadata via `tags`.
    While the "long name" `Repository/Dataset/Series` carries the identity of an individual series, its `tags` defines its meaning.
    If two complete sets of descriptions (tags) are identical, that implies identity.
    If there is a "real" difference (as opposed to merely a copy existing) it should show up in the metadata.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    The type system
    ---------------
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
def _(mo):
    mo.md(r"""
    Creating a Dataset
    ------------------
    """)
    return


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
    If not specified, the configuration will determine which one is used, if there is more than one.
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


    The technical attributes are both object properties and reflected in `Dataset.tags`. This minimal amount of mandatory metadata is applied creation time and can not be changed without running the risk of breaking functionality.
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

    sample_set.tag_series('x',tags={'product': 'coffee'})
    sample_set.tag_series('y',tags={'product': 'crispbread'})
    sample_set.tag_series('z',tags={'product': 'brown cheese'})

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
    xyz = Dataset('Sample Data')
    return (xyz,)


@app.cell
def _(xyz):
    xyz.tags
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Selecting series

    Series can be selected from the dataset by name, regex patterns or tags.
    """)
    return


@app.cell
def _(xyz):
    xyz['x','y'].plot()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    And tags as well:
    """)
    return


@app.cell
def _(xyz):
    xyz[{'area': 'z'}].plot()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    With the simple "XYZ" dataset this is not so exciting.
    However, selection by tags becomes very powerful for bigger datasets.
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
    #az.save()
    return (az,)


@app.cell(hide_code=True)
def _(az, mo):
    mo.md(f"""
    The `az` set has {len(az.series)} series.
    """)
    return


@app.cell(hide_code=True)
def _():
    #az.tags
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    At this scale, it is not longer practical to deal with individual series:
    """)
    return


@app.cell
def _(az):
    az.tags["series"]["a_price_coffee_NW"]
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    While one could do something like looping over name patterns, organising the data in subsets identified by tags is much more practical:
    """)
    return


@app.cell
def _(az):
    prices = az[{'variable':'price'}]
    volumes = az[{'variable':'volume'}]
    return prices, volumes


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Series in `prices`:
    """)
    return


@app.cell(hide_code=True)
def _(prices):
    print( *prices.series[1:3], '...', *prices.series[-3:], '\n\n', f"{len(prices.series)=}",)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    New objects and tag maintenance
    -------------------------------

    The selection returns new dataset instnances for which both the data and the metadata have been filtered to match the criteria.
    The retrieved data is sorted to allow calculations to be performed without complicated matching.
    """)
    return


@app.cell
def _(prices, volumes):
    revenue = prices * volumes
    return (revenue,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    (Explicit matching may still be required in some corner cases.)

    After a calculations, the original metadata will rarely be accurate anymore. Some functions update the metadata automatically, but in general tags need to be updated after calculations.
    """)
    return


@app.cell
def _(revenue):
    revenue.rename('AZ_drinks', ('prices', 'volumes'))
    revenue.replace_tags(({'variable':'price'},{'variable':'revenue'}))
    revenue.plot()
    return


@app.cell
def _(mo):
    mo.md(r"""
    See [tag maintenance](meta-tag-maintenance) or [calculations with metadata](calc-with-metadata) for more about either topic.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Formal taxonomies
    -----------------

    As seen in the code above, the SSB Timeseries library implements tags as key value pairs and handles them through Python dictionaries.
    This is a very lightweight approach that provides a lot of flexibility.
    Just about anything that fits into the key value structure goes.

    A more formal approach will put some governance and standardisation on which attributes to use, how to name them, and which values are allowed.

    Integrating with such formal structures - and metadata systems - through the `meta` module is in the shaping.
    The design philosophy is to keep the integration lightweight and configurable.
    At the core is the idea that `attributes` take their `values` defined in a `Taxonomy`.

    The code snippet below shows how a taxonomy may be consumed from Statistics Norway's taxonomy system KLASS.
    """)
    return


@app.cell
def _():
    from ssb_timeseries.meta import Taxonomy

    klass157 = Taxonomy(klass_id=157)
    klass157.print_tree()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    In this example the taxonomy has a hierarcical structure.
    Hierarchical (or even graph) structures may be used for [calculations](calc-with-metadata), as long as the tag values match a taxonomy.

    While features for [tag mainatenance](meta-tag-maintenance) allow fixing some mistakes after the fact,
    attribute structures are important considerations that should not be taken lightly.
    They are, after all, a subset of ["naming things"](https://martinfowler.com/bliki/TwoHardThings.html).
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Data catalog
    ------------

    The SSB Timeseries library can be configured to deal with the metadata in more than one way.
    The library configuration allows setting up metadata repositories independent of the data storage.
    That allows multiple data repositories to share a single metadata repository.
    At the most technical level, storage comes down to IO implementation, but the separate configurations allow the metadata to be stored more than once. It can be stored both near the actual data, say in header or footer fields of file based storage, and in a sentral repository accessed through an API.

    Regardless of setup, multiple metadata repositories in a configuration can be treated as a single catalog.
    Collecting structured metadata in one place makes it easier to search.
    """)
    return


@app.cell
def _():
    from ssb_timeseries import get_catalog

    our_timeseries_database = get_catalog()
    all_the_datasets = our_timeseries_database.datasets()
    return (all_the_datasets,)


@app.cell
def _(all_the_datasets):
    type(all_the_datasets)
    return


@app.cell
def _(all_the_datasets):
    type(all_the_datasets[0])
    return


@app.cell
def _(all_the_datasets):
    [catalog_item.object_name for catalog_item in all_the_datasets]
    return


@app.cell(disabled=True)
def _(all_the_datasets):
    import pandas as pd
    pd.DataFrame(all_the_datasets )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    The list above should correspond to what we find in our file based repository:
    """)
    return


@app.cell(hide_code=True)
def _(repository_tree):
    repository_tree()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    See the guide to [search and filtering](meta-search-and-filtering) for more details on the `Catalog`.
    """)
    return


@app.function
# @supress

def test_success():
    assert True


@app.cell
def _(testing):
    testing.run_and_report([test_success])
    return


if __name__ == "__main__":
    app.run()
