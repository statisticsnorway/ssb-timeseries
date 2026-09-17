import marimo

__generated_with = "0.24.0"
app = marimo.App()


@app.cell(hide_code=True)
def _():
    import marimo as mo

    from filetree import tree
    from ssb_timeseries import get_configuration

    CONFIG = get_configuration()

    def repository_tree():
        repositories = CONFIG.repositories
        root_dir = repositories['tutorials']['directory']['options']['path']
        print(tree(root_dir))

    return mo, repository_tree, tree


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

    return create_df, date_utc, ensure_datetime, timedelta


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
def _(az):
    prices = az[{'variable': 'price'}]
    return (prices,)


@app.cell
def _(Dataset):
    volumes = Dataset('AZ_drinks')[{'variable':'volume'}]
    return (volumes,)


@app.cell
def _(prices, volumes):
    revenue = prices * volumes
    return (revenue,)


@app.cell
def _(revenue):
    print(revenue.name)
    type(revenue)
    return


@app.cell
def _(revenue):
    revenue.nw.to_pandas()
    return


@app.cell
def _(revenue):
    revenue.rename('AZ_drinks', ('prices', 'volumes'))
    revenue.plot()
    return


@app.cell
def _(omsetning):
    omsetning.nw.to_pandas()
    return


@app.cell
def _(omsetning):
    # DEBUG: tags are lost in selects above, hence not flowing through
    omsetning.tags["series"]["a_omsetning_brus"]
    return


@app.cell
def _(omsetning):
    # ... tag maintenance is likely to be necessary after calculations:
    omsetning.replace_tags(({'variabel':'pris'},{'variabel':'omsetning'}))
    omsetning.tags["series"]["a_omsetning_brus"]
    return


@app.cell
def _(omsetning, repository, tree):
    omsetning.save()
    tree(repository)
    return


@app.cell
def _(antall, display, omsetning, priser):
    # review the data
    display(priser.data)
    display(antall.data)
    display(omsetning.nw.to_pandas())
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
def _(repository, tree):
    tree(repository)
    return


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


@app.cell
def _(repository, tree):
    tree(repository)
    return


@app.cell
def _(Dataset, as_of_dates):
    first = Dataset('XYZ', as_of_tz=as_of_dates[0])
    last = Dataset('XYZ', as_of_tz=as_of_dates[-1])
    return first, last


@app.cell
def _(Dataset):
    specific = Dataset('XYZ', as_of_tz='2025-08-04')
    specific
    return


@app.cell
def _(last):
    last
    return


@app.cell
def _(first):
    first.nw.to_pandas()
    return


@app.cell
def _(last):
    last.nw.to_pandas()
    return


@app.cell
def _(first, last):
    diff = last - first
    diff
    return (diff,)


@app.cell
def _(diff):
    diff.nw.to_pandas()
    return


@app.cell
def _(diff):
    diff.plot()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ...
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    The timeseries "catalog"
    ------------------------
    """)
    return


@app.cell
def _():
    from ssb_timeseries import get_catalog

    return (get_catalog,)


@app.cell
def _(get_catalog):
    our_timeseries_database = get_catalog()
    return (our_timeseries_database,)


@app.cell
def _(our_timeseries_database):
    all_sets = our_timeseries_database.datasets()
    [s.object_name for s in all_sets]
    return


@app.cell
def _(our_timeseries_database):
    series_in_xyz = our_timeseries_database.series(tags={'dataset': 'XYZ'})
    [s.object_name for s in series_in_xyz]
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
    ### Consuming KLASS
    """)
    return


@app.cell
def _():
    from klass import get_classification
    from klass import KlassClassification # Import the class for KlassClassifications

    return (get_classification,)


@app.cell
def _(get_classification):
    print(get_classification(157))
    return


@app.cell
def _():
    from ssb_timeseries.meta import Taxonomy

    return (Taxonomy,)


@app.cell
def _():
    import matplotlib.pyplot as plt
    import networkx as nx
    from networkx.drawing.nx_agraph import graphviz_layout

    return graphviz_layout, nx, plt


@app.cell
def _(Taxonomy):
    klass157 = Taxonomy(klass_id=157)
    klass157.entities # <-- arrow table, with an extra row 0
    #klass157.structure
    return (klass157,)


@app.cell
def _(graphviz_layout, klass157, nx, plt):
    # ... is inserted by the Taxonomy() bto create a tree structure with a single root node
    digraph = klass157.structure
    pos = graphviz_layout(digraph, prog="dot")

    nx.draw(digraph, pos)
    plt.show()
    return


@app.cell
def _(klass157):
    print('\nAll leaf nodes')
    print([n.name for n in klass157.structure.root.leaves])
    print('')
    return


@app.cell
def _(Taxonomy, klass157):
    # read/write to file -> taxonomies can be defined outside KLASS
    klass157.save('klass157.json')
    file157 = Taxonomy(path='klass157.json')
    file157.structure.show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Using metadata for calculations
    """)
    return


@app.cell
def _(klass157):
    klass157_leaves = [n.name for n in klass157.structure.root.leaves]
    return (klass157_leaves,)


@app.cell
def _(Dataset, SeriesType, create_df, klass157_leaves):
    #series_tags = {"klass157": klass157_leaves, "B": ["q"], "C": ["z"]}
    series_tags = {"klass157": klass157_leaves, 'variabel': 'verdi'}
    tag_based_names: list[list[str]] = [value for value in series_tags.values()]
    df = create_df( *tag_based_names, start_date="2022-01-01", end_date="2022-04-03", freq="D" )

    leaves_157 = Dataset(
        name="set_using_klass_157",
        data_type=SeriesType.estimate(),
        as_of_tz="2022-01-01",
        data=df,
        attributes=["k157","var"],
    )
    return (leaves_157,)


@app.cell
def _(leaves_157):
    leaves_157.data.head()
    return


@app.cell
def _(leaves_157):
    leaves_157.tags['series']['8.1_verdi']
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
    Integration with KLASS works as demonstrated.

    However many "taxonomies" will be defined in other places.
    Notably VardDef, but also Statistical entitites: Organisations, ...,
    """)
    return


if __name__ == "__main__":
    app.run()
