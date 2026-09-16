import marimo

__generated_with = "0.24.0"
app = marimo.App()


@app.cell(hide_code=True)
def _():
    import marimo as mo

    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Tag maintentance
    ================
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Scope
    -----

    As shown in the basics, a dataset gets a few mandatory technical attributes on creation.

    Additional descriptive metadata may be provided, that is the dataset and its series may be tagged.
    The tagging is a one time operation that neeed should not need to be repeated.
    That is, unless mistakes or omissions have been made, or new series are added to the set.

    For calculations that derive new data.
    Some functions will automaticly update the metadata.
    Others will require that to be handled by the user.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Setup
    """)
    return


@app.cell
def _():
    from datetime import timedelta

    from ssb_timeseries.sample_data import create_df,date_ranges
    from ssb_timeseries.dates import ensure_datetime, date_utc

    return (create_df,)


@app.cell
def _():
    import polars as pl
    from datetime import datetime

    return


@app.cell
def _():
    from ssb_timeseries.types import SeriesType, Versioning, Temporality

    return (SeriesType,)


@app.cell
def _():
    from ssb_timeseries.dataset import Dataset

    return (Dataset,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Manually tagging set and series
    -------------------------------
    """)
    return


@app.cell
def _():
    return


@app.cell
def _(create_df):
    def some_simple_data_from_file_or_query(
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

    pqr_df = some_simple_data_from_file_or_query()
    return (pqr_df,)


@app.cell
def _(Dataset, SeriesType, pqr_df):
    pqr = Dataset(
        name = 'PQR',
        data_type = SeriesType('NONE', 'AT'),
        data = pqr_df,
    )
    return (pqr,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    The technical metadata is added at creation time.
    """)
    return


@app.cell
def _(pqr):
    pqr.tags
    return


@app.cell
def _(pqr):
    pqr.tag_dataset(tags={'variabel': 'pris','varegruppe': 'nødvendigheter'})

    pqr.tag_series('p',tags={'vare': 'kaffe'})
    pqr.tag_series('q',tags={'vare': 'knekkebrød'})
    pqr.tag_series('r',tags={'vare': 'brunost'})

    pqr.tags
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Tags can be used immediately.
    """)
    return


@app.cell
def _(pqr):
    pqr[{'vare': 'kaffe'}].data
    return


@app.cell
def _(pqr):
    pqr.save()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Autotagging
    -----------
    """)
    return


@app.cell
def _(SeriesType):
    interval_data = SeriesType('NONE', 'FROM_TO')
    return (interval_data,)


@app.cell
def _(create_df):
    def mock_interval_data_from_file_or_query(start, end):
        a_to_z = [chr(i) for i in range(ord('a'), ord('z') + 1)]
        variables = ['volume', 'price']
        goods = ['coffe', 'tea', 'softdrinks', 'beer', 'wine']
        return create_df(
            a_to_z, variables, goods,
            start_date=start,
            end_date=end,
            freq='M',
            temporality='FROM_TO',
            implementation='polars'
        )

    return (mock_interval_data_from_file_or_query,)


@app.cell
def _(mock_interval_data_from_file_or_query):
    bigger_data = mock_interval_data_from_file_or_query(start='2025-01-01', end='2025-06-01')
    return (bigger_data,)


@app.cell
def _(bigger_data):
    bigger_data
    return


@app.cell
def _(Dataset, bigger_data, interval_data):
    az = Dataset(
        name = 'AZ Drinks',
        data_type = interval_data,
        data = bigger_data,
        attributes=['store','variable','product'], # <-- this is the clever part
    )
    az.save()
    return (az,)


@app.cell
def _(az):
    len(az.series)
    return


@app.cell
def _(az):
    az_selection = az[{'product': 'tea', 'variable': 'price'}]
    az_selection.data
    return (az_selection,)


@app.cell
def _(az_selection):
    len(az_selection.series)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    By supplying the `attributes` parameter, we utilised the fact that names were structured as underscore separated strings.
    This way, we managed to tag 26 * 2 * 5 attributes across 260 series.

    The autotagging is quite powerful.
    Additional parameters may be supplied to specify other separators, substitutions, or more complex patterns with regexes.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Updating tags after calculations
    --------------------------------
    """)
    return


@app.cell
def _(Dataset):
    prices = Dataset('AZ Drinks')[{'variable':'price'}]
    volumes = Dataset('AZ Drinks')[{'variable':'volume'}]
    revenues = prices * volumes
    return (revenues,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    The new `Dataset` instance `revenues` gets an autogenerated name.
    The series names are also inherited from the inputs to the calculation.
    """)
    return


@app.cell
def _(revenues):
    print(revenues.name)
    print(revenues.series)
    return


@app.cell
def _():
    return


@app.cell
def _(revenues):
    revenues.rename('AZ Revenue', ('price', 'revenue'))
    print(revenues.name)
    print(revenues.series)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    A similar operation is required for tags:
    """)
    return


@app.cell
def _(revenues):
    # DEBUG: tags are lost in selects above, hence not flowing through
    revenues.tags["series"]["a_revenue_beer"]
    return


@app.cell
def _(revenues):
    # ... tag maintenance is likely to be necessary after calculations:
    revenues.replace_tags(({'variable':'price'},{'variable':'revenue'}))
    revenues.tags["series"]["a_revenue_beer"]
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Detagging
    ---------
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    If mistakes have been made, it may be necessary to remove tags.
    """)
    return


@app.cell
def _(pqr):
    from copy import deepcopy
    deepcopy(pqr.tags)
    return


@app.cell
def _(pqr):
    pqr.detag_series('varegruppe', vare='knekkebrød')
    return


@app.cell
def _(pqr):
    pqr.detag_series( vare='knekkebrød' )
    pqr.tags
    return


@app.cell
def _():
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    The catalog
    ------------------------

    Allows inspection and analysis of tags across all sets and series.
    Actual maintenance via `Dataset` methods.
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


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    The `Dataset` and `Series` attributes are *technically* just key-value pairs.
    It is, however, possible (even recommended) to rely on more formal taxonomies top structure these.
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
def _(Taxonomy):
    print('"Energy balance posts" - KLASS 157 is an example of a taxonomy with hierarchical structure.')
    klass157 = Taxonomy(klass_id=157)
    klass157.entities # <-- arrow table, with an extra row 0
    return (klass157,)


@app.cell
def _(klass157):
    # ... is inserted by the Taxonomy() bto create a tree structure with a single root node
    klass157.print_tree()
    return


@app.cell
def _(klass157):
    print(klass157.leaf_nodes)
    return


@app.cell
def _(klass157):
    print(klass157.parent_nodes)
    return


@app.cell
def _():
    # read/write to file -> taxonomies can be defined outside KLASS
    #klass157.save('klass157.json')
    #file157 = Taxonomy(path='klass157.json')
    return


@app.cell
def _():
    #klass157 == file157
    return


if __name__ == "__main__":
    app.run()
