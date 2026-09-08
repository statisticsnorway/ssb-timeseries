import marimo

__generated_with = "0.24.0"
app = marimo.App()


@app.cell(hide_code=True)
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _():
    return


@app.cell
def _():
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Meta data
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

    return create_df, date_utc, ensure_datetime, timedelta


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


@app.cell
def _(pqr):
    pqr.save()
    return


@app.cell
def _(Dataset):
    # reading the data back:
    x = Dataset('PQR')

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
        variables = ['antall', 'pris']
        goods = ['kaffe', 'te', 'brus', 'øl', 'vin']
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
        name = 'AZ_drikkevarer',
        data_type = interval_data,
        data = bigger_data,
        attributes=['butikk','variabel','vare'], # <-- this is the clever part
    )
    return (az,)


@app.cell
def _(az):
    az.save()
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
    priser = Dataset('AZ_drikkevarer')[{'variabel':'pris'}]
    antall = Dataset('AZ_drikkevarer')[{'variabel':'antall'}]
    omsetning = (priser * antall)
    print(omsetning.name)
    type(omsetning)
    return antall, omsetning, priser


@app.cell
def _(omsetning):
    omsetning.nw.to_pandas()
    return


@app.cell
def _(omsetning):
    omsetning.rename('AZ_omsetning', ('pris', 'omsetning'))
    print(omsetning)
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
def _(antall, display, omsetning, priser):
    # review the data
    display(priser.data)
    display(antall.data)
    display(omsetning.nw.to_pandas())
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Detagging
    ---------
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
def _(Taxonomy, klass157):
    # read/write to file -> taxonomies can be defined outside KLASS
    klass157.save('klass157.json')
    file157 = Taxonomy(path='klass157.json')
    return (file157,)


@app.cell
def _(file157, klass157):
    klass157 == file157
    return


if __name__ == "__main__":
    app.run()
