import marimo

__generated_with = "0.24.0"
app = marimo.App()


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _():
    from filetree import tree
    from ssb_timeseries import get_configuration
    CONFIG = get_configuration()
    return CONFIG, tree


@app.cell
def _():
    import subprocess

    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Setup
    """)
    return


@app.cell
def _(CONFIG):
    # both data and metadata will be stored here
    data_path = CONFIG.repositories['tutorials']['directory']['options']['path']
    print(data_path)
    return (data_path,)


@app.cell
def _():
    demo_product = 'demo-data-produkt'
    return


@app.cell
def _(data_path, tree):
    # what is there before we start?
    print(tree(data_path))
    return


@app.cell
def _():
    from ssb_timeseries.dataset import Dataset
    from ssb_timeseries.types import SeriesType, Versioning, Temporality

    return Dataset, SeriesType, Temporality, Versioning


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

    return (pl,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Lasting av data
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Eksempel: momentane data, *uten* versjonering
    """)
    return


@app.cell
def _(SeriesType):
    point_in_time_data = SeriesType('NONE', 'AT')
    return (point_in_time_data,)


@app.cell
def _(create_df):
    def some_simple_data_from_file_or_query(start='2020-01-01', end='2025-06-01'):
        return create_df(['p','q','r'], start_date=start,end_date=end, freq='D')

    return (some_simple_data_from_file_or_query,)


@app.cell
def _(some_simple_data_from_file_or_query):
    pqr_df = some_simple_data_from_file_or_query()
    print(type(pqr_df))
    pqr_df
    return (pqr_df,)


@app.cell
def _(Dataset, point_in_time_data, pqr_df):
    pqr = Dataset(
        name = 'PQR',
        data_type = point_in_time_data,
        data = pqr_df,
    )
    return (pqr,)


@app.cell
def _(pqr):
    type(pqr)
    return


@app.cell
def _(pqr):
    pqr.data
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


@app.cell
def _(pqr):
    pqr.save()
    return


@app.cell
def _(pqr):
    pqr.io.data_dir
    return


@app.cell
def _(data_path, tree):
    print(tree(data_path))
    return


@app.cell
def _(Dataset):
    # reading the data back:
    x = Dataset('PQR')
    x.data    # ... now an Arrow table
    return (x,)


@app.cell
def _(x):
    x.nw.to_pandas()
    return


@app.cell
def _(x):
    x.plot()
    return


@app.cell
def _(some_simple_data_from_file_or_query):
    more_pqr_data = some_simple_data_from_file_or_query('2025-05-29','2025-08-15')
    more_pqr_data
    return (more_pqr_data,)


@app.cell
def _(Dataset, more_pqr_data):
    pqr_second_write = Dataset(
        name = 'PQR',
        data = more_pqr_data,
    )
    # obj init will retrieve previously saved metadata for an existing set and series:
    print(pqr_second_write.tags)
    return (pqr_second_write,)


@app.cell
def _(pqr_second_write):
    pqr_second_write.save()
    return


@app.cell
def _(pqr, pqr_second_write):
    # in memory object instances do not change
    print(pqr.data)
    print(pqr_second_write.data)
    return


@app.cell
def _(Dataset):
    y = Dataset('PQR')
    y.nw.to_polars()
    return (y,)


@app.cell
def _(pl, y):
    # ... but the data file has been overwritten:
    y.nw.to_polars().filter(
        pl.col("valid_at").is_between(pl.date(2025, 5, 29), pl.date(2025, 6, 2))
    )
    return


@app.cell
def _(data_path, tree):
    # note that for unversioned type: we operate on the same files all the way
    print(tree(data_path))
    return


@app.cell
def _(data_path, tree):
    print(tree(data_path))
    return


@app.cell
def _(x):
    x.data = x.nw.to_pandas() # <-- workaround for bug in groupby
    xx = x.groupby('Q','sum')

    # xx is a new dataset, hence gets a new name on creation:
    xx
    return (xx,)


@app.cell
def _(xx):
    xx.data
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Eksempel: data for periode / intervall
    """)
    return


@app.cell
def _(SeriesType, Temporality, Versioning):
    interval_data = SeriesType(Versioning.NONE, Temporality.FROM_TO)
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
    bigger_data.shape
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
        attributes=['butikk','variabel','vare'],
    )
    return (az,)


@app.cell
def _(az):
    az.tags
    return


@app.cell
def _(data_path, tree):
    print(tree(data_path))
    return


@app.cell
def _(az, data_path, tree):
    az.save()
    print(tree(data_path))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    #### Algebra
    """)
    return


@app.cell
def _(az):
    az.tags
    return


@app.cell
def _(az):
    # bug: ValueError: Invalid dir_name: NONE_FROM_TO
    # priser = Dataset('AZ_drikkevarer')[{'variabel':'pris'}]
    # antall = Dataset('AZ_drikkevarer')[{'variabel':'antall'}]
    priser = az[{'variabel':'pris'}]
    antall = az[{'variabel':'antall'}]
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
    omsetning.replace_tags(({'variabel':'pris'},{'variabel':'omsetning'}))
    print(omsetning)
    return


@app.cell
def _(data_path, omsetning, tree):
    omsetning.save()
    print(tree(data_path))
    return


@app.cell
def _(antall, omsetning, priser):
    # review the data
    print(priser.data)
    print(antall.data)
    print(str(omsetning))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Eksempel: momentane data, *med* versjonering
    """)
    return


@app.cell
def _(SeriesType, Temporality, Versioning):
    estimated_point_in_time = SeriesType(Versioning.AS_OF, Temporality.AT)
    return (estimated_point_in_time,)


@app.cell
def _(data_path, tree):
    print(tree(data_path))
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
def _(data_path, tree):
    print(tree(data_path))
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


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Eksempel: data for periode/intervall, *med* versjonering
    """)
    return


@app.cell
def _(SeriesType, Temporality, Versioning):
    estimated_interval_data = SeriesType(Versioning.AS_OF, Temporality.FROM_TO)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ... left as an excercise for the reader.
    """)
    return


if __name__ == "__main__":
    app.run()
