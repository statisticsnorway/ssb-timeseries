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


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Data types and storage

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

    from ssb_timeseries.sample_data import create_df
    from ssb_timeseries.dates import ensure_datetime, date_utc

    return create_df, date_utc, ensure_datetime, timedelta


@app.cell
def _():
    import polars as pl
    from datetime import datetime

    return pl, datetime


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Saving data
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Example: point-in-time data, *without* versioning
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
    pqr.tag_dataset(tags={'variable': 'price','product group': 'essentials'})

    pqr.tag_series('p',tags={'product': 'coffee'})
    pqr.tag_series('q',tags={'product': 'crispbread'})
    pqr.tag_series('r',tags={'product': 'brown cheese'})

    pqr.tags
    return


@app.cell
def _(pqr):
    pqr.save()
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
    ### Example: data for periods, *without* versioning
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
        variables = ['quantity', 'price']
        goods = ['coffee', 'tea', 'soda', 'beer', 'wine']
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
        name = 'AZ_beverages',
        data_type = interval_data,
        data = bigger_data,
        attributes=['store','variable','product'],
    )
    return (az,)


@app.cell
def _(az):
    az.tags
    return


@app.cell
def _(az):
    # the periods need two date columns, since they have a duration:
    az.data
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
    ### Example: point-in-time data, *with* versioning
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
    ### Example: data for periods, *with* versioning
    """)
    return


@app.cell
def _(SeriesType, Temporality, Versioning):
    estimated_interval_data = SeriesType(Versioning.AS_OF, Temporality.FROM_TO)
    return (estimated_interval_data,)


@app.cell
def _(create_df):
    def monthly_periods(start, end):
        return create_df(
            ['tea', 'coffee'],
            ['quantity'],
            start_date=start,
            end_date=end,
            freq='M',
            temporality='FROM_TO',
            implementation='polars'
        )

    return (monthly_periods,)


@app.cell
def _(monthly_periods):
    beverage_periods = monthly_periods('2025-01-01', '2025-06-01')
    beverage_periods
    return (beverage_periods,)


@app.cell
def _(Dataset, beverage_periods, estimated_interval_data):
    bno = Dataset(
        name = 'BNO',
        data_type = estimated_interval_data,
        data = beverage_periods,
        attributes = ['product', 'variable'],
    )
    return (bno,)


@app.cell
def _(bno):
    bno.tags
    return


@app.cell
def _(bno):
    # the periods need two date columns, since they have a duration:
    bno.data
    return


@app.cell
def _(Dataset, beverage_periods, date_utc, estimated_interval_data):
    # every production run writes a new file, named after the as of date
    for _as_of in ['2025-06-01', '2025-08-07']:
        Dataset(
            name = 'BNO',
            data_type = estimated_interval_data,
            as_of_tz=date_utc(_as_of),
            data = beverage_periods,
        ).save()
    return


@app.cell
def _(data_path, tree):
    # versioning and temporality together decide the folder layout:
    print(tree(f'{data_path}/AS_OF_FROM_TO'))
    return


@app.cell
def _(Dataset):
    bno_june = Dataset('BNO', as_of_tz='2025-06-01')
    return (bno_june,)


@app.cell
def _(bno_june):
    bno_june.nw.to_pandas()
    return


@app.cell
def _(bno_june):
    # the as of date is not part of the data, it identifies the version:
    'as_of' in bno_june.nw.columns
    return


@app.cell
def _(bno_june):
    from ssb_timeseries.io import versions

    # ... it is the version marker of the file the data is read from:
    versions(bno_june)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### What the series type decides

    | series type | date columns in `.data` | stored as |
    |---|---|---|
    | `SeriesType(NONE, AT)` | `valid_at` | one file, `NONE_AT/<name>/<name>-latest-data.parquet` |
    | `SeriesType(NONE, FROM_TO)` | `valid_from`, `valid_to` | one file, `NONE_FROM_TO/<name>/<name>-latest-data.parquet` |
    | `SeriesType(AS_OF, AT)` | `valid_at` | one file per version, `AS_OF_AT/<name>/<name>-as_of_<timestamp>-data.parquet` |
    | `SeriesType(AS_OF, FROM_TO)` | `valid_from`, `valid_to` | one file per version, `AS_OF_FROM_TO/<name>/<name>-as_of_<timestamp>-data.parquet` |

    Without versioning, a save merges the new data into the single existing file, as seen above with PQR.
    With `AS_OF`, a save never overwrites: it adds a file, and the `as_of` column it writes there is a storage detail that `.data` does not expose.
    """)
    return


if __name__ == "__main__":
    app.run()
