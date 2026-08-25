import marimo

__generated_with = "0.23.1"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _():
    import marimo as mo

    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    # Getting started

    Assuming you have installed and configured the library, you are ready to start coding.
    If not, refer to the [Quick start guide](quickstart.md) for instructions on how to set up.
    """)
    return


@app.cell
def _():
    from ssb_timeseries import get_configuration

    config = get_configuration()
    print(config)
    return (config,)


@app.cell
def _(mo):
    f = mo.ui.file_browser("/tmp/")
    return (f,)


@app.cell
def _(config, f):
    config.save(path=f)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Create a Dataset

    To create our Dataset we need some data.
    Let us generate a dataframe `df` with some random data for three series `x`, `y` and `z`.
    """)
    return


@app.cell
def _():
    from ssb_timeseries.sample_data import xyz_at

    df = xyz_at()
    print(df)
    return (df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Note the structure: one shared `valid_at` column for time and one column for each of the series.

    This is known as "wide format" in Tidy nomenclature.
    While the "long" format is generally preferred in the "Tidyverse", the wide format has its advantages with column oriented storage.
    (See the guide on [how to configure I/O]() for more detail.)
    It also lends itself directly to [matrix and vector maths]() using Numpy.

    The single date signifies a "point in time" *temporality*, `Temporality.AT`.
    The temporality can be combined with any variant of *versioning* to define a `SeriesType` for a `Dataset`.
    See the [core concepts]() and [the datatypes tutorial]() for more about data types.
    For now, let us go with `Versioning.NONE`.
    """)
    return


@app.cell
def _():
    from ssb_timeseries.types import SeriesType
    from ssb_timeseries.types import Temporality
    from ssb_timeseries.types import Versioning

    POINT_IN_TIME = SeriesType(Versioning.NONE, Temporality.AT)
    return (POINT_IN_TIME,)


@app.cell
def _(Dataset, POINT_IN_TIME, df):
    pqr = Dataset("PQR", data_type=POINT_IN_TIME, data=df)
    # pqr
    return


@app.cell
def _(mo):
    mo.md(r"""
    If the configuration points to a repository with existing data, data can be retrieved simply by instantiating a dataset object by its name.

    By default this will collect either all the data or the data corresponding to the latest version. If the dataset does not exist, the call will throw an error. To create a new dataset, we can specify additional parameters, notably .

    Data types
    Types are defined by versioning and temporality:

    from ssb_timeseries.types import SeriesType, Versioning, Temporality

    POINT_IN_TIME = SeriesType(
        Versioning.NONE,
        Temporality.AT)

    PERIOD = SeriesType(
        Versioning.NONE,
        Temporality.FROM_TO)

    POINT_IN_TIME_ESTIMATE = SeriesType(
        Versioning.AS_OF,
        Temporality.AT)

    PERIOD_ESTIMATE = SeriesType(
        Versioning.AS_OF,
        Temporality.FROM_TO)

    These concepts impacts key behaviours and how the data is physically stored. Versioned data require a version marker.

    as_of = '2026-01-01'
    Series data
    Data should be provided as a Narwhals compatible) Dataframe or Arrow table. A dataset can contain any number of numeric series, each represented as a dataframe column, and shared datetime columns corresponding to the data type:

    a single valid_at for points in time

    a pair of valid_from (inclusive) and valid_to (exclusive)

    Example: Point in time data
    The temporality = 'AT' indicates that values are instantaneous, that is valid at an exact point in time.
    """)
    return


@app.cell
def test_true():
    assert True
    assert (1 + 1 == 2)
    return


@app.cell
def _(config):
    assert config.active_file()
    return


if __name__ == "__main__":
    app.run()
