# ruff: noqa
import marimo

__generated_with = "0.23.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    # Getting started

    Assuming you have installed and configured the library, you are ready to start coding.
    If not, refer to the [getting_started.md](Quick start guide) for instructions on how to set up.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    from ssb_timeseries.dataset import Dataset
    If the configuration points to a repository with existing data, data can be retrieved simply by instantiating a dataset object by its name.

    pqr = Dataset("PQR")
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


if __name__ == "__main__":
    app.run()
