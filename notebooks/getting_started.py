import marimo

__generated_with = "0.24.0"
app = marimo.App(width="comnpact")


@app.cell(hide_code=True)
def _():
    import marimo as mo
    from tools import testing

    mo.Html(
        """
        <style>
        [data-testid="static-notebook-banner"],
        [data-testid="watermark"] {
            display: none !important;
        }
        z-index: 10; /* Higher numbers sit on top of lower numbers */
        </style>
        """
    )
    return mo, testing


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Getting Started

    ## Introduction

    The time series library was created to operate in lieu of a fully blown time series system.
    There is no services or compute, so the library does not *do* anything by itself.
    However, it provides the most key functionality that a full system would need to include for storage and analysis, metadata and automated workflows.
    That means it can be used to build a complete system, or to interact with the services and components forming one.

    Here we focus on the most basic operations: finding, reading and writing data.

    We will cover the basic features for understanding and manipulating the data, and organising it for presentation or calculation purposes.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    /// note | Configuration depencency

    The code below assumes access to a working configuration.
    See the [Quick start guide](quickstart.md) for how to prepare it.
    ///
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Datasets as first class citizens

    The library is designed to support processes that are heavily batch oriented.
    Hence it is constructed to work with *datasets* as a primary unit,
    and read and write (and most other functionality) operates on the dataset level.
    While it is possible to work on individual series or values, this strongly encourages writing vectorised code.

    Practically, it the `Dataset` class becomes the centerpiece of the library.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Create a dataset

    To create our first `Dataset` we need some data.
    Let us generate a dataframe `df` with some random data for three series.
    The library used to create the dataframe (here: Pandas) does not matter, [any Narwhals compatible library will do](compatibility.md).
    """)
    return


@app.cell
def _():
    from ssb_timeseries.sample_data import xyz_at

    df = xyz_at()

    print(type(df))
    print(df)
    return (df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    Note the structure: one shared `valid_at` column and one column for each of the series  `x`, `y` and `z`. The single date signifies a "point in time" *temporality*, `Temporality.AT`.

    The temporality can be combined with any variant of *versioning* to define a `SeriesType` for a `Dataset`
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
def _(mo):
    mo.md(r"""
    See the [core concepts]() and [the datatypes tutorial]() for more about data types.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    This is all we need to define a `Dataset` object. Let us call it "XYZ".
    """)
    return


@app.cell
def _(POINT_IN_TIME, df):
    from ssb_timeseries.dataset import Dataset

    xyz = Dataset("XYZ", data_type=POINT_IN_TIME, data=df)
    xyz
    return Dataset, xyz


@app.cell
def _(mo):
    mo.md(r"""
    We now have a `Dataset` object with name "XYZ" assigned to the variable `xyz`.
    The object lives in memory only.
    If our configuration specifies a repository, we can save it.
    The set will be written to the default repository if more than one is configured.
    """)
    return


@app.cell
def _(xyz):
    xyz.save()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""

    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Having saved the dataset, we can read it back.
    If we do not specify and interval, this will collect either all the data or for the latest version.
    Since we chose `Versioning.NONE` we expect all the data.
    """)
    return


@app.cell
def _(Dataset):
    read_xyz_back = Dataset("XYZ")
    return (read_xyz_back,)


@app.cell
def _(mo):
    mo.md(r"""
    Note the std_out indicating that both reading and writing was logged.
    This can be leveraged in automated workflows.
    The logging behaviour can be modified in the configuration.
    """)
    return


@app.cell
def _(read_xyz_back, xyz):
    check_if_they_are_equal = xyz == read_xyz_back
    return (check_if_they_are_equal,)


@app.cell
def _(mo):
    mo.md(r"""
    Note how the check returned a new dataset object, with a new name automatically generated.
    A new dataset object is the standard behaviour for all [calculations](calculations.rst) performed by the library.
    Note also the [lineage naming](lineage.md) that tells which operations were performed.

    Inspecting the `.data` property of the new set we can verify that all values are equal:
    """)
    return


@app.cell
def _(check_if_they_are_equal):
    check_if_they_are_equal.data
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    A more direct check is `Dataset.all()` to check if all the values for the series (ie. not the dates) evaluate to `True`.
    """)
    return


@app.cell
def _(check_if_they_are_equal):
    check_if_they_are_equal.all()
    return


@app.cell(hide_code=True)
def _():
    return


@app.cell(hide_code=True)
def _():
    return


@app.cell(hide_code=True)
def _(Dataset, xyz):
    def test_xyz_is_a_dataset():
        assert isinstance(xyz, Dataset)

    return (test_xyz_is_a_dataset,)


@app.cell(hide_code=True)
def _(check_if_they_are_equal):
    def test_xyz_is_equal_to_itself():
        assert check_if_they_are_equal.all()

    return (test_xyz_is_equal_to_itself,)


@app.cell(hide_code=True)
def _(test_xyz_is_a_dataset, test_xyz_is_equal_to_itself, testing):
    testing.run_and_report([test_xyz_is_a_dataset, test_xyz_is_equal_to_itself])
    return


@app.cell(hide_code=True)
def _():
    return


if __name__ == "__main__":
    app.run()
