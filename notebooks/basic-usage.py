import marimo

__generated_with = "0.24.0"
app = marimo.App(width="comnpact", html_head_file="resources/custom.css")


@app.cell(hide_code=True)
def _():
    import marimo as mo
    import testing

    import inspect
    import textwrap
    from collections.abc import Callable
    from typing import Any, TypeVar

    F = TypeVar("F", bound=Callable[..., Any])

    def prompt(cmd, str=''):
        """print a prompt with command before output str"""
        if cmd:
            return f"\n```\n>>> {cmd}\n\n{str}\n```\n"
        else:
            return  f"\n```\n{str}\n```\n"

    from tabulate import tabulate
    _tbl_format = 'simple'
    _float_format=".2f"
    def tbl(df, cmd=''):
        """print a str formatted table"""
        tbl_str = tabulate(
            df,
            headers = df.columns,
            tablefmt = _tbl_format,
            floatfmt =_float_format,
            showindex=False,
        )
        return prompt(cmd, tbl_str)

    from ssb_timeseries.types import Versioning, Temporality

    return Temporality, Versioning, mo, prompt, testing


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    BEGIN GUIDE
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Basic Usage

    ## Introduction

    The time series library was created to operate in lieu of a fully blown timeseries system.

    Being a code libarary it does not provide any services, of its own, but is built to *manage* or *communicate* with a storage layer, and metadata and workflow orchestration services.

    Its main responsibility is to connect an information model and analytic features to loosely coupled services that combines into a complete solution.
    It is designed mainly to facilitate quantitative analysis in code, but can also be the foundation for GUI applications for charts, tables and dashboards.

    That means it provides (or connects to) all the main building blocks required to build a full, but lightweight, timeseries system.

    This guide focuses on the most basic operations: reading and writing `Datasets`, and some basic features and manipulations.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    Configuration depencency:
    The code below assumes access to a working configuration.
    See the [Quick start guide](quickstart.md) for how to prepare it.
    """)
    return


@app.cell
def _():
    import ssb_timeseries as ts

    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Datasets

    The statistics production processes are largely batch oriented and encourages writing vectorised code.
    The library is therefore constructed to work with *datasets* as a primary unit of analysis.
    While it is possible to work on individual series or values, the library is built around the idea that datasets are matrices and series are vectors.

    Practically, this means that the `Dataset` class is the very core of the library.
    Reads and writes (and most other core functionality) operates on the dataset level.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Create a dataset

    To create our first `Dataset` we need some data.
    Let us generate a dataframe `df` with some random data for three series.
    The library used to create the dataframe (here: Pandas) does not matter, [any Narwhals compatible library will do](interoperability.md).
    """)
    return


@app.cell
def _():
    from ssb_timeseries import sample_data
    df = sample_data.xyz_at()
    return (df,)


@app.cell
def _(df):
    df
    return


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
    POINT_IN_TIME = SeriesType('NONE', 'AT')
    return POINT_IN_TIME, SeriesType


@app.cell(hide_code=True)
def _(SeriesType, Temporality, Versioning, mo):
    mo.md(f"""
    `SeriesType('NONE', 'AT')` is a shorthand that resolves to `{repr(SeriesType(Versioning.NONE, Temporality.AT))}`.

    See the [core concepts](..info-model) and [the datatypes tutorial]()-types-and-storage for more about data types.
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
    xyz = Dataset("XYZ", data_type=POINT_IN_TIME, data=df,)
    return Dataset, xyz


@app.cell(hide_code=True)
def _(mo):
    mo.md(f"""
    We now have a `Dataset` object with name "XYZ" assigned to the variable `xyz`.
    """)
    return


@app.cell
def _(xyz):
    repr(xyz)
    return


@app.cell(hide_code=True)
def _(mo, xyz):
    mo.tree(xyz.__dict__)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Write a dataset
    """)
    return


@app.cell
def _(mo):
    mo.md(f"""
    The object lives in memory only untill we save it.
    Saving relies on configuration to tell which *repositories* it can go into.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    If more than one repository is configured, we can specify which we want to write to.
    If we don't, an existing set will be written to the repository it was read from and a new set will be written to the default repository.
    """)
    return


@app.cell
def _(xyz):
    xyz.save()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Dataset .data and .tags
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(f"""
    We find `df` as `Dataset.data`.
    """)
    return


@app.cell(hide_code=True)
def _(mo, xyz):
    mo.md(f"""
    {xyz.pd.to_markdown()}
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(f"""
    Note that the original type of `.data` is not persisted over saving and reading back, or calculations.
    That should not matter for operations on the dataset itself,
    and interoperability shorthands like `pa`, `pd` and `pl` are available to request a specific implemewntation.
    """)
    return


@app.cell
def _(mo):
    mo.md(f"""
    The `.tags` attribute package a dictionary of metadata:
    """)
    return


@app.cell
def _(mo, xyz):
    mo.tree(xyz.tags)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    For our sample set, we have not made any effort to descrtibe our data, so only a minimal set of technical attributes are applied.
    Descriptive attributes, or "tags" can be specified as well. See [tag maintenance](tag-maintenance).
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""

    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    Tags apply at both the `Dataset` and `Series` levels.
    The technical implementation for the tags is a key-value structure in the form of a Python `dictionary`.
    Some rules and conventions that apply are described in the [core concepts](..info-model), and other guides go deeper into [search and filtering](meta-search-and-filtering), [tag maintenance](meta-tag-maintenance) and [calculations with metadata](calc-with-metadata).
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Read a dataset
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    For an existing dataset, initating a `Dataset("<name>")` will read it.
    If we do not specify and interval, reading will collect either all the data (for `Versioning.NONE`) or the latest version (for `Versioning.AS_OF`).
    """)
    return


@app.cell
def _(Dataset):
    read_xyz_back = Dataset("XYZ")
    return (read_xyz_back,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(f"""
    You may see several lines of text written to `std_out` for both reading and writing.
    The logging behaviour can be modified in the configuration.
    The logging of reading and writing provides data lineage, and can be leveraged for orchestration: adding a queue or API logger allows even driven workflows.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(f"""
    ### Deriving new datasets

    The library has built in funcitonality for basic arithmetics and linear algebra, calculations with time and calculations with metadata.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(f"""
    Other groups have limited or no functionality at the time of writing, but may be added later:
    - Logical functions
    - Set functions
    - Unit conversion
    - Currency conversion
    - Indexing
    """)
    return


@app.cell
def _(read_xyz_back, xyz):
    check_equality = (xyz == read_xyz_back)
    return (check_equality,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(f"""
    The calculation returns a new `Dataset` object.
    Inspect the data to very that all values are equal:
    """)
    return


@app.cell(hide_code=True)
def _(check_equality, mo):
    mo.md(f"""
    {check_equality.pd.to_markdown()}
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    The boolean return type is semi-supported for now:
    That is sufficient for intermediate calculations, but most functionality will fail and the storage model will require casting to numbers.

    A more direct check for the test above is `Dataset.all()` to check if all the values for the series (ie. not the dates) evaluate to `True`.
    """)
    return


@app.cell
def _(check_equality):
    check_equality.all()
    return


@app.cell(hide_code=True)
def _(check_equality, mo, prompt):
    mo.md(f"""
    {prompt('check_equality.all()',check_equality.all())}
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    The new dataset object got a new name automatically generated.
    The same thing can be observed for the filter and multiplication below.

    A new dataset object with a new name is the standard behaviour for all [calculations](calculations.md) performed by the library.
    The new name is an important safeguard against destroying data.
    The "lineage naming" that tells which operations were performed hints about the larger topic of [data lineage](lineage.md).

    In real production code, the calculation of any dataset that we intend to save should be followed by a `Dataset.rename()` and updating the [descriptive metadata]() to reflect whichever calculations where performed.
    Functions and guidelines for [metadata maintenance](meta-tag-maintenance) is a topic in itself.
    """)
    return


@app.cell
def _(xyz):
    big_xyz = xyz['x', 'y'] *1000
    big_xyz.plot()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    Planned: Iterators
    ------------------

    While many of the most used calculation features are implemented for the `Dataset` objects, iterating over `Series` ... --> TODO.
    """)
    return


@app.cell(hide_code=True)
def _(Dataset, check_equality, xyz):
    #@suppress

    def test_xyz_is_a_dataset():
        assert isinstance(xyz, Dataset)

    def test_xyz_is_equal_to_itself():
        assert check_equality.all()

    return test_xyz_is_a_dataset, test_xyz_is_equal_to_itself


@app.cell(hide_code=True)
def _():
    return


@app.cell(hide_code=True)
def test_run_all(test_xyz_is_a_dataset, test_xyz_is_equal_to_itself, testing):
    testing.run_and_report([test_xyz_is_a_dataset, test_xyz_is_equal_to_itself])
    return


if __name__ == "__main__":
    app.run()
