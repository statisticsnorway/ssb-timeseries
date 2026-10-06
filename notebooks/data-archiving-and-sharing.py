import marimo

__generated_with = "0.24.2"
app = marimo.App()


@app.cell(hide_code=True)
def _():
    import marimo as mo

    # this guide should start with an empty data repository
    return (mo,)


@app.cell(hide_code=True)
def _():
    from filetree import tree
    from ssb_timeseries import get_configuration
    CONFIG = get_configuration()
    return CONFIG, tree


@app.cell(hide_code=True)
def _():
    import textwrap

    def print_err_and_continue(c, width:int=80):
        try:
            c()
        except Exception as ex:
            wrapped = textwrap.fill(str(ex), width=width)

            print(f"{'-'*32} ERROR OCCURRED {'-' *32}\n{wrapped}\n{'-'*80}")

    return (print_err_and_continue,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Archiving and sharing
    =====================

    Motivation
    ----------

    Statistics Norway collects data from a vast number of sources.
    We have a broad mandate, but are also subject to strict regulations and reviews.
    Our production platform is designed to limit access to data, and we commit to a formal process model so that we can prove that we operate within the mandate.
    Persisting data in stable states is a key element in how this translates into practice.

    The practical implication is that at defined points in the statistics production pipeline, the data has to be persisted.
    Stricter requirements apply than just saving.
    First, immutability: the persisted data must be stored "forever", without being subject to change.
    Second, conventions apply to storage location, technology and format, naming and documentation.

    Data shared between different statistics are subject to similar restrictions.
    The requirements are not particularily hard to meet, per se.
    In both cases, the conventions are designed for transparency and review, not for efficient data handling.

    Our solution is to separate "archiving" and "sharing" from ordinary "reading" and "writing".
    In trivial use cases it does not really matter.
    Beyond those, we keep our options open, and can switch technologies freely while conventions dictate that archiving and sharing are always file based.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Scope
    -----

    Here we focus on the practical side.
    The I/O abstractions are described in more detail in the architecture section, and there is a separate guide to I/O configurations.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Prerequisites
    -------------

    Since the sole purpose is to separate archiving and sharing from reading and writing, additional configuration is required.
    Sharing and archiving have their own I/O handlers and repository entries.

    With the global configuration in place, dataset level properties control how individual sets are archived or shared.
    """)
    return


@app.cell(hide_code=True)
def _(CONFIG, tree):
    data_path = CONFIG.repositories['tutorials']['directory']['options']['path']
    def treee():
        print(tree(data_path))

    return data_path, treee


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Create some sample data
    -----------------------

    Example: point-in-time data, *without* versioning
    """)
    return


@app.cell(hide_code=True)
def _():
    from ssb_timeseries.sample_data import xyz_at

    return (xyz_at,)


@app.cell
def _():
    from ssb_timeseries.dataset import Dataset
    from ssb_timeseries.types import SeriesType

    return Dataset, SeriesType


@app.cell
def _(treee):
    # what is there before we start?
    treee()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    For data in between the stable "checkpoint" states, naming is unconstrained.
    The naming conventions for archiving limits character usage and allow no spaces.
    """)
    return


@app.cell
def _():
    set_name = 'Sample Dataset to be Archived'
    return (set_name,)


@app.cell
def _(Dataset, SeriesType, set_name, xyz_at):
    p = Dataset(
        name = set_name,
        data_type = SeriesType('NONE','AT'),
        data = xyz_at(),
    ).save()
    return


@app.cell
def _(treee):
    # what is there after the .save():
    treee()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Dataset properties
    ------------------
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    We read the data back, just because we can, and because there is no guarantee that the archiving happens in the same process (or at the same frequency) that the data is gathered, calculated or saved.
    """)
    return


@app.cell
def _(Dataset, set_name):
    q = Dataset(set_name)
    return (q,)


@app.cell
def _(q):
    statistics_product = 'Our Sample Statistic'
    q.process_stage = 'statistics'
    q.product = statistics_product
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    A team can be responsible for one or more "statistical products" or "data products".
    Outsiders to Statistics Norway can safely ignore the nuance.

    The `process stage` is a named stable state, or "checkpoint" in the production process.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Archiving
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    Then, the actual action happens via `Dataset.archive()`.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    The `.archive()` function takes care of both archiving and sharing.
    This reflects the conventions that applies in Statistics Norway, but is technically a remnant from early PoC phase.

    **Warning:** It may be subject to change later.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Archiving writes to the archive and, for each sharing key, to the
    destination that key names.
    The archive path is built from the tags, so `product` and `process_stage` are set
    before the dataset is archived.
    """)
    return


@app.cell
def _(print_err_and_continue, q):
    print_err_and_continue( q.archive )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Surprise.
    We ignore proper change management and rename.
    Note that the rename creates a new dataset, and any data written earlier that was not read back now will be left behind in the old set.
    """)
    return


@app.cell
def _(q):
    q.rename("SampleDatasetForArchiving")
    q.save() # it will fail again if we do not save first.
    q.archive()
    return


@app.cell
def _(treee):
    print(treee())
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Note that we have both the original dataset and the copy, and the `/archives` part:
    """)
    return


@app.cell
def _(data_path, tree):
    print(tree(f'{data_path}/archives'))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Sharing
    -------

    We silently create some more data, and read it back before setting slightly different properties:
    """)
    return


@app.cell(hide_code=True)
def _(Dataset, SeriesType, xyz_at):
    from ssb_timeseries.sample_data import create_df

    # Archiving and sharing apply to any dataset, not only the one created above.
    # The datasets used by the other guides are created here, from the same
    # generators, so that this guide runs on its own and writes identical data.
    for name, data in (
        ("XYZ", xyz_at()),
        (
            "PQR",
            create_df(
                ["p", "q", "r"],
                start_date="2020-01-01",
                end_date="2025-06-01",
                freq="D",
                temporality="AT",
            ),
        ),
    ):
        Dataset(
            name = name,
            data_type = SeriesType('NONE','AT'),
            data = data,
        ).save()
    return


@app.cell
def _(Dataset):
    # let us differentiate sharing
    r = Dataset("XYZ")
    r.sharing = ["s123", "s234"]
    r.archive()
    return


@app.cell
def _(Dataset):
    s = Dataset("PQR")
    s.process_stage = "statistics"
    s.sharing = ["s234"]
    s.archive()
    return


@app.cell
def _(data_path, tree):
    # each sharing key falls back to the destination configured as "default":
    print(tree(f'{data_path}/shared'))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Observing what happened in `archives/` we see what happens if the `product`, or both `product` and `process_stage` is empty - the tree is flattened.
    """)
    return


@app.cell
def _(data_path, tree):
    print(tree(f'{data_path}/archives'))
    return


@app.cell
def _(treee):
    print(treee())
    return


if __name__ == "__main__":
    app.run()
