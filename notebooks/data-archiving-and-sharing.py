import marimo

__generated_with = "0.24.0"
app = marimo.App()


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Archiving and sharing
    =====================

    To comply with legal requirements, Statistics Norway commits itself to working according to a formal process model.
    For the sake of transparency and process reviews, at certain points in the process data has to be persisted.
    That does not simply mean the data must be saved.
    Stricter requirements apply.
    First, immutability: the persisted data must be stored "forever", without being subject to change.
    Second, conventions apply to storage formats, naming and documentation.

    Data shared between different statistics are subject to the same restrictions.

    The conventions that apply are designed for archive and review purposes, not to for efficient data manipulation or retrievel.
    That is contrary to the purpose of the SSB Timeseries library,
    which is the reason "archiving" and "sharing" are treated differently from ordinary reads and writes.

    Configurations at the set level control how a dataset is archived and shared, but the actual writing happens when data is persisted.
    Since an archive is a persisted copy, the `.archive()` function takes care of both.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Setup
    """)
    return


@app.cell(hide_code=True)
def _():
    import marimo as mo

    return (mo,)


@app.cell(hide_code=True)
def _():
    from filetree import tree
    from ssb_timeseries import get_configuration
    CONFIG = get_configuration()
    return CONFIG, tree


@app.cell
def _(CONFIG, tree):
    data_path = CONFIG.repositories['tutorials']['directory']['options']['path']
    def treee():
        print(tree(data_path))
    treee()
    return data_path, treee


@app.cell
def _(treee):
    # what is there before we start?
    treee()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Example: point-in-time data, *without* versioning
    """)
    return


@app.cell
def _():
    from ssb_timeseries.sample_data import xyz_at

    return (xyz_at,)


@app.cell
def _():
    import ssb_timeseries as ts

    return (ts,)


@app.cell
def _():
    # the ssb archive convention allows no spaces in a name:
    set_name = 'SampleDataset'
    return (set_name,)


@app.cell
def _(set_name, ts, xyz_at):
    p = ts.dataset.Dataset(
        name = set_name,
        data_type = ts.types.SeriesType('NONE','AT'),
        data = xyz_at(),
    ).save()
    return


@app.cell
def _(treee):
    # what is there after the .save():
    treee()
    return


@app.cell
def _(set_name, ts):
    #read the data back, just because we can
    q = ts.dataset.Dataset(set_name)
    return (q,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Archiving
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
def _(q):
    statistics_product = 'The Sample Statistic'
    q.process_stage = 'statistics'
    q.product = statistics_product
    return (statistics_product,)


@app.cell
def _(q):
    q.archive()
    return


@app.cell
def _(data_path, tree):
    print(tree(f'{data_path}/archives'))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Sharing

    Archiving and sharing apply to any dataset, not only the one created above.
    The two datasets used by the other guides are created here so that this guide runs on its own.
    """)
    return


@app.cell
def _(ts, xyz_at):
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
        ts.dataset.Dataset(
            name = name,
            data_type = ts.types.SeriesType('NONE','AT'),
            data = data,
        ).save()
    return


@app.cell
def _(ts):
    # let us differentiate sharing
    r = ts.dataset.Dataset("XYZ")
    r.sharing = ["s123", "s234"]
    r.archive()
    return


@app.cell
def _(ts):
    s = ts.dataset.Dataset("PQR")
    s.process_stage = "statistics"
    s.sharing = ["s234"]
    s.archive()
    return


@app.cell
def _(data_path, tree):
    # each sharing key falls back to the destination configured as "default":
    print(tree(f'{data_path}/shared'))
    return


if __name__ == "__main__":
    app.run()
