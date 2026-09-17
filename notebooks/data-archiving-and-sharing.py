import marimo

__generated_with = "0.24.0"
app = marimo.App()


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Archiving and sharing
    ---------------------

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

    Configurations at the set level controls how a dataset is shared, but the actual sharing happens when data is persisted.
    Since shared data must be persisted, the `.snapshot()` function takes care of both.
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
    ### Eksempel: momentane data, *uten* versjonering
    """)
    return


@app.cell
def _():
    from ssb_timeseries.sample_data import xyz_at
    from ssb_timeseries.types import SeriesType
    from ssb_timeseries.dataset import SeriesType

    return (xyz_at,)


@app.cell
def _():
    import ssb_timeseries as ts

    return (ts,)


@app.cell
def _():
    set_name = 'A Sample Dataset'
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
    ...
    """)
    return


@app.cell
def _(q):
    statistics_product = 'The Sample Statistic'
    q.process_stage = 'statistikk'
    q.product = statistics_product
    return (statistics_product,)


@app.cell
def _():
    #q.snapshot()
    return


@app.cell
def _(CONFIG):
    CONFIG.configuration_file= '/home/bernhard/code/ssb-timeseries/notebooks/sharing_config.json'
    CONFIG.activate()
    CONFIG.refresh()
    #CONFIG.__dict__
    return


@app.cell
def _(q):
    q.snapshot()
    return


@app.cell
def _(data_path, tree):
    print(tree(data_path))
    return


@app.cell
def _(statistics_product, treee, ts):
    # let us differentiate sharing
    r = ts.dataset.Dataset("XYZ")
    r.sharing = [{'team': 's123', 'path': f'/home/bernhard/timeseries/{statistics_product}/shared/s123/'}, {'team': 's234', 'path': f'/home/bernhard/timeseries/{statistics_product}/shared/s234/'}]
    r.snapshot()
    treee()
    return


@app.cell
def _(statistics_product, ts):
    s = ts.dataset.Dataset("PQR")
    s.process_stage = 'statistikk'
    s.sharing = [
        {"team": "s234", "path": f'/home/bernhard/timeseries/{statistics_product}/shared/s234/'},
    ]
    s.snapshot()
    return


@app.cell
def _(treee):
    treee()
    return


@app.cell(hide_code=True)
def _(data_path, tree):
    print(tree(data_path))
    return


if __name__ == "__main__":
    app.run()
