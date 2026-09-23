import marimo

__generated_with = "0.24.0"
app = marimo.App(width="full")


@app.cell(hide_code=True)
def _():
    import marimo as mo
    import testing
    from mdtools import catalog_item_list_to_df, hex

    return catalog_item_list_to_df, hex, mo, testing


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Introduksjon til SSB Timeseries
    ===============================

    Bernhard Ryeng, September 2026
    ------------------------------
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    SSB Timeseries
    --------------
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    - **Kodebibliotek** som later som det er et fullverdig **tidsseriesystem**
    - [Anbefalt retning for utvikling](https://adr.ssb.no/0031-fellesloesning-for-haandtering-av-tidsserier/)
    - [statisticsnorway/ssb-timeseries](https://github.com/statisticsnorway/ssb-timeseries)
    - [Installasjon og konfigurasjon](https://statisticsnorway.github.io/ssb-timeseries/guides/quickstart.html)
    - [Grunnleggende bruk](https://statisticsnorway.github.io/ssb-timeseries/guides/basic-usage.html)
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(f"""
    Tilnærmingen
    ------------
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(f"""
    - Abstrakt: Informasjonsmodell
      - `Repository` > `Dataset` > `Series`
      - metadata knyttes til objekter via "tagger"
      - typesystem: sammenheng mellom tid og verdier

    - Konkret: Teknologi
      - Python.
      - Fleksibel mht lagring i filer/databaseer og eksterne tjenester
      - Trekker veksler på stort økosystem
        og biblioteker som Numpy, Pandas, Polars, Pyarrow
      - Tilpasset DAPLA
    """)
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Typesystem
    ----------
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    - Versjonering
    - Temporalitet

    Konseptuelle forskjeller som legger føringer for *grunnleggende* teknisk implementasjon.

    Andre ting, som fast vs variabel frekvens gjør forskjell for enkelte funksjoner.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Demo
    ====
    """)
    return


@app.cell
def _():
    from ssb_timeseries import get_catalog
    from ssb_timeseries.dataset import Dataset

    return Dataset, get_catalog


@app.cell(hide_code=True)
def _():
    from ssb_timeseries.types import SeriesType
    from ssb_timeseries.sample_data import create_df
    from itertools import product
    from datetime import date

    return SeriesType, create_df, date, product


@app.cell(hide_code=True)
def _(Dataset, SeriesType, create_df, date):
    def create_some_example_data(
        set_name: str,
        as_of_dates: list[date],
        series_tags: dict[str,list[str]],
        freq='M'
    ):
        """Generate and save some sample data."""
        set_tags = { "Country": "Norway" }
        for d in as_of_dates:
            df = create_df(
                *[value for value in series_tags.values()],
                temporality= 'AT',
                start_date="2025-01-01",
                end_date="2026-12-01",
                freq=freq,
            )
            Dataset(
                name=set_name,
                data_type=SeriesType('AS_OF', 'AT'),
                as_of_tz=str(d),
                data=df,
                tags = set_tags,
                attributes = series_tags.keys(),
            ).save()

    return (create_some_example_data,)


@app.cell(hide_code=True)
def _(create_some_example_data, date, product):
    create_some_example_data(
        set_name="ABC",
        as_of_dates = [date(*d) for d in product({2024,2025}, range(1,13), {1})],
        series_tags = {'area': ["x", "y","z"], 'product': ['coffee', 'tea'], 'var':['price']},
        freq='D',
    )
    return


@app.cell(hide_code=True)
def _(create_some_example_data, date, hex, product):
    for i in range(10):
        set_name = hex("XYZ")
        create_some_example_data(
            set_name=set_name,
            as_of_dates = [date(*d) for d in product({2024,2025}, range(1,13), {1})],
            series_tags = {'area': ["x", "y","z"]},
            freq='M',
        )
    return (set_name,)


@app.cell(disabled=True, hide_code=True)
def _(create_df):
    # bigger example - not needed?
    tags = {"Var": ["price", "volume"], \
            "Product": ["milk", "eggs", "bread", "cheese", "ham"], \
            "Store": ["A", "B", "C", "D", "E"], \
            "Region": ["N", "S", "E", "W", "NE", "NW", "SE", "SW"]}

    bigger_data = create_df(
        *[value for value in tags.values()],
        start_date="2000-12-01",
        end_date="2024-01-01",
        freq="MS",
        implementation="pandas").set_index('valid_at')
    return


@app.cell
def _(get_catalog):
    db = get_catalog()

    everything= db.items()
    all_sets = db.datasets()
    all_series = db.series()

    print(f"{len(everything)} items, {len(all_series)} series and {len(all_sets)} datasets.")
    return all_sets, db


@app.cell
def _(all_sets, catalog_item_list_to_df):
    catalog_item_list_to_df(all_sets)
    return


@app.cell
def _(catalog_item_list_to_df, db):
    series = db.series(tags={'product':'coffee', 'area':'x'})
    catalog_item_list_to_df(series)
    return (series,)


@app.cell
def _(series):
    series[0].object_tags
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Endring fra versjon til versjon:
    --------------------------------
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ... for *versjonerte* datasett lagres data for hver `as_of` dato.
    """)
    return


@app.cell
def _(Dataset):
    n= "ABC"
    jul = Dataset(name=n, as_of_tz="2025-07-01")
    feb = Dataset(name=n, as_of_tz="2025-02-01")

    change_from_feb_to_july = jul - feb
    return change_from_feb_to_july, jul


@app.cell
def _(change_from_feb_to_july):
    change_from_feb_to_july.plot()
    return


@app.cell
def _(change_from_feb_to_july):
    change_from_feb_to_july.tags
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    [Elementvise operasjoner](calc-basic-arithmetic) med Numpy under panseret.

    - støtter flere objekttyper: skalarer, matriser/vektorer, sett/serier som passer med [Numpy "broadcasting rules"](https://numpy.org/doc/stable/user/basics.broadcasting.html)
    - evaluert for serieverdiene (numeriske kolonner)
    - ingen magisk datomatching --> tillater diff som over
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""

    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(f"""

    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Intervaller og datofiltrering
    ------------------------------
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ... trenger mer utvikling.

    I mellomtiden kan vi støtte oss på standardbibliotekene i Python-økosystemet:
    Integrasjonsbiblioteket **Narwhals** --> `Dataset.pa`, `.nw`, `.pd`, and `.pl` for Arrow tables og Narwhals, Pandas og Polars dataframes.
    """)
    return


@app.cell
def _(jul):
    jul.pl
    return


@app.cell
def _():
    import polars as pl

    return (pl,)


@app.cell
def _(jul, pl):
    d_from = pl.date(2025, 3, 1)
    d_to = pl.date(2025, 7, 15)
    jul_filtered = jul.pl.filter( pl.col("valid_at").is_between(d_from, d_to) )
    jul_filtered
    return


@app.cell
def _():
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""

    """)
    return


@app.cell
def _():
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Funksjoner over tidsaksen(e)
    ----------------------------
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    - Aggregering (group by)
      - (Re)sampling
      - Moving average.

    Planlagte enkle utvidelser:
      - Indeksering
      - Direkte støtte for diff, shift, cumsum (alle tilgjengelige via eksterne biblioteker)

      - Changing types.

    Proper timeseries analysis and seasonal adjustment. (Planned integrations.)
    """)
    return


@app.cell
def _(jul, pl):
    jul.pl.describe().select(pl.col(["statistic", "valid_at"]))
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Resample
    --------
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    --------
    """)
    return


@app.cell
def _(set_name):
    set_name
    return


@app.cell
def _(Dataset):
    xyz_monthly = Dataset("XYZ_c71b3793")
    xyz_monthly.plot()
    return (xyz_monthly,)


@app.cell
def _(xyz_monthly):
    xyz_d_ffill = xyz_monthly.resample('D','ffill')
    xyz_d_ffill.plot()
    return (xyz_d_ffill,)


@app.cell
def _(xyz_monthly):
    xyz_d_bfill = xyz_monthly.resample('D','bfill')
    xyz_d_bfill.plot()
    return (xyz_d_bfill,)


@app.cell
def _(xyz_d_ffill):
    xyz_q_sum_resample = xyz_d_ffill.resample('QS','sum')
    xyz_q_sum_resample.plot()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Group by
    --------
    """)
    return


@app.cell
def _(xyz_monthly):
    xyz_y_mean = xyz_monthly.group_by('q', 'mean')
    xyz_y_mean.pd
    return


@app.cell
def _(xyz_d_bfill):
    xyz_q_sum_groupby = xyz_d_bfill.groupby('Q','sum')
    xyz_q_sum_groupby.pd
    return


@app.cell
def _(xyz_d_bfill):
    xyz_d_bfill.pd
    return


@app.cell
def _(xyz_d_bfill):
    xyz_d_bfill.group_by('Q','mean', tz='UTC').pd
    return


@app.cell
def _():
    return


@app.cell(hide_code=True)
def _(xyz_d_bfill):
    xyz_d_bfill.groupby('Q','mean').pd
    return


@app.cell
def _():
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Moving average
    --------------
    """)
    return


@app.cell
def _(quarterly):
    rolling_4q_avg = quarterly.moving_average(-2,-1)
    return (rolling_4q_avg,)


@app.cell
def _(rolling_4q_avg):
    rolling_4q_avg.pd
    return


@app.cell
def _():
    # Observe BUG: valid_at as period_index converted to number
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(f"""

    """)
    return


@app.cell(disabled=True, hide_code=True)
def _():
    # @supress
    def test_true():
        assert True

    # add better tests in this cell and add to list in cell below!
    return (test_true,)


@app.cell(hide_code=True)
def _(test_true, testing):
    testing.run_and_report([test_true])
    return


if __name__ == "__main__":
    app.run()
