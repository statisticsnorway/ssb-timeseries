import marimo

__generated_with = "0.24.2"
app = marimo.App(width="medium", auto_download=["html"])


@app.cell(hide_code=True)
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _():
    from pathlib import Path
    import polars as pl

    csv_file = Path("~")/"Downloads"/ "10211_20260915-010226.csv"
    return csv_file, pl


@app.cell
def _(csv_file, pl):
    df = pl.read_csv(csv_file,  separator=";",  skip_rows=1 )
    return (df,)


@app.cell
def _(df):
    df.select(df.columns[0:6]).slice(0, 5)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    We check the dimensionality of the data:
    """)
    return


@app.cell
def _(df):
    contents = df['contents'].unique().to_list()
    years = df.columns[3:]
    sexes = df['sex'].unique().to_list()
    age = df['age'].unique().sort().to_list()
    return age, contents, sexes, years


@app.cell(hide_code=True)
def _(age, contents, sexes, years):
    print("Contents:" , *contents)
    print("Years:", *years[0:2], '...', *years[-2:])
    print("Sexes:", *sexes)
    print(f"{len(age)} age groups:", *age[0:5], '...', *age[-2:])
    return


@app.cell
def _():
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    We note that "Persons" apply to the entire table.
    It is a critical to understanding what the data is about, but does not differentitate within the dataset.

    The data table is in a crosstab data format with "attributes" `sex`, `age` and `context` as row identifiers and one column per year from 1846 to 2026

    To rearrange into a long format:
    """)
    return


@app.cell
def _(df, pl, years):
    long = df.drop('contents').unpivot(
        index=~pl.selectors.contains(years),
        variable_name='year',
        value_name ='value'
    ).sort(by=['sex','age','year']) # the sort makes the timeseries nature of the data clearer
    long
    return (long,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    We now have a timeseries representation.
    Two important details:
    - The series are identified by unique combinations of the dimensional columns `sex` and `age`. We will shorten age and combine them into a single unique identifier.
    - The string `year` must be replaced by a proper time specification.
    We need to convert it to either `valid_at` for points in time, or pairs of `valid_from` and `valid_to` for periods.
    """)
    return


@app.cell
def _(long, pl):
    long_renamed = long.with_columns(
        pl.concat_str([
            pl.col('sex'), #.str.replace(r"^([F|M]{1})(.*)","$1"),
            pl.col('age').str.replace(r"(.*)( year.*)","$1")]
            ,
            separator="_",
        ).alias("name"),
        pl.col('year').str.strptime(pl.Date, '%Y').alias("valid_at"),
        ).select(['name', 'valid_at','value']) #.drop('sex', 'age', 'year')
    long_renamed
    return (long_renamed,)


@app.cell
def _(long_renamed):
    wide = long_renamed.pivot(on='name', values='value')
    wide
    return (wide,)


@app.cell
def _(wide):
    from ssb_timeseries.dataset import Dataset
    from ssb_timeseries.types import SeriesType

    pop = Dataset(
        name="Norwegian population by age and sex",
        data_type = SeriesType('NONE','AT'),
        data = wide,
        dataset_tags={'contents':'Persons'}, # better: 'statistical entity'? --> units?
        attributes=['sex','age'],
        substitutions= [('Males', 'M'), ('Females', 'F')],
    )
    return (pop,)


@app.cell
def _():
    from ssb_timeseries import get_configuration
    cfg = get_configuration()
    print(cfg.__dict__)
    return


@app.cell
def _(pop):
    pop.tags
    return


@app.cell
def _():
    from ssb_timeseries.meta import Taxonomy

    sex = Taxonomy(data=[
        {"code": "Males", "parentCode": "0"},
        {"code": "Females", "parentCode": "0"},
    ] )
    sex.print_tree()
    ag = []
    for aa in range(10):
        ag.append ({"code": f"{aa:02}", "parentCode": "0"})
    for aaa in range(107):
        a=f"{aaa:03}"
        ag.append({"code": a, "parentCode": a[0:2]})
    age_groups = Taxonomy(data=ag)
    age_groups.print_tree()
    return (Taxonomy,)


@app.cell
def _():
    return


@app.cell
def _():
    return


@app.cell
def _(Taxonomy):
    Taxonomy(data=[
        {"code": "0-10", "parentCode": "0"},
        {"code": "11-20", "parentCode": "0"},
    ] )
    return


@app.cell
def _():
    return


@app.cell
def _(pop):
    m = pop.select(tags={'sex': 'Females'})
    return


if __name__ == "__main__":
    app.run()
