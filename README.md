# Time series

## Background

Statistics Norway is building a new procuction system in the cloud.

In a move towards modern architecture, development methodology and open source technologies: 
 * Python is replacing SAS for production 
 * Oracle databases and ODI for ETL are largely to be replaced with Python code and Parquet files a data lake architecture.

While databases are not completely banned, but as we shift responsibilities for maintaing larger the tech stack closer to the teams that own the statistical processesthorough consideration is required to apply them. The time series solution FAME needs a replacement. 

Basic read/write functionality, calculations, time aggregation and plotting was demonstrated Friday December 8. See `src/demo.ipynb` for demo content and `tests/test_*.py` for more examples of what works and in some cases what does not.

Note that
 * linux spesific filepaths have been rewritten, *but not tested in a pure Windows environment*.
 * the solution relies on env vars  TIMESERIES_ROOT and LOG_LOCATION being set. They *must* be set if `/home/jovyan/sample-data` is not reachable, but *should* be set anyway.
* with env variables set and the code on python path `poetry run pytest` should succeed. If not, let me know that I ****** something up. ;) 

## How to get started?

The library is in a workable state and should work both locally (it was developed in VS Code on Windows using WSL) and in Jupyter. It is still in an exploratory phase, so there is a risk that fundamental choices are reversed and breaking changes introduced. 

With that disclaimer, feel free to explore and experiment, and do not be shy about asking questions or giving feedback. At this stage, feedback is all important.

Proper library packaging is planned for a later development stage. In the meantime, you will have to clone/pull directly from the GitHub repository.

Assuming you have Python working with a standard SSB setup for git and poetry etc, the following should get you going:

``` bash
# Get the poc package
git clone https://github.com/statisticsnorway/arkitektur-poc-tidsserier.git

# Run inside a poetry controlled venv:
poetry shell

# Create and set a location for data and log files.
# This could be anywhere, but separated from the code is preferrable.
mkdir series
export TIMESERIES_ROOT=${PWD}/series
export LOG_LOCATION=${PWD}/series

# Run the tests to check that everything is OK: 
poetry run pytest

# A couple of the test cases *will* fail when running for the first time.  
# They will create the structures they need and should succeed in subsequent runs.

```


## Functionality overview

The core of the library is the Dataset class. This is essentially a wrapper around a DataFrame (for now Pandas, later probably Polars) in the .data attribute. 

The .data attribute should comply to conventions implied by the underlying *information model*. These will start out as pure conventions and subject to evalutation. At a later stage they are likely to be enforced by Parquet schemas. Failing to obey them will cause some methods to fail. 

The Dataset.io attribute connects the dataset to a helper class that takes care of reading and writing data. This structure abstracts away the IO mechanics, so that the user do not need to know about the "physical" details, only the *information model meaning* of the choices made.

 * Read and write for both versioned and unversioned data types.
 * Search for sets by name, regex and (planned for later) metadata.
 * Basic filtering of sets (selecting series within a selected set).
 * Basic linear algebra: Datasets can be added, subtracted, multiplied and divided with each other and dataframes, matrices, vectors (untested) and scalars according to normal rules.  
 * Basic plotting: Dataset.plot() as shorthand for Dataset.data.plot(<and sensible defaults>).
 * Basic time aggregation: 
 `Dataset.groupby(<frequency>, 'sum'|'mean'|'auto')`
 * 


 ## The information model

 ### TLDR

 * **Types** are defined by
  * **Versioning** defines how updated versions of the truth are represented: NONE overwrites a single version, NAMED or AS_OF maintaines new "logical" versions identified by name or date.
  * **Temporality** describes the "real world" valid_at or valid_from - valid_to datetime of the data. It will translate into columns, datetime or period indexes of Dataset.data.
  * Value type (only scalars for now) of Dataset.data "cells".
* **Datasets** can consists of multiple series. (Later: possible extension with sets of sets.)
* All series in a set must be of the same type. 
* **Series** are value columns in Datasets.data, rows identified by date(s) or index corresponding temporality.
* The combination `<Dataset.name>.<Series.name>` will serve as a globally unique series identifier.
* `<Dataset.name>` identifies a "directory", hence must be unique. (Caveat: Directories per type creates room for error.)
* `<Series.name>` (.data column name) must be unique within the set. 
* Series names *should* be related to (preferrably constructed from) codes or meta data in such a way that they can be mapped to "tags" via a format mask (and if needed a translation table). 

Yes, that *was* the short version. The long version is still pending production.

To be continued ...

### How to contribute

More information about this will come later, but contributions are welcome. If you want to contribute, just let us know. 

### Other sources of documentation:

* https://statistics-norway.atlassian.net/wiki/spaces/Arkitektur/pages/3581313026/Statistikkproduksjon
* https://statistics-norway.atlassian.net/wiki/spaces/Arkitektur/pages/3595665419/Lagring+av+tidsserier