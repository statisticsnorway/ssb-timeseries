# Overview

```{contents} Table of Contents
:depth: 2
```

## Modules

The main interfaces is located in the two core modules:

 * The **Catalog** module, that provides *search* across one or more *repositories* and exposes *descriptions* of sets and series. 
 * The **Dataset** module is built around the  `Dataset` class. It brings data and metadata together and adds functionality, notably:
   * Finding, reading and writing; with [workflow](workflow.md) integrations
   * Calculations
   * Visualisations

```{py:class} dataset.Dataset
:nocontentsentry:
Docstring content
```

The core is described extensively in [reference](reference.md). Here we try to explain and bring some context to the overall design. 

Datasets and time series are key concepts of the underlying [information model](info-model.md). 

Some parts of the functionality have been implemented in a number of helper modules. This helps keeping the core modules as simple as possible. 

Notable examples are: 

 * The **Configuration** module for initial set up and later switching between repositories, if needed.
 * **Date** string standardisation and time zone handling in the dates.py module
 * **Type definitions**, in properties
 * The **meta** module puts KLASS code lists into proper tree structures, and deals with tagging implemenation details.
 * An **IO** abstraction allows the core to be agnostic of whether data and metada resides in files or databases.
 * The main purpose of **fs.py** is to make IO agnostic of local vs GCS file systems.

See the [reference] for detail on each module.


## Design choices

### Storage: IO and FS modules

The `io module` connects the dataset to helpers that take care of reading and writing data. This structure abstracts away the IO mechanics, so that the user does not need to know the implementation details, only the _information model meaning_ of the choices made when creating the datasets. 

Also, although the current implementation (as of version 0.2.6) relies mainly on pyarrow and parquet, it is not obvious that this will remain the case forever. The implementation of some features would in fact be simpler within a database. 

By replacing or updating the io-module, a database *could* be used instead.

In a similar fashion, the `fs` module is created so that the library should work for Statistics Norway both in the cloud and on prem. 

While the current cloud oriented object storage implementation is very specific to Statistics Norway and our choice of the Google Cloud Platform, it is easy enough to extend or replace if required.

### Data in Arrow and Parquet formats

A key feature is the linear algebra interpretation of datasets as matrices of series vectors aligned by dates. 
Although this does not command any given specific technical implementation, it translates very naturally into a choice of `Arrow tables` for data in memory and Parquet files for permanent storage. These are column oriented formats that work well together and make a good match for analytic data like time series. 

Arrow has clear benefits related to sharing data in memory with "no copy" between processes in different languages. 
Besides Python, both R and Julia play a role in Statistics Norway. 

The dataset is a wrapper around data and metadata, exposed in `Dataset.data` and `Dataset.tags` respecively.
In early PoC versions data was pure `Pandas dataframes`, stored in `parquet` files using a `pyarrow` backend. 

The current our current implementation (version 0.2.6) moving towards a pure Arrow implementation. 
Reading and writing relies on `pyarrow`, but the `.data` attribute still returns a Pandas Dataframe,
and calculations rely on `Pandas` or `Numpy`.
In a future version `.data` can be expected to return an `Arrow table`.

### Metadata

Information model excerpt: Sets are "parents" / can consist of one or more series. Both sets and series are "objects" that can be *tagged*. See the [information model description](info-model.md) for more detail on this. 

At the practical level, both datasets and series tags consists of an *attribute* and a *value* and `Dataset.tags` returns a dictionary with the attributes as keys. A few attributes are special in that they are enforced, notably name and type attributes. Another special attribute is `Dataset.tags.series`, which will return a dictionary with series names as keys and for each series a dictionary with series tags. 

The result is  nested structure holds all the metadata for the dataset and its series. 
There are more than one way to store such a structure, and we have chosen to use more than one:
The very first round stored metadata "tags" in `.json` files next to the `.parquet` files that held the data. 
Later implementations are stricter and apply tags to headers of Arrow and Parquet schemas. That makes the Arrow and Parquet representations self sufficient. 
The JSON files are no longer needed for dealing with individual sets. 
Instead, they have been moved into a central "catalog" directory. That makes it possible to search across all datasets in a repositry without traversing the data directories. 

Of course, this approach is contingent on the choice of file based storage. 

Note that even the "global" catalog spanning multiple time series repositories is time series specific. For now it only hints at future integration with a more canonical data catalog for Statistics Norway.

### Calculations

Consistent storage of both data and metadata simplifies search and all treatment of the data. 
While time series in the simplest sense are just numbers associated with dates, there are many nuances that makes that more complicated. 
The details are described in [information model](info-model.md).

A key feature that the library aims to provide is easy use of *linear algebra* for calculating derived data. 

To accomplish that, the basic data structure is a table or dataframe where each series is represented as a column vector in a dataset matrix, accompanied by one or more shared date columns. 
Also, the dataset object itself exposes a number of math operations on the numeric part of the data:
Datasets can be added, subtracted, multiplied and divided with each other and dataframes, matrices, vectors and scalars according to normal rules of linear algebra.

The linear algebra features typically simply ignores the date columns. Other features take them explicitly into account.
*Time algebra* features use the date columns for up- and downsampling.
This typically create new dataset objects with a different time resolution; 
the new values are calculated over "windows" defined by the data columns. 

Basic time aggregation:
`Dataset.groupby(<frequency>, 'sum'|'mean'|'auto')`

Yet another group of functionality use the descriptive metadata of the series for calculations. These can range from simple operations that work on individual series(like unit conversions) to more complex ones that take into account the relations between entities in the tag values. An example of the latter would be calculating aggregates over groups of series based on the hierarchical structure of a taxonomy.

```python
x = Dataset(...)
tree = Taxonomy(...)

x.aggregate(
  attribute=<series_tag_attribute_to_operate_on>, 
  taxonomy=tree, 
  method=['mean', 'median'])
```