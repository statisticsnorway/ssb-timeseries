# Information model

## The core concepts

Conceptually, time series are simply numbers associated with (and ordered by) time.
Time series datasets are collections of time series.

It makes sense to require series and sets to have names.
Names should be unique. Datasets names globally unique, series names within the set, so that the concatenation of `set_name.series_name` becomes globally unique.

It also makes sense to require series and sets to have descriptions.
Descriptions should be as complete as possible.

Sets makes more sense if there is some commonality in content, ie if there is a common denominator to descriptions.
That also makes naming the set a lot easier.

Workflows becomes much tidier and a lot easier to set up if we add the requirement that all series orginates from the same technical process at roughly the same time.

Descriptions should be structured and machine comparable:
If two sets or series have the same (complete) description, they are the same.
Then it is most likely also possible to merge the data without conflict with some knowledge about the time of updates.


## Sets, series and types

Data types implies mandatory date columns shared by all series in the dataset. Series are represented as columns.
 enforced by Parquet schemas. Failing to obey them will cause some methods to fail.

Both the datasets and the series in the set can be *tagged*ie associated with any number of key-value pairs. While the related features can benefit greatly from using controlled vocabularies or structured taxonomies, and some integrations with Statistics Norway meta data are built in, this is not a strict requirement.

**Types** are defined by
Versioning
: How updated versions of the truth are represented:
    NONE
    : overwrites a single version,
    NAMED
    : lacks implement
    AS_OF
    : maintains new "logical" versions identified by name or date.
Temporality
: The "real world" valid_at or valid_from - valid_to datetime of the data. It will translate into columns, datetime or period indexes of Dataset.data.
- Value type (only scalars for now) of Dataset.data "cells".
- **Datasets** can consists of multiple series. (Later: possible extension with sets of sets.)
- All series in a set must be of the same type.
- **Series** are value columns in Datasets.data, rows identified by date(s) or index corresponding temporality.
- The combination `<Dataset.name>.<Series.name>` will serve as a globally unique series identifier.
- `<Dataset.name>` identifies a "directory", hence must be unique. (Caveat: Directories per type creates room for error.)
- `<Series.name>` (.data column name) must be unique within the set.
- Series names _should_ be related to (preferrably constructed from) codes or meta data in such a way that they can be mapped to "tags" via a format mask (and if needed a translation table).


(A future/planned feature is likely to also allow sets to consist of other sets, as this can greatly enhance advanced [workflows](./workflow.md#automation).)

## Data frame, matrix and vector representations

The dataset class implementation requires all series in the set to be of the same type.

To benefit from the power of linear algebra, we *want* to consider datasets as matrices of time series vectors. In dataframe terminology, series are numeric value columns that with shared date columns.

It is helpful if null values (NaNs) are relatively rare. The dataset matrix / dataframe should not to sparse, ie if there is a value for all series for all (or most) dates.
