# Core Concepts

The time series library relies on an *information model*, where a set of core concepts get very specific technical meanings.

## Primary building blocks

The most basic concept is that of a **time series**.
In the simplest sense, the term simply denotes numbers associated with (and ordered by) time.
Time can, however, be related to the numeric values in multiple ways.
Some of these ways have technical implications and constitute **types** of series.

Time series **datasets** are collections of time series.
They live in **repositories** (file system, object storage, database).
Multiple repositories can be configured to form a **catalog**.

* The name of a repository must be unique within the configuration.
* Every dataset must have a name that is unique within the repository.
* Every time series must have a name that is unique within the dataset.

Within a configuration context, the concatenation of `repository.set_name.series_name` becomes globally unique.

## Descriptive concepts and entities

Within the time series libraries, datasets and series are the core *object types*.
They should be described completely, precisely and comprehensively for the purposes of 1) uniquely identified, 2) understood by humans and 3) interpreted by machines.

Recent advances in AI have vastly increased the capacity of machines to handle unstructured textual descriptions,
but machine readability still benefits greatly from formal metadata systems.
Here "descriptions" primarily refer to any relevant set of "tags".
The tags are essentially just key-value pairs associated with objects.
The attribute structure is very flexible and allows free text attributes and values, but can benefit from attaching to more formal models.

In a complete system, precisely defined **attributes** and **taxonomies** that define their value domains ensure consistency.
If taxonomies not only provide controlled vocabularies for attribute values, but also add or map structure and relations, the totality of entity types and concepts and the relations between them form a knowledge graph, or concept model.

The time series library does not itself define such a model, but consumes externally defined taxonomies, provides features for tag maintenance.
The library enforces few constraints on attributes and taxonomy usage.
That said, there is some common denominator and inheritance logic for datasets and series.
Some mandatory attributes are enforced, mainly related to the type system.
We also plan features related to time and units of measure, and potentially other attributes that lend themselves to transformations or calculations in such a way that special treatment is warranted.

On the shadow side of that:
While **taxonomies** can be consumed from APIs or files, the tag maintenance features currently do not validate or enforce consistency of tag values.
A stricter option where attributes are defined and associated with taxonomies is under consideration.


The first requirement for completeness is uniqueness:
If two series are the same, they should have the same data, and the same full descriptions.
If there is a difference, it should be explainable by a difference in at least one formal attribute.
The other direction also holds:
If two sets or series have the same (complete) description, they share identity.
Then the data should be consistent in such a way that it is possible to merge the data without conflict (at least with sufficient knowledge about the time of updates).



## Dataset scope

Loosely defined, any collection of series can form a dataset.
We narrow this down in several ways:

A dataset should group series that share a common descriptive denominator.
The flip side of that is that there should be a limited number of key attributes differentiating between series in the set.
Precisely what that means can not be defined up front.
A great indicator is how easy it is to name the set and the series.

A dataset should group series that originate in a similar way based on similar inputs.
For optimal workflow automation all series in a set should originate from the same technical process at (roughly) the same time.
In cases where a process calculates multiple outputs in steps or iterations of the same run, it is largely a contextual assessment or implementation choice whether to do intermediate writes to multiple sets or to accumulate into a single set before writing.
If on the other hand the calculation is spread out over multiple runs, that is a strong (but not definitive) indication that there should be multiple datasets.


## Sets, series and types

Data types imply mandatory date columns shared by all series in the dataset.
Series are represented as columns.
The column structure implied by type is enforced by Parquet schemas.
Failing to obey them will cause writing and potentially other methods to fail.

Both the datasets and the series in the set can be *tagged* (ie. associated with any number of key-value pairs).
While the related features can benefit greatly from using controlled vocabularies or structured taxonomies, and some integrations with Statistics Norway meta data are built in, this is not a strict requirement.

**Types** are defined by
Versioning
: How updated versions of the truth are represented:
    NONE
    : Maintains a single version and overwrites on changed data,
    NAMED
    : (Planned) Maintains new "logical" versions identified by name.
    AS_OF
    : Maintains new "logical" versions identified by date. Writing twice *for the same as_of date* will overwrite any changed data.
Temporality
: Defines the time axis for the data points (ie. the "real world" *point in time* the data applies *at* or the *period* it applies *from and to*). It will translate into one or more columns with datetime or period indexes of Data.data.
Value type
: Only scalar numbers in a strict interpretation of *time series*. However, numbers may be differentiated by technical types (float, integer, decimal, ...). To be decided later; for now the level of detail stops at "numeric".
Some calculations lead naturally into booleans, although they are not supported for all operations without casting to integers.
Extensions into non-numeric and non-scalar types are conceivable, and have some compelling use cases.
Like numeric sub-typing this is defined out of scope for now.

Note that *versioning* is closely related to *immutability*, but not quite the same.
Versioning is semi-immutable in the sense that it allows writing new versions without ever changing anything, but for any version multiple writes may overwrite date.
The logical versions are *transparent*, meaning they can be easily compared at any time.
True immutability is a matter of IO implementation, even for `versioning = NONE`.
Immutability does not imply transparent versions, though, and transparency comes at a cost of heavier queries.
Many immutable implementations make it quite painful to compare versions.

The transparent, but only semi-immutable versioning is supplemented by the `Dataset.persist()` (snapshot) functionality.
This allows archiving data in immutable, stable stages that directly align with Statistics Norway's "inverted GSBPM" model (as outlined in the [2023 modernization strategy](https://unece.org/sites/default/files/2023-06/CES%202023%2021_Item%203.pdf)).
By saving strict, point-in-time snapshots to dedicated buckets, the library enforces the strict progression of data through the five official steady states (Source, Input, Processed, Statistics, and Output).
While daily work and transparent versioning happen dynamically in the active repositories, snapshots provide the absolute idempotency and traceability legally mandated for official statistics.

- **Datasets** can consist of multiple series. (Later: possible extension with sets of sets.)
- All series in a set must be of the same type.
- **Series** are value columns in Datasets.data, rows identified by date(s) or index corresponding temporality.
- The combination `<Dataset.name>.<Series.name>` serves as a unique series identifier within a repository.
- In simple IO implementations, `<Dataset.name>` identifies a "directory", hence must be unique. (Caveat: Directories per type creates room for error.)
- `<Series.name>` (.data column name) must be unique within the set. It depends on IO implementations and coding patterns whether it can be safely changed. The general recommendation is to match on metadata tags rather than names.
- Series names _should_ be related to (preferably constructed from) codes or meta data in such a way that they can be mapped to "tags" via a format mask (and if needed a translation table).


(A future/planned feature is likely to also allow sets to consist of other sets, as this can greatly enhance advanced [workflows](./workflow.md#automation).)

## Data frame, matrix and vector representations

The dataset class implementation requires all series in the set to be of the same type.

To benefit from the power of linear algebra, we *want* to consider datasets as matrices of time series vectors. In dataframe terminology, series are numeric value columns that with shared date columns.

It is helpful if null values (NaNs) are relatively rare. The dataset matrix / dataframe should not to sparse, ie. if there is a value for all series for all (or most) dates.
