# ADR-0002: SeriesType = Versioning × Temporality

Status: Accepted

## Context

Time series are essentially values associated with, and ordered by, time.
The precise nature of that association give raise to a series typology.
Some of the type characteristics have direct impact on the implementation of storage models or specific functionality, while others are just descriptive.

Different time series require different notions of "time" and "version".
Some are valid at a point in time (e.g. a stock at a given date), others span a period (e.g. a flow over from/to).
Some are unversioned, some are versioned as-of a time point, and others use named versions.

We base the type system on the differences that have direct implications for storage layout, querying, and valid operations.

## Decision

Define `SeriesType` as the combination of `Versioning` (e.g. NONE, AS_OF, NAMES) and `Temporality` (e.g. AT, FROM_TO). All series in a `Dataset` must share the same `SeriesType`. The type determines the temporal part of the structure used for persistence and the semantics of versioning. The value type of the series is not declared up front; it is decided from the data when the file is written, and it may have to be declared up front later.

## Consequences

- Enforces type consistency across datasets.
- Allows different physical representations optimized for each type (e.g. as-of encoded in filename/partition).
- Makes versioning semantics explicit and prevents mixing incompatible versions/timings.
- Harder to evolve types once persisted data exists.
- Named versioning was acknowledged as a valid variant from the start, but never implemented end to end, so it is reserved in the type system and asking for it is an error.
- All series in a dataset share one value type, decided from the data when the file is written.
- Descriptions that do not impact technical implementation should be represented as metadata.
