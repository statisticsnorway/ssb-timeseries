# ADR-0002: SeriesType = Versioning × Temporality

Status: Accepted

## Context

Time series are essentially numbers associated with, and ordered by, time.
Different time series relates to time in different ways.
Values may occur at different fixed or variable frequencies.
Some values refer to a point in time (e.g. a stock at a given date), others span a period (e.g. a flow over from/to).
Some series may be unversioned authoritative truths, others are loose estimates with multiple updates, so keeping track of versions becomes key.

The precise nature of the relationship to time can form a series typology.
Not all the possible type descriptors are equally important.
Some characteristics are just descriptive.
Others have direct impact on the implementation of storage models or specific functionality.

## Decision

We base the *type system* on the differences that have direct implications for storage layout, querying, and valid operations.
Other properties can be expressed in descriptive metadata.

Define `SeriesType` as the combination of `Versioning` (e.g. NONE, AS_OF, NAMES) and `Temporality` (e.g. AT, FROM_TO).
All series in a `Dataset` must share the same `SeriesType`.
The type determines the temporal part of the structure used for persistence and the semantics of versioning.
The value type of the series is not declared up front; it is decided from the data when the file is written, and it may have to be declared up front later.

## Consequences

- Enforces type consistency across datasets.
- Versioning determines the layout: a non-versioned dataset is one file, a versioned dataset is one file per version, and the type is encoded in the path.
- Makes versioning semantics explicit and prevents mixing incompatible versions/timings.
- Harder to evolve types once persisted data exists.
- Named versioning was acknowledged as a valid variant from the start, but never implemented end to end, so it is reserved in the type system and asking for it is an error.
- All series in a dataset share one value type, decided from the data when the file is written.
- Descriptions that do not impact technical implementation should be represented as metadata.
