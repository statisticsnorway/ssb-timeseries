# ADR-0002: SeriesType = Versioning × Temporality (+ value type)

Status: Accepted

## Context

Different time series require different notions of "time" and "version". Some are valid at a point in time (e.g. stock/flow values with a valid-at timestamp), others span intervals (from/to). Some are unversioned, some are versioned as-of a time point, and others use named versions. These differences have direct implications for storage layout, querying, and valid operations.

## Decision

Define `SeriesType` as the combination of `Versioning` (e.g. NONE, AS_OF, NAMED) and `Temporality` (e.g. AT, FROM_TO), plus value type. All series in a `Dataset` must share the same `SeriesType`. The type determines the schema/structure used for persistence and the semantics of versioning.

## Consequences

- Enforces type consistency across datasets.
- Allows different physical representations optimized for each type (e.g. as-of encoded in filename/partition).
- Makes versioning semantics explicit and prevents mixing incompatible versions/timings.
- Harder to evolve types once persisted data exists.
