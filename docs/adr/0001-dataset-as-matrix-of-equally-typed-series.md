# ADR-0001: Dataset as a matrix of equally-typed series

Status: Accepted

## Context

SSB's workflow for time series in statistics production is batch-oriented. A set of series often stems from the same process, shares descriptive metadata context, and should be read and written together for simplicity and consistency. Operations such as aggregation, arithmetic, and resampling benefit from treating aligned series as vectors.

## Decision

A `Dataset` is the unit of analysis and storage. It contains one or more series of the same `SeriesType` (equally typed). All series in a dataset share a common date/time axis and are represented as columns in a matrix-like structure. Operations act on datasets as wholes, enabling linear algebra and batch processing.

## Consequences

- Enables natural arithmetic between datasets and with scalars/vectors where aligned.
- Simplifies I/O (read/write as a set).
- Imposes a constraint: series with different type characteristics must not be mixed in the same dataset.
- Provides a clean boundary for metadata commonality and workflow integration.
