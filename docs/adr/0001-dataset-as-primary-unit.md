# ADR-0001: Dataset as the primary unit of analysis and storage

Status: Accepted

## Context

The workflows in statistics production are heavily batch-oriented.
Datasets are natural units of work.
A set of series often stems from the same process, shares descriptive metadata context, and should be read and written together for simplicity and consistency.
Operations such as aggregation, arithmetic, and resampling benefit from treating aligned series as vectors.

## Decision

The `Dataset` is the primary unit of analysis and storage.
It contains one or more series of the same `SeriesType` (ADR-0002).
All series in a dataset share a common date/time axis and are represented in memory as columns in a matrix-like structure.
Operations can act on datasets as wholes, enabling efficient processing with linear algebra.

## Consequences

- Enables natural arithmetic between dataset slices as matrices and with scalars/vectors where aligned.
- Simplifies I/O, since a dataset is read and written as a set.
- The `Dataset` is not a random collection of series, but is linked to process origin.
- Series with different type characteristics must not be mixed in the same dataset.
- Provides a clean boundary for metadata commonality and workflow integration.
- The concept of a dataset as a matrix of date aligned and equally typed series invites column oriented in memory representations like Arrow and storage with Parquet or columnar databases.
