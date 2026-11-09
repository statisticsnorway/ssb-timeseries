# ADR-0006: The Parquet file is self-describing

Status: Accepted

## Context

A non-versioned dataset is one file that holds all its data; a versioned dataset is one file per version.
A data file that is moved or shared should still be describable without the repository that holds it.
A description kept only in a catalog (ADR-0005) does not travel with the file.

## Decision

The metadata is written into the Parquet file as well: the dataset tags in the schema metadata and each series' tags in the metadata of its column.
The schema is derived from the `SeriesType`, so the file carries the structure it was written under as well as its description.
Data that does not fit its type is not written.

## Consequences

- A data file is self-sufficient: it can be moved or shared without the repository or catalog that describes it.
- The description exists in two places, in the file and in the repository's catalog of ADR-0005, and the two copies can disagree.
