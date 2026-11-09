# ADR-0009: Hive partitioning is the target layout for file-based storage

Status: Accepted

## Context

ADR-0008 leaves the storage layout to the handler.
The first file handler puts the version in the file name, so the versions of a dataset are separate files whose relationship only the library knows.
An external query engine should be able to read a repository across versions as one table.

## Decision

Versioned data is partitioned on disk, with the version as a Hive partition key.
The partition key is `as_of`; unversioned data uses the Hive default partition.
The partitioned layout is a separate handler, so the simple layout remains available and nothing in the library depends on either.
The partitioned layout is the target for file-based storage.

## Consequences

- An external query engine sees one table across all versions, with `as_of` as a column that is null for unversioned data.
- The query view is a property of the layout; the library cannot make a layout queryable that does not partition.
- The partitioned layout is not the default, so the simple layout remains in use.
- Adoption is per repository, because the handler is bound in configuration.
- A dataset written in one layout cannot be read through the other, and changing the handler rewrites the data.
- The new layout needed no change to the handler contract, which is the evidence for ADR-0008.
