# ADR-0005: Metadata is indexed per repository and presented as one catalog

Status: Accepted

## Context

A repository has to be searchable without opening the data files.
The descriptions of its datasets live in the data files, so answering a query by reading them means traversing the repository.
A configuration may hold several repositories, and a query should not have to know which one holds an answer.
A dataset that is moved or shared has to keep its description.
Statistics production registers its data in a central catalog, but that catalog is external to the library and separate from its design; the requirement this record answers is only that a repository be searchable without reading its data files.

## Decision

Each repository has a catalog that indexes the descriptions of its datasets; search reads the catalog rather than the data files.
The catalog is configured per repository, with its own storage and location, separate from the data.
The `Catalog` object traverses the catalogs of all configured repositories and aggregates them into a virtual catalog for the configuration, at query time.

## Consequences

- A search costs a catalog read, not a read of every dataset.
- A search over the configuration is global without any repository's catalog being physically central.
- Repositories that configure the same metadata handler and location share one catalog store.
- The catalog is a copy, so it can disagree with the data; it is the source for search.
- The catalog grows with the datasets it indexes, and a metadata format change has to migrate it.
- The description that travels with a dataset is the subject of ADR-0006.
