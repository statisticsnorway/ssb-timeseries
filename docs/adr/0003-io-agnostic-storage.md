# ADR-0003: Storage access is I/O agnostic

Status: Accepted

## Context

Storage access must be independent of any specific backend.
The initial proof of concept stored metadata as JSON and data as Parquet files.
The target environment is a Linux based production environment on Google Cloud Platform (GCP),
where each production pipeline run inside its own Virtual Private Cloud (VPC).
Data access is managed at a network level before any code can run.

## Decision

The `Dataset` core relies on configuration to define paths and the `io` and `fs` helpers to reach storage through an abstraction,
so the same code runs against a local filesystem and against cloud storage.
Abstractions beyond filesystem operations of `fs` are left for later.
The package is tested to be working for Linux, Mac OS and Windows.

## Consequences

- The library runs unchanged in a local checkout and in the cloud environment.
- A storage backend is added behind the abstraction, not by changing the callers.
- Operations a file interface cannot express are out of reach.
- The filesystem abstraction remains a layer beneath the handlers, and is where the two meet.
- This abstraction enables pluggable I/O handlers contracted by a `typing.Protocol` (ADR-0008).
