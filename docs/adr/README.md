# Architectural Decision Records

The Architecture Decision Records (ADR) documents the key design decisions.
They are on a best effort basis numbered in chronological order and associated with the release that carries it.
The sections below group them by the argument they build.

## Foundations

The intent behind the library, the unit of work it is built around, and the type system that follows from it.

| ID | Title | Released in | Status |
|---|------|-------------|--------|
| [0000](0000-origin-and-intent.md) | Origin and intent | [ADR 0031](https://adr.ssb.no/0031-fellesloesning-for-haandtering-av-tidsserier/) | Accepted |
| [0001](0001-dataset-as-primary-unit.md) | Dataset as the primary unit of analysis and storage | [v0.0.0](https://github.com/statisticsnorway/ssb-timeseries/releases/tag/v0.0.0) | Accepted |
| [0002](0002-seriestype-versioning-temporality.md) | SeriesType = Versioning × Temporality | [v0.0.0](https://github.com/statisticsnorway/ssb-timeseries/releases/tag/v0.0.0) | Accepted |

## Storage, configuration and metadata

How the library reaches storage, how it is configured, and how a dataset is described.

| ID | Title | Released in | Status |
|---|------|-------------|--------|
| [0003](0003-io-agnostic-storage.md) | Storage access is I/O agnostic | [v0.0.0](https://github.com/statisticsnorway/ssb-timeseries/releases/tag/v0.0.0) | Accepted |
| [0004](0004-external-validated-configuration.md) | Configuration is an external, validated file with named presets | [v0.3.2](https://github.com/statisticsnorway/ssb-timeseries/releases/tag/v0.3.2) | Accepted |
| [0005](0005-per-repository-metadata-catalog.md) | Metadata is indexed per repository and presented as one catalog | [v0.2.6](https://github.com/statisticsnorway/ssb-timeseries/releases/tag/v0.2.6) | Accepted |
| [0006](0006-self-describing-parquet-file.md) | The Parquet file is self-describing | [v0.2.6](https://github.com/statisticsnorway/ssb-timeseries/releases/tag/v0.2.6) | Accepted |

## The environment and the stack

The dataframe and storage boundaries, and the layout that proves the storage one.

| ID | Title | Released in | Status |
|---|------|-------------|--------|
| [0007](0007-interoperability-strategy-and-boundary-abstraction.md) | Interoperability strategy and boundary abstraction | v0.6.0 | Accepted |
| [0008](0008-storage-implementation-behind-api.md) | The storage implementation is a technical detail behind the API | v0.7.1 | Accepted |
| [0009](0009-hive-partitioning-target-layout.md) | Hive partitioning is the target layout for file-based storage | v0.7.1 | Accepted |

## The subsystems and the API

The metadata subsystem, the per-series view, and how operations address a dataset.

| ID | Title | Released in | Status |
|---|------|-------------|--------|
| [0010](0010-taxonomy-as-graph.md) | A taxonomy may be structured as a graph, not only a tree. loaded through decoupled loaders | v0.7.4 | Accepted |
| [0011](0011-detached-series-projections.md) | Iterating a Dataset yields detached Series projections | v0.8.1 | Accepted |
| [0012](0012-dataset-ref-and-archive-conventions.md) | I/O operations are addressed by a DatasetRef; archive conventions are data | v0.9.0 | Accepted |
