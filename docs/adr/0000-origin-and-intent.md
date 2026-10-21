# ADR-0000: Origin and intent

Status: Accepted

## Context

The work on a shared solution for handling time series at Statistics Norway led to [ADR 0031: Fellesløsning for håndtering av tidsserier](https://adr.ssb.no/0031-fellesloesning-for-haandtering-av-tidsserier/).

The full text and background material for that decision are preserved in project notes, including `notes/2025-03-14_0000_adr-0031_fellsløsning-tidsserier.md`. These materials capture the motivations, considered alternatives, and decision drivers that preceded the implementation of the library itself.

## Decision

This ADR records the intent behind the creation of `ssb-timeseries`: to provide a shared, flexible, open-source Python library for time series handling that fits SSB's process and information models, with an emphasis on reproducibility, traceability (lineage/versioning), and integration with SSB's metadata and production platform.

## Consequences

- The library exists as an implementation of the direction set by ADR 0031.
- Architectural decisions within the library (e.g. dataset-centric design, type system, pluggable I/O) build on this intent and are recorded in subsequent library-level ADRs.
