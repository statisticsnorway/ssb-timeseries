# ADR-0000: Origin and intent

Status: Accepted

## Context

As Statistics Norway is moving from an on-prem platform to the cloud.
In this process closed source legacy solutions based on SAS, Oracle and the time series database FAME are being replaced by modern open source solutions.
The platform is designed to protect sensitive data and ensure reproducibility, traceability (lineage/versioning) by adhering strictly to a formal process model.

The analysis of user requirements for time series handling let to a PoC with a Python package to standardize storage and information models and integrate with metadata systems and production platform designs.
The evaluation concluded that 1) a common solution is required, but 2) an approach with a code library rather than a system with its own infrastructure is likely sufficient: [ADR 0031: Fellesløsning for håndtering av tidsserier](https://adr.ssb.no/0031-fellesloesning-for-haandtering-av-tidsserier/).

## Decision

Statistics Norway's ADR 0031 points to `ssb-timeseries` as the chosen approach to provide a shared, flexible, open-source Python library for time series handling.
The library is designed as a code library rather than a system with its own infrastructure, and aims to link key concepts and features of the platform and process model with state of the art libraries in the Python ecosystem.

## Consequences

- The code library `ssb-timeseries` implements the direction set by ADR 0031.
- Architectural decisions within the library (e.g. dataset-centric design, type system, pluggable I/O) are recorded in subsequent library-level ADRs.
- The package does not need to manage access to data and system resources. That is dealt with at a project level.
- Decisions on whether a larger system complete with storage and service layers are deferred until later.
