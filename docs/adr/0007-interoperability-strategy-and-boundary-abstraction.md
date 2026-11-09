# ADR-0007: Interoperability strategy and boundary abstraction

Status: Accepted

## Context

ADR-0001 treats a dataset as a matrix of equally typed series and defines the algebra over it, but leaves open how that matrix is represented in memory.
That leaves the library to position itself among the dataframe, table and array implementations: which it accepts, which it returns, and how it hands data to the ones it does not speak directly.
The PoC spoke Pandas for data representation, I/O and core operations, with NumPy for linear algebra, so Pandas semantics — the index, resampling, `numeric_only` — leaked into what the API returned, and the choice of implementation became part of the contract.
Polars, DuckDB and PyArrow offer different capabilities, notably in performance and lazy execution, and Ibis and Narwhals address interoperability across these and other ecosystems.
The current I/O implementations are eager, so lazy input is collected at the boundary.

## Decision

The library stays independent of dataframe ecosystems: it does not make one of them the price of entry.
The library adopts Narwhals at the API boundary to accept any compatible frame as inputs, and return results in the same backend where practical.
Internals can in principle use any dataframe or array backend as long as the choice does not become part of the public API contract.
The Narwhals API should be preferred and direct backend dependencies added only if strictly necessary.

Interoperability is not limited to dataframe libraries: the Arrow C data interface, the dataframe interchange protocol and the NumPy array protocol are implemented, so that other code, including other languages such as R, can consume a dataset directly.
The library supports Arrow-based interoperability, in memory and between processes.
Arrow is the interchange with storage (ADR-0003) and the persisted Parquet schema is the Arrow schema (ADR-0006), but Arrow is not a canonical internal representation; where Narwhals conversion suffices, it is preferred.

## Consequences

- Users can work with Pandas, Polars, Arrow and other supported Narwhals-compatible backends.
- Backend choice remains with the user, within documented support.
- A dataset can be handed to Arrow-native code, to other languages, and to NumPy-based code without the user converting it first.
- Backend-specific capabilities and semantic differences must be considered, and behaviour previously inherited implicitly from Pandas must be explicitly implemented, documented or deliberately left backend-dependent.
- Narwhals is a common denominator, so an operation it cannot express has to drop to one backend, which is where backend-specific behaviour leaks and where a result can change backend.
- Narwhals becomes an important architectural dependency, and its API is a moving target.
- The design allows adding support for lazy execution later at the cost of new I/O handlers and a rewrite and QA for operations.
- The tests must cover the supported backends.
