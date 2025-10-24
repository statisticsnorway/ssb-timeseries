# Architecture

This document provides a high-level overview of the `ssb-timeseries` library's internal architecture and its core design principles.

## Core User Interfaces

A user primarily interacts with the library through two main entry points, which are designed to separate the acts of *working with* data from *finding* data.

-   The **`Dataset` class**: This is the central point of interaction. It represents a single time series dataset, bringing together its data and metadata. It provides a rich API for I/O, calculations, and data manipulation.
-   The **`Catalog` module**: This provides functions for discovering and searching for datasets across all configured storage locations (repositories).

## Key Helper Modules

While `Dataset` and `Catalog` are the main interfaces, several helper modules provide the foundation for the library's flexibility and robustness.

-   **`config`**: This module is the library's central nervous system. It loads all environment-specific settings from a JSON file, defining where data is stored and which backend handlers to use. This decouples the library's logic from hardcoded paths and storage implementations.
-   **`meta`**: This module manages structured metadata. Beyond simple key-value tags, it handles taxonomies (like those from SSB's KLASS) and provides the logic for advanced, metadata-driven operations like hierarchical aggregation.
-   **`io`**: This module acts as the single gateway for all storage operations. As a strict **facade**, it ensures that all parts of the library read and write data through a consistent, high-level API.
-   **`dates`**: This module provides utility functions for standardizing all time-related operations, ensuring consistent handling of timezones, frequencies, and formats throughout the library.

## Data Handling: The Interoperable Data Model

The library's approach to data handling is guided by a core conceptual model that directly influences its choice of technologies and commitment to interoperability.

### The Concept: Datasets as Matrices

A key design feature is the interpretation of datasets as mathematical matrices of series vectors, all aligned by a common date axis.
The library aims to provide easy and intuitive use of linear algebra for calculating derived data.

To accomplish this, the basic data structure is a table where each time series is a column vector, and the `Dataset` object itself exposes a rich set of mathematical operations (e.g., `+`, `-`, `*`, `/`).
This allows for natural, expressive code, such as `new_dataset = (dataset_a + dataset_b) / 2`.

### The Implementation: An Opinionated, High-Performance Stack

This conceptual model naturally leads to an opinionated selection of high-performance, column-oriented technologies:

-   **[Apache Parquet](https://parquet.apache.org/)** is the standard for permanent storage. Its columnar format is highly efficient for the analytical queries typical in time series analysis.
-   **[Apache Arrow](https://arrow.apache.org/)** is the preferred format for in-memory data. Its columnar layout and zero-copy read capabilities ensure high performance and seamless data sharing between processes.
-   **[NumPy](https://numpy.org/)** serves as the powerful and reliable engine for all linear algebra calculations. When you perform a mathematical operation on a `Dataset`, the numeric data is typically converted to NumPy arrays to execute the computation.

### The Principle: Openness and Abstraction

While the core stack is opinionated, a primary goal is to avoid creating a "walled garden." The library is designed to be a good citizen in the PyData ecosystem.

This is achieved through **[Narwhals](https://narwhals-dev.github.io/narwhals/)**, a lightweight abstraction layer that provides a unified API over multiple dataframe backends. This means the library's internal logic works seamlessly whether the in-memory data is a Pandas DataFrame, a Polars DataFrame, or a PyArrow Table, offering maximum flexibility to users.

### A Commitment to Interoperability

To guarantee that the `Dataset` object can be used by other libraries, it adheres to several standard protocols:

-   **The NumPy `__array__` Protocol**: A `Dataset` can be passed directly to most NumPy functions (e.g., `np.mean(my_dataset)`), as it knows how to convert itself into a NumPy array.
-   **The DataFrame Interchange Protocol (`__dataframe__`)**: This allows a `Dataset` to be converted into other dataframe types (like Pandas or Polars) with minimal overhead.
-   **The Arrow C Data Interface (`__arrow_c_stream__`)**: This enables efficient, zero-copy data sharing with other Arrow-native libraries and even other programming languages like R or Julia.
-   **Standard Python Operators**: By overloading operators like `__add__` and `__mul__`, the `Dataset` object can be used directly in mathematical expressions. The motive is to provide a natural and highly expressive syntax, allowing users to write code like `new_dataset = (dataset_a + dataset_b) / 2`.

## Metadata Handling: From Concept to Implementation

The library's approach to metadata is central to its design. It begins with a conceptual model and is realized through a specific technical implementation.

### The Concept: Rich, Structured Descriptions

At its core, every `Dataset` and `Series` is described by a collection of attributes, or "tags".
Rather than being simple key-value pairs, these attributes are designed to take their values from well-defined taxonomies (such as those from SSB's KLASS).
This ensures that metadata is structured, consistent, and meaningful.

This conceptual model is detailed further in the {doc}`info-model`.

### The Implementation: A Dual Storage Approach

The technical implementation is designed to satisfy two core requirements: data portability and centralized discoverability. This is achieved with a dual storage approach:

1.  **Embedded for Portability**: All descriptive tags are embedded directly into the header of the Parquet file. This ensures that the data and metadata are always connected, making each file a self-contained artifact that can be moved or shared without losing its context.
2.  **Indexed for Discoverability**: To fulfill the requirement for a central data catalog, the metadata is also duplicated into an indexed **JSON catalog**. This provides the crucial performance benefit of enabling fast, efficient searches across all datasets in a repository without needing to read the large data files themselves.

## The Decoupled I/O System

The library's ability to adapt to different storage environments is based on a decoupled I/O system that follows a classic **Facade** and **Strategy** design pattern.

-   **The Facade (`ssb_timeseries.io`)**: As mentioned, this module is the single entry point for all storage operations. It presents a simple API (e.g., `read_data`, `save`) to the rest of the application.
-   **Pluggable Handlers (The Strategy)**: The facade reads the project's configuration to dynamically load the appropriate **I/O handler** for a given task. These handlers are the concrete "strategies" for different backends (e.g., local files, cloud buckets) and are defined in a single JSON file, as detailed in the {doc}`configure-io` guide. This design allows for specifying custom handlers from outside the core library.
-   **The Contract (`protocols.py`)**: The methods required for any I/O handler are formally defined in `protocols.py` using `typing.Protocol`, ensuring that any custom handler is compatible with the library's I/O system.
