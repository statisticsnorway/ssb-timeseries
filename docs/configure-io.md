# Configure I/O

This guide provides detailed examples for configuring data repositories, metadata catalogs, and snapshot (persistence) behavior in `ssb-timeseries`.

## 1. IO Handlers

The `io_handlers` section of your configuration file defines the backend Python classes that will handle reading and writing data.
You must define a handler for each type of storage interaction you need (e.g., for data, metadata, and snapshots).

### Example: Handler Definitions

This example defines the three standard handlers used by the library.

```json
{
    "io_handlers": {
        "my_data_handler": {
            "handler": "ssb_timeseries.io.pyarrow_simple.FileSystem",
            "options": {}
        },
        "my_metadata_handler": {
            "handler": "ssb_timeseries.io.json_metadata.JsonMetaIO",
            "options": {}
        },
        "my_snapshot_handler": {
            "handler": "ssb_timeseries.io.snapshot.FileSystem",
            "options": {}
        }
    }
}
```

## 2. Handler Directory Structures

The choice of I/O handler determines how your data is organized on disk. Below are examples of the directory structures created by the built-in handlers.

### `pyarrow_simple`

This handler stores each dataset in a single Parquet file, with versioning information encoded in the filename.

```
<repository_root>/
├── AS_OF_AT/
│   └── my_versioned_dataset/
│       ├── my_versioned_dataset-as_of_20230101T120000+0000-data.parquet
│       └── my_versioned_dataset-as_of_20230102T120000+0000-data.parquet
└── NONE_AT/
    └── my_dataset/
        └── my_dataset-latest-data.parquet
```

### `pyarrow_hive`

This handler creates a Hive-partitioned directory structure, which is optimized for query engines like Spark or DuckDB.

```
<repository_root>/
├── data_type=AS_OF_AT/
│   └── dataset=my_versioned_dataset/
│       ├── as_of=2023-01-01T120000+0000/
│       │   └── part-0.parquet
│       └── as_of=2023-01-02T120000+0000/
│           └── part-0.parquet
└── data_type=NONE_AT/
    └── dataset=my_dataset/
        └── as_of=__HIVE_DEFAULT_PARTITION__/
            └── part-0.parquet
```

## 3. Repository Configuration

A "repository" is a named storage location for your time series.
It connects a data handler and a metadata handler to a specific set of paths.

Given the `io_handlers` defined above, a data repository can be configured as follows:

```json
{
    "repositories": {
        "my_repo": {
            "directory": {
                "path": "/path/to/your/timeseries/data",
                "handler": "my_data_handler"
            },
            "catalog": {
                "path": "/path/to/your/timeseries/metadata",
                "handler": "my_metadata_handler"
            },
            "default": true
        }
    }
}
```

-   **`repositories`**: The top-level key for all repository definitions.
-   **`my_repo`**: A custom name for your repository.
-   **`directory`**: Configures the primary data storage. Its `handler` key must match a handler defined in `io_handlers`.
-   **`catalog`**: Configures the metadata storage. Its `handler` key must also match a handler in `io_handlers`.
-   **`default`**: Setting this to `true` makes this the default repository for operations where one is not specified.

## 3. Snapshot and Sharing Configuration (`persist`)

The `persist` function copies datasets to immutable, versioned locations for archival or sharing.
This is controlled by the `snapshots` and `sharing` sections.

Given the `my_snapshot_handler` defined in the `io_handlers` section, a snapshot configuration can be set up as follows:

```json
{
    "snapshots": {
        "default": {
            "directory": {
                "path": "/path/to/your/snapshots",
                "handler": "my_snapshot_handler"
            }
        }
    },
    "sharing": {
        "default": {
            "directory": {
                "path": "/path/to/your/shared/default",
                "handler": "my_snapshot_handler"
            }
        }
    }
}
```

-   **`snapshots`**: Defines named locations for persisting datasets. The destination path is constructed as `<path>/<process_stage>/<product>/<dataset>/*.parquet`.
-   **`sharing`**: Defines named locations for sharing datasets.
-   The `Dataset` attributes `.sharing` and `.process_stage` are used to select the correct configuration paths at runtime.
