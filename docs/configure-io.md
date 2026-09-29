# Configure I/O

This guide provides detailed examples for configuring data repositories, metadata catalogs, and archiving behavior in `ssb-timeseries`.

## 1. IO Handlers

The `io_handlers` section of your configuration file defines the backend Python classes that will handle reading and writing data.
You must define a handler for each type of storage interaction you need (e.g., for data, metadata, and archives).

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
        "my_archive_handler": {
            "handler": "ssb_timeseries.io.archiving.Archive",
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

## 3. Archive and Sharing Configuration (`archive`)

The `archive` function writes an immutable, versioned copy of a dataset's data, keeping every version it has ever written.
This is controlled by the `snapshots` and `sharing` sections.

Given the `my_archive_handler` defined in the `io_handlers` section, an archive configuration can be set up as follows:

```json
{
    "snapshots": {
        "default": {
            "directory": {
                "path": "/path/to/your/archives",
                "handler": "my_archive_handler"
            },
            "options": {
                "archive_format": "ssb"
            }
        }
    },
    "sharing": {
        "default": {
            "directory": {
                "path": "/path/to/your/shared/default",
                "handler": "my_archive_handler"
            }
        }
    }
}
```

-   **`snapshots`**: Defines where archives are written. The `options` block configures how they are written rather than where.
-   **`archive_format`**: Names the convention to follow, either `ssb` or `generic` (the default, which assumes nothing about your organisation). The convention decides the folders, the file names and the data format, and whether an archive is also copied to the dataset's shared locations.
-   **`sharing`**: Defines the named locations datasets can be shared to.
-   The `Dataset` attribute `.sharing` is a list of the keys in `sharing`, not a list of paths, so a dataset says which locations it belongs in and never where they are.
-   A key with no location of its own is archived to the `default` sharing location, so naming a location that is not set up yet is not a reason to write nothing.
