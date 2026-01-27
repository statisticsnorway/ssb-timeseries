# Quickstart Guide

## Installation

Clone from [GitHub](https://github.com/statisticsnorway/ssb-timeseries/),
or install from [PyPi](https://pypi.org/project/ssb-timeseries/):

```bash
poetry add ssb-timeseries
```

The library should work out of the box with default settings that may suffice for local use or testing,
but modifications are likely to be required.

## Configuration

The library expects an environment variable TIMESERIES_CONFIG to identify a valid configuration file,
but neither the name nor the location of the file matters.

The primary purpose of the configuration is to specify the "repositories" where data and meta data are stored,
and the "handlers" that implement the read and write functionality.
Data and metadata are configured independent of each other, multiple repositories can be configured at the same time, and handlers may support a variety of technologies.
See the {Configure IO} guide for more details.

The configuration can also define some behaviours of the library, notably logging.
Options include a standard logging.dictConfig or `"logging": {},` for no logging.
See the {Configure Logging} guide for examples.

A minimal working example for version 0.7.0 and above may look like this:

```json
{
    "bucket": "/home/onyxia/work/timeseries/",
    "configuration_file": "/home/onyxia/work/timeseries/configuration/minimal.json",
    "logging": {},
    "io_handlers": {
        "pyarrow-simple-parquet": {
            "handler": "ssb_timeseries.io.pyarrow_simple.FileSystem",
            "options": {}
        },
        "json": {
            "handler": "ssb_timeseries.io.json_metadata.JsonMetaIO",
            "options": {}
        }
    },
    "repositories": {
        "my_repo": {
            "name": "my_repo",
            "catalog": {
                "handler": "json",
                "options": {
                    "path": "/home/onyxia/work/timeseries/my_repo/metadata"
                }
            },
            "directory": {
                "handler": "simple-parquet",
                "options": {
                    "path": "/home/onyxia/work/timeseries/my_repo/data"
                }
            },
            "default": true
        }
    }
}
```

For users in Statistics Norway,

- `/home/onyxia/work/timeseries/configuration/timeseries_config.json` for working with your own testing and development in **Dapla Lab**.
- `/buckets/<bucket-name>/konfigurasjon/tidsserier/<name-of-file>` for working with the team's data.
- `gs://<team>/konfigurasjon/<name-of-file>` for GCP service configuration.

The easiest way to set the environment variable within a Dapla Lab session is with a cell magic.
Assuming the configuration file is `/home/onyxia/work/timeseries/configuration/minimal.json`:

```
%env TIMESERIES_CONFIG=/home/onyxia/work/timeseries/configuration/minimal.json
```

Note that managed this way, `TIMESERIES_CONFIG` will not be persisted between Jupyter sessions or kernel restarts.
Refer to the Dapla documentation for setting the variable at the project level or in a startup script.

With the environment variable pointing to the configuration file you should be ready to go.

See the [API reference] or tutorials section (coming soon) for more.

## Helper CLI

The library exposes some configuration management features in a helper CLI.
The command `poetry run timeseries-config <OPTION>` can be run from a terminal in order to shift between defaults.

## Disclaimer

The library is under active development and is considered stable for internal use.
While it has not officially reached a Minimum Viable Product (MVP) milestone, the core functionality is well-tested.
As we continue to refine the API, please be aware that breaking changes are still possible in future releases.

We welcome questions and feedback.
For users at Statistics Norway, feel free to contact the maintainers directly.
For any external users, the best channel for discussion is through the project's [GitHub Issues](https://github.com/statisticsnorway/ssb-timeseries/issues).
