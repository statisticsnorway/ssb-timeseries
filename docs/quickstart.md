# Quickstart Guide

## Installation

Clone from [GitHub](https://github.com/statisticsnorway/ssb-timeseries/),
or install from [PyPi](https://pypi.org/project/ssb-timeseries/):

```bash
poetry add ssb-timeseries
```

The library should work out of the box with default settings.
For local use or testing, but are likely to require modifications to be suitable in a production setting.

## Configuration

The library expects an environment variable TIMESERIES_CONFIG to identify a configuration file.
Neither name nor location of the file matters as long as the file is identified correctly, accessible and complies with the JSON schema for the library version.

A minimal configuration specifies the "repositories" where data and meta data are stored, and associated with the "handlers" that implement the read and write functionality.
See the {Configure IO} guide for more details, multiple repositories can  different
A working example for version 0.7.0 and above may look like this:

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

Note that managed this way, `TIMESERIES_CONFIG` will not be persisted between subshells.
The procedure need to be repeated for every new session or kernel restart.
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
