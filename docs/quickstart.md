# Quickstart Guide

## Installation

Clone from [GitHub](https://github.com/statisticsnorway/ssb-timeseries/),
or use your favourite dependency management tool to install from [PyPi](https://pypi.org/project/ssb-timeseries/).
Despite the increasing traction of `uv`, the standard in Statistics Norway is still `Poetry`:

```bash
poetry add ssb-timeseries
```


If you are lucky, the library works out of the box with default or predefined settings.

Note that the defaults are for local testing, ie not be suitable for the production setting.

## Configuration

The library expects an environment variable TIMESERIES_CONFIG to provide the path to a valid configuration file.
The name or location of the file does not really matter, but some environments may require specific locations.

For users in Statistics Norway:

- `/home/onyxia/work/` for your own testing and development in **Dapla Lab**.
- `/home/onyxia/buckets/<bucket-name>` for working with sharp data.
- `gs://<team>/<timeseries-config-path>` for GCP service configuration.

The configuration file should be in JSON format.
A minimal example suitable for testing (version 0.7.0 and above) look like this:

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
