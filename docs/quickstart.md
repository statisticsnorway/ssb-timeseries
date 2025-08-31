# How to get started?

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

The library expects an environment variable TIMESERIES_CONFIG to provide the path to a valid configuration file. The name or location of the file does not really matter, but some environments may require specific locations.

For users in Statistics Norway:

- `/home/onyxia/work/` for your own testing and development in **Dapla Lab**.
- `/home/onyxia/buckets/<bucket-name>` for working with sharp data.
- `gs://<team>/<timeseries-config-path>` for GCP service configuration.

The configuration file should be in JSON format.
A minimal working example suitable for testing can look like this:

```json
{
    "bucket": "/home/onyxia/work/timeseries/",
    "configuration_file": "/home/onyxia/work/timeseries/configuration/minimal.json",
    "logging": {},
    "repositories": {
        "my_repo": {
            "name": "my-repo",
            "catalog": "/home/onyxia/work/timeseries/my_repo/metadata",
            "directory": "/home/onyxia/work/timeseries/my_repo/data",
            "default": true
        }
    }
}
```

The easiest way to set the environment variable within a Dapla Lab session is with a cell magic. Assuming our configuration file is `/home/onyxia/work/timeseries/configuration/minimal.json`:

```
%env TIMESERIES_CONFIG=/home/onyxia/work/timeseries/configuration/minimal.json
```

Note that set this way, `TIMESERIES_CONFIG` will not be persisted between sessions, so the procedure need to be repeated every time.
Refer to the Dapla documentation for *permanently* setting environment variables.

With the environment variable set consistent with the location of a configuration file you should be ready to go.

See the [API reference] or tutorials section (coming soon) for more.

## Helper CLI

The library exposes some configuration management features in a helper CLI.


The command `poetry run timeseries-config <OPTION>` can be run from a terminal in order to shift between defaults.



## Disclaimer

Note that while the library is in a workable state and should work both locally and (for SSB users) in JupyterLab, it is still in early development. There is a risk that fundamental choices are reversed and breaking changes introduced.

Do not be shy about asking questions or giving feedback. The best channel for that is via https://github.com/statisticsnorway/ssb-timeseries/issues.
