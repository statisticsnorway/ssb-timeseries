---
title: Quickstart
marimo-version: 0.24.0
width: comnpact
---

Quickstart Guide 2.0
====================

Installation
------------

Clone from [GitHub](https://github.com/statisticsnorway/ssb-timeseries/),
or install from [PyPi](https://pypi.org/project/ssb-timeseries/):

```bash
poetry add ssb-timeseries
```
<!---->
Configuration
--------------

<!-- @output:bkHC -->

On first use, the library is likely to warn that it is not properly configured.
The library expects an environment variable TIMESERIES_CONFIG to identify a valid configuration file.
Neither name nor location of the file matters as long as the file is identified correctly, accessible and complies with the JSON schema for the library version,
but unless you are working in a pre-configured environment, none of these conditions are likely to be satisfied.

There are a few different approaches to maintain the configuration:
- Editing by hand
- Using the `config` module
- Using the command line interface (CLI)

The CLI
-------

The library provides a helper CLI with features that are mainly for inspection.
The CLI is accessible in a terminal shell where the library is installed.
This sounds obvious, but means that there are som subtle differences depending on how the library was installed and how Python virtual environments are managed in your working environment.

If `poetry` was used for installation, the commands below will need to be prefixed with `poetry run`.

From a terminal, the main entry point is `ssb-timeseries` or the shorthand `ts`:

`ts --help` will provide an overview of the CLI, whereas `ts config --help` will do the same for the configuration features.

``` bash
ts config path
```
will show the value of the TIMESERIES_CONFIG environment variable, if it is set.

``` bash
ts config show [option]
```
will show the entire active configuration, or a named preset.
This gives us a way to create or replace a configuration file:

``` bash
ts config show defaults > ~/.config/ssb-timeseries/default-config.json
```

Valid presets can be listed with

``` bash
ts config list
```
<!---->
Using the `config` module
-------------------------

```python {.marimo}
from ssb_timeseries.config import Config
```

The following Python code will apply and save default settings.

```python {.marimo}
cfg = Config(preset='default')
cfg.save()
cfg.activate()
```

<!-- @output:BYtC -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">&lt;ssb_timeseries.config.Config object at 0x7fcf4d9b7ed0&gt;</pre>

The defaults may be OK for local use or testing.
<!---->
The activation will set the environment variable, but only in the current shell.
That means, the effect is local and not permanent.
It will be lost after the shell session that Python runs inside ends.

<!-- @output:emfo -->

Note that while `.activate()` will set the environment variable, it wil not do so permanently.
The variable will be gone when the active shell session that Python runs within ends.

On a linux-like system, setting it permanently may look like:

```bash
echo 'export TIMESERIES_CONFIG="~/.config/ssb_timeseries/config.json"' >> .bashrc
```

To inspect the active configuration, either open the JSON file, or access it via `.active()`:

```python {.marimo}
Config.active()
```

<!-- @output:nWHF -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">&lt;ssb_timeseries.config.Config object at 0x7fcf4d9b7ed0&gt;</pre>

An alternative way is:

```python {.marimo}
import ssb_timeseries as ts

ts.get_configuration()
```

<!-- @output:ZHCJ -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">&lt;ssb_timeseries.config.Config object at 0x7fcf4d9b7ed0&gt;</pre>

```python {.marimo}
cfg is Config.active()
```

<!-- @output:ROlb -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">True</pre>

## Configuration Values Explained

The most important role of the configuration is to specify one or more "repositories" where data and meta data are stored, and associated with the "handlers" that implement the read and write functionality.

This is explained in more detail in the [Configure IO](..configure-io) guide.

A minimal working example for version 0.7.0 and above may look like this:

````python {.marimo}
mo.md(f"""
```json
{minimal(cfg)}
```
""")
````

<!-- @output:DnEU -->

```json
{
  "configuration_file": "/home/bernhard/.config/ssb_timeseries/timeseries_config.json",
  "io_handlers": {
    "json": {
      "handler": "ssb_timeseries.io.json_metadata.JsonMetaIO",
      "options": {}
    },
    "simple-parquet": {
      "handler": "ssb_timeseries.io.pyarrow_simple.FileSystem",
      "options": {}
    }
  },
  "logging": {},
  "repositories": {
    "<teamname>": {
      "catalog": {
        "handler": "json",
        "options": {
          "path": "/home/bernhard/timeseries/metadata"
        }
      },
      "directory": {
        "handler": "simple-parquet",
        "options": {
          "path": "/home/bernhard/timeseries"
        }
      }
    }
  }
}
```

Happy coding!
-------------
<!---->
With the library installed and configured all is set.
The guide to [basic usage](basic-usage) is a good place to go next.
<!---->
Issues or questions
-------------------

Users in Statistics Norway will know where to contact the maintainers directly.
For any external users, the best channel for discussion is through the project's [GitHub Issues](https://github.com/statisticsnorway/ssb-timeseries/issues).
