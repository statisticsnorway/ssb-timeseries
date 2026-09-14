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

```python {.marimo}
from ssb_timeseries.config import Config
```

<!-- @output:lEQa -->

On first use, the library is likely to warn that it is not properly configured.
The library expects an environment variable TIMESERIES_CONFIG to identify a valid configuration file.
Unless you are in a pre-configured environment, none of these conditions are likely to be satisfied.

Neither name nor location of the file matters as long as the file is identified correctly, accessible and complies with the JSON schema for the library version.

The following Python code will apply and save default settings.

```python {.marimo}
cfg = Config(preset='default')
cfg.activate()
cfg.save()
```

<!-- @output:Xref -->

The defaults may be OK for local use or testing.

Note that while `.activate()` will set the environment variable, it wil not do so permanently.
The variable will be gone when the active shell session that Python runs within ends.

On a linux-like system, setting it permanently may look like:

```bash
echo 'export TIMESERIES_CONFIG="~/.config/ssb_timeseries/config.json"' >> .bashrc
```

Inspecting the Configuration
----------------------------

To inspect the active configuration, either open the JSON file, or access it via `.active()`:

```python {.marimo}
Config.active()
```

<!-- @output:BYtC -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">&lt;ssb_timeseries.config.Config object at 0x7f9c784eae90&gt;</pre>

An alternative way is:

```python {.marimo}
import ssb_timeseries as ts

ts.get_configuration()
```

<!-- @output:Kclp -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">&lt;ssb_timeseries.config.Config object at 0x7f9c784eae90&gt;</pre>

```python {.marimo}
cfg is Config.active()
```

<!-- @output:emfo -->

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

<!-- @output:ZHCJ -->

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
    "tutorials": {
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

## Helper CLI

The library exposes some configuration management features in a helper CLI.
The command `poetry run timeseries-config <OPTION>` can be run from a terminal in order to shift between defaults.
<!---->
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

<!-- @output:ulZA -->

<pre class="stderr" style="white-space: pre-wrap; overflow-wrap: break-word;">&#91;W 260914 19:28:43 compiler:353&#93; pytest is not installed, skipping assertion rewriting
</pre>
