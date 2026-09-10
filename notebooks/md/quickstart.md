---
title: Quickstart
marimo-version: 0.24.0
width: comnpact
---

```python {.marimo hide_code="true"}
import marimo as mo
from tools import testing
from ssb_timeseries.config import ENV_VAR_NAME

mo.Html(
    """
    <style>
    [data-testid="static-notebook-banner"],
    [data-testid="watermark"] {
        display: none !important;
    }
    z-index: -2; /* Higher numbers sit on top of lower numbers */

    /* Hides the desktop sidebar table of contents */
    div[class*="marimo-toc"],
    aside[class*="sidebar"],
    [data-testid="marimo-toc"] {
        display: none !important;
    }

    /* Adjusts the main content margin to center it */
    main {
        margin-left: auto !important;
        margin-right: auto !important;
        max-width: 960px !important;
    }
    </style>
    """
)
```

# Quick Start Guide 2.0

## Installation

Clone from [GitHub](https://github.com/statisticsnorway/ssb-timeseries/),
or install from [PyPi](https://pypi.org/project/ssb-timeseries/):

```bash
poetry add ssb-timeseries
```
<!---->
## Configuration

```python {.marimo}
from ssb_timeseries.config import Config
```

```python {.marimo hide_code="true"}
mo.md(f"""
On first use, the library is likely to warn that it is not properly configured.
The library expects an environment variable {ENV_VAR_NAME} to identify a valid configuration file.
Unless you are in a pre-configured environment, none of these conditions are likely to be satisfied.

Neither name nor location of the file matters as long as the file is identified correctly, accessible and complies with the JSON schema for the library version.

The following Python code will apply and save default settings.
""")
```

```python {.marimo}
cfg = Config(preset='default')
cfg.activate()
cfg.save()
```

````python {.marimo hide_code="true"}
mo.md(f"""
The defaults may be OK for local use or testing.

Note that while `.activate()` will set the environment variable, it wil not do so permanently.
The variable will be gone when the active shell session that Python runs within ends.

On a linux-like system, setting it permanently may look like:

```bash
echo 'export {ENV_VAR_NAME}="~/.config/ssb_timeseries/config.json"' >> .bashrc
```
""")
````

## Inspecting the Configuration

To inspect the active configuration, either open the JSON file, or access it via `.active()`:

```python {.marimo}
Config.active()
```

An alternative way is:

```python {.marimo}
import ssb_timeseries as ts

ts.get_configuration()
```

```python {.marimo}
cfg is Config.active()
```

```python {.marimo hide_code="true" name="handlers"}
def handlers(c):
    return {
        handler
        for repo in c.repositories.values()
        for handler in (
            repo["catalog"]["handler"],
            repo["directory"]["handler"],
        )
    }
```

```python {.marimo hide_code="true" name="minimal"}
def minimal(c):
    active_handlers = handlers(c)
    used = {}
    for k,v in c.io_handlers.items():
        if k in active_handlers: used[k]=v

    c.io_handlers = used
    c.logging = {}
    return c
```

````python {.marimo hide_code="true"}
mo.md(f"""
## Configuration Values Explained

The most important role of the configuration is to specify one or more "repositories" where data and meta data are stored, and associated with the "handlers" that implement the read and write functionality.

This is explained in more detail in the [Configure IO](configure-io) guide.

A minimal working example for version 0.7.0 and above may look like this:
```json
{minimal(cfg)}
```
""")
````

## Helper CLI

The library exposes some configuration management features in a helper CLI.
The command `poetry run timeseries-config <OPTION>` can be run from a terminal in order to shift between defaults.

## We welcome questions and feedback.

For users at Statistics Norway, feel free to contact the maintainers directly.
For any external users, the best channel for discussion is through the project's [GitHub Issues](https://github.com/statisticsnorway/ssb-timeseries/issues).
<!---->
With the library installed and configured, we are ready to start coding.

See the [Getting Started Guide](getting_started.md) for an introduction to basic features.

```python {.marimo hide_code="true"}

```

```python {.marimo hide_code="true"}
def test_cfg_is_valid_config():
    assert isinstance(cfg, Config)
    assert cfg.is_valid
```

```python {.marimo hide_code="true"}
testing.run_and_report([test_cfg_is_valid_config])
```

```python {.marimo hide_code="true"}
mo.md(r"""

""")
```
