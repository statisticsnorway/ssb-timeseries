import marimo

__generated_with = "0.24.0"
app = marimo.App(width="comnpact")


@app.cell(hide_code=True)
def _():
    import marimo as mo
    from tools import testing
    from ssb_timeseries.config import ENV_VAR_NAME

    mo.Html(
        """
        <style>
        /*
        [data-testid="static-notebook-banner"],
        [data-testid="watermark"] {
            display: none !important;
        }
        */
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

    return ENV_VAR_NAME, mo, testing


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    # Quick Start Guide 2.0

    ## Installation

    Clone from [GitHub](https://github.com/statisticsnorway/ssb-timeseries/),
    or install from [PyPi](https://pypi.org/project/ssb-timeseries/):

    ```bash
    poetry add ssb-timeseries
    ```
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Configuration
    """)
    return


@app.cell
def _():
    from ssb_timeseries.config import Config

    return (Config,)


@app.cell(hide_code=True)
def _(ENV_VAR_NAME, mo):
    mo.md(f"""
    On first use, the library is likely to warn that it is not properly configured.
    The library expects an environment variable {ENV_VAR_NAME} to identify a valid configuration file.
    Unless you are in a pre-configured environment, none of these conditions are likely to be satisfied.

    Neither name nor location of the file matters as long as the file is identified correctly, accessible and complies with the JSON schema for the library version.

    The following Python code will apply and save default settings.
    """)
    return


@app.cell
def _(Config):
    cfg = Config(preset='default')
    cfg.activate()
    cfg.save()
    return (cfg,)


@app.cell(hide_code=True)
def _(ENV_VAR_NAME, mo):
    mo.md(f"""
    The defaults may be OK for local use or testing.

    Note that while `.activate()` will set the environment variable, it wil not do so permanently.
    The variable will be gone when the active shell session that Python runs within ends.

    On a linux-like system, setting it permanently may look like:

    ```bash
    echo 'export {ENV_VAR_NAME}="~/.config/ssb_timeseries/config.json"' >> .bashrc
    ```
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Inspecting the Configuration

    To inspect the active configuration, either open the JSON file, or access it via `.active()`:
    """)
    return


@app.cell
def _(Config):
    Config.active()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    An alternative way is:
    """)
    return


@app.cell
def _():
    import ssb_timeseries as ts

    ts.get_configuration()
    return


@app.cell
def _(Config, cfg):
    cfg is Config.active()
    return


@app.function(hide_code=True)
def handlers(c):
    return {
        handler
        for repo in c.repositories.values()
        for handler in (
            repo["catalog"]["handler"],
            repo["directory"]["handler"],
        )
    }


@app.function(hide_code=True)
def minimal(c):
    active_handlers = handlers(c)
    used = {}
    for k,v in c.io_handlers.items():
        if k in active_handlers: used[k]=v

    c.io_handlers = used
    c.logging = {}
    return c


@app.cell(hide_code=True)
def _(cfg, mo):
    mo.md(f"""
    ## Configuration Values Explained

    The most important role of the configuration is to specify one or more "repositories" where data and meta data are stored, and associated with the "handlers" that implement the read and write functionality.

    This is explained in more detail in the [Configure IO](configure-io) guide.

    A minimal working example for version 0.7.0 and above may look like this:
    ```json
    {minimal(cfg)}
    ```
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Helper CLI

    The library exposes some configuration management features in a helper CLI.
    The command `poetry run timeseries-config <OPTION>` can be run from a terminal in order to shift between defaults.

    ## We welcome questions and feedback.

    For users at Statistics Norway, feel free to contact the maintainers directly.
    For any external users, the best channel for discussion is through the project's [GitHub Issues](https://github.com/statisticsnorway/ssb-timeseries/issues).
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    With the library installed and configured, we are ready to start coding.

    See the [Getting Started Guide](getting_started.md) for an introduction to basic features.
    """)
    return


@app.cell(hide_code=True)
def _():
    return


@app.cell(hide_code=True)
def _(Config, cfg):
    def test_cfg_is_valid_config():
        assert isinstance(cfg, Config)
        assert cfg.is_valid

    return (test_cfg_is_valid_config,)


@app.cell(hide_code=True)
def _(test_cfg_is_valid_config, testing):
    testing.run_and_report([test_cfg_is_valid_config])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""

    """)
    return


if __name__ == "__main__":
    app.run()
