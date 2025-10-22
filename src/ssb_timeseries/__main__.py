"""Use this module for running / validating setups?

ISSUE: the code is not found when trying to run `python timeseries`
... but is working fine for `python -m timeseries`
--> have a closer look at pyproject.toml?
"""

from __future__ import annotations

# from ssb_timeseries import config
import click

from ssb_timeseries.config import Config


@click.command()
def main() -> None:
    """Validate and print the active configuration."""
    active_config = Config.active()
    print(active_config)
    # perform set up steps:
    # os.environ["TIMESERIES_CONFIG"] = DEFAULT_CONFIG_LOCATION


if __name__ == "__main__":
    """Running `python timeseries` or `python -m timeseries` should run or validate setup."""
    main()
