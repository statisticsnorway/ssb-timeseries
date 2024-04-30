import os

# from ssb_timeseries import config

"""Use this module for running / validating setups?
ISSUE: the code is not found when trying to run `python timeseries`
... but is working fine for `python -m timeseries`
--> have a closer look at pyproject.toml?
"""


def main() -> None:
    """Validate setup."""
    config_file = os.environ["TIMESERIES_CONFIG"]
    greeting = f"Hello Timeseries!\n... the configuration file is: {config_file}. \nAdditional set up steps may be added later!"

    print(greeting)
    # perform set up steps:
    # os.environ["TIMESERIES_CONFIG"] = DEFAULT_CONFIG_LOCATION


if __name__ == "__main__":
    """Running `python timeseries` or `python -m timeseries` should run or validate setup."""
    main()
