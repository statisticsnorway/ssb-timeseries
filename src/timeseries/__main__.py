import os

# from timeseries import config


def main():
    config_file = os.environ["TIMESERIES_CONFIG"]
    greeting = (
        f"Hello Timeseries!\n... the configuration file is: {config_file}. \n"
        + "Additional set up steps may be added later!"
    )

    print(greeting)

    # perform set up steps:
    # os.environ["TIMESERIES_CONFIG"] = DEFAULT_CONFIG_LOCATION


if __name__ == "__main__":
    # execute only if run as the entry point into the program
    main()
