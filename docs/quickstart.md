# How to get started?

## Install 

Install from PyPi with pip, poetry or your favourite dependency management tool:

````python
poetry add ssb_timeseries
````

Or clone from https://github.com/statisticsnorway/ssb-timeseries/.

The library should work out of the box with default settings. 

Note that the defaults are for local testing, ie not be suitable for the production setting.

## Configuration

To apply custom settings: The environment variable TIMESERIES_CONFIG should point to a JSON file with configurations.
- The command `poetry run timeseries-config <...>` can be run from a terminal in order to shift between defauls.
- Run `poetry run timeseries-config home` to create the environment variable and a file with default configurations in the home directory, ie `/home/jovyan` in the SSB Jupyter environment (or the equivalent running elsewhere).
- The similar `poetry run timeseries-config gcs` will put configurations and logs in the home directory and time series data in a shared GCS bucket `gs://ssb-prod-dapla-felles-data-delt/poc-tidsserier`. Take appropriate steps to make sure you have access. The library does not attempt to facilitate that.
- With the environment variable set and the configuration in place you should be all set. See the reference https://statisticsnorway.github.io/ssb-timeseries/reference.html

## Disclaimer

Note that while the library is in a workable state and should work both locally and (for SSB users) in JupyterLab, it is still in early development. There is a risk that fundamental choices are reversed and breaking changes introduced.

Do not be shy about asking questions or giving feedback. The best channel for that is via https://github.com/statisticsnorway/ssb-timeseries/issues.