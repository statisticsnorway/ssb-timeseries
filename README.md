# arkitektur-poc-tidsserier



Created by:
Bernhard Ryeng <'rye@ssb.no

---

statisticsnorway/arkitektur-poc-tidsserier

Basic read/write functionality, calculations, time aggregation and plotting was demonstrated Friday December 8. See `src/demo.ipynb` for demo content and `tests/test_*.py` for more examples of what works and in some cases what does not.

Note that
 * filepaths are linux spesific, so Windows without WSL is nmot supported (yet).
 the solutionm relies on env vars  TIMESERIES_ROOT and LOG_LOCATION. They *must* be set if `/home/jovyan/sample-data` is not reachable, but *should* be set anyway.
* with env variables set and the code on python path `poetry run pytest` should succeed. If not, let me know that I ****** something up. ;) 

## To get started

The following should get you going:

``` bash
# get the poc package
git clone https://github.com/statisticsnorway/arkitektur-poc-tidsserier.git

# To run inside a poetry controlled venv:
poetry shell

# add ./arkitektur-poc-tidsserier/src/ to the python path:
export PYTHONPATH=${PYTHONPATH}:${PWD}/arkitektur-poc-tidsserier/src/

# to create and set a location for data and log files 
# it does not really matter where, although separated from the code is preferrable 
mkdir series
export TIMESERIES_ROOT=${PWD}/series
export LOG_LOCATION=${PWD}/series

# Run the tests TWICE to check that everything is OK: 
# (A couple of them will fail if the expected directory structure does not exist. # They should succeed the second time.) 
poetry run pytest
```

