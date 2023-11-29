#!/usr/bin/env bash
# to make the  src/ directory visible to python:

export PYTHONPATH=${PYTHONPATH}:${PWD}/src/

# move this stuff to .venv?
export TIMESERIES_ROOT=${PWD}/sample-data
export LOG_LOCATION=${PWD}/sample-data
