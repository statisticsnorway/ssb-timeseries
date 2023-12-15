#!/usr/bin/env bash

# to make the  src/ directory visible to python:
export PYTHONPATH=${PYTHONPATH}:${PWD}/src/

# move this stuff to .venv?
# mkdir sample-data/series
# mkdir sample-data/logs

export TIMESERIES_ROOT=${PWD}/sample-data/series
export LOG_LOCATION=${PWD}/sample-data/logs
