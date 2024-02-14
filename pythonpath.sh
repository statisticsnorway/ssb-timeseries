#!/usr/bin/env bash

# do this from within python instead:
# mkdir sample-data/inndata
# mkdir sample-data/klargjorte-data
# mkdir sample-data/statistikk
# mkdir sample-data/utdata
# mkdir sample-data/series
# mkdir sample-data/logs
#
#    .io.init_root(path, products=['sample-data'], create_all=True)
#

# move this stuff to .venv?
#export PRODUCT_BUCKET=${PWD}
#export STATISTICS_PRODUCT=sample-data

# export BUCKET=${PWD}
# export PRODUCT=sample-data
# export TIMESERIES_ROOT=${BUCKET}/${PRODUCT}/series_data
# export LOG_LOCATION=${BUCKET}/${PRODUCT}/logs

export BUCKET=${PWD}/sample-data
