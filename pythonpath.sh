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
export PRODUCT_BUCKET=${PWD}
export STATISTICS_PRODUCT=sample-data
export TIMESERIES_ROOT=${PRODUCT_BUCKET}/${STATISTICS_PRODUCT}/series
export LOG_LOCATION=${PRODUCT_BUCKET}/${STATISTICS_PRODUCT}/logs
