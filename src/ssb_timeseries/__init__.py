"""The :py:mod:`ssb_timeseries` package is a helper library for production and analysis of statistical data in the form of *time series*.

It is designed to make it as easy as possible to store data and metadata for datasets and series in ways that are consistent with the :doc:`information model <../info-model>`, and to facilitate integration with automated workflows.

Functionality includes:

* Read and write data and metadata
* Metadata maintenance: tagging, detagging, retagging
* Search and filtering
* Time algebra: downsampling and upsampling to other time resolutions
* Linear algebra operations with sets (matrices) and series (column vectors)
* Metadata aware calculations, like unit conversions and aggregation over taxonomy hierarchies
* Basic plotting

The most practical entry points are the :py:mod:`ssb_timeseries.dataset` and :py:mod:`ssb_timeseries.catalog` modules.

.. automodule:: ssb_timeseries.dataset
    :noindex:
    :synopsis:
    :exclude-members: __init__

.. automodule:: ssb_timeseries.catalog
    :noindex:
    :synopsis:
    :exclude-members: __init__

The other modules of the package are helpers used by these core modules, and not intended for direct use.

Some notable exceptions are taxonomy and hierarchy features of :py:mod:`ssb_timeseries.meta` and type definitions in :py:mod:`ssb_timeseries.properties`.
:py:mod:`ssb_timeseries.config` may be used for initial set up and later switching between repositories, if needed.
The :py:mod:`ssb_timeseries.io` seeks to make the storage agnostic of whether data and metada are stored in files or databases and :py:mod:`ssb_timeseries.fs` is an abstraction for local vs GCS file systems.
"""

from ssb_timeseries.config import CONFIG as configuration
from ssb_timeseries.config import Config
from ssb_timeseries.logging import set_up_logging_according_to_config

logger = set_up_logging_according_to_config(__name__, configuration.logging)

active_config = Config.active

__all__ = [
    "active_config",
    "configuration",
    "dataset",
    "dates",
    "fs",
    "io",
    "logger",
    "properties",
    "sample_data",
]
