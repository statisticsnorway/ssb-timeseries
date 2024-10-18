"""SSB timeseries is a helper library for statistics production and analytics.

It provides storage and search functionality with meta data and workflow integrations.

The very core of the library is the :py:class:`ssb_timeseries.dataset.Dataset`. It is designed for storage and workflow integration, and basic linear algebra operations can be performed on datasets.

Catalog
----------

.. automodule:: ssb_timeseries.catalog
    :show-inheritance:
    :noindex:

Config
------

.. automodule:: ssb_timeseries.config
    :show-inheritance:
    :noindex:

Dataset
-------

.. automodule:: ssb_timeseries.dataset
    :show-inheritance:
    :noindex:
"""

__all__ = [
    "dataset",
    "dates",
    "io",
    "fs",
    "config",
    "logging",
    "properties",
    "sample_data",
]
