"""The :py:mod:`ssb_timeseries.meta` module defines the public API for metadata operations.

It provides data structures and functions for managing tags and taxonomies.
Functionality is imported from submodules to create a single, convenient point of access.
"""

from ssb_timeseries.meta.loaders import KlassTaxonomy
from ssb_timeseries.meta.tags import DatasetTagDict
from ssb_timeseries.meta.tags import SeriesTagDict
from ssb_timeseries.meta.tags import TagDict
from ssb_timeseries.meta.tags import TagValue
from ssb_timeseries.meta.tags import add_tag_values
from ssb_timeseries.meta.tags import delete_dataset_tags
from ssb_timeseries.meta.tags import delete_series_tags
from ssb_timeseries.meta.tags import filter_tags
from ssb_timeseries.meta.tags import inherit_set_tags
from ssb_timeseries.meta.tags import matches_criteria
from ssb_timeseries.meta.tags import replace_dataset_tags
from ssb_timeseries.meta.tags import search_by_tags
from ssb_timeseries.meta.taxonomy import Taxonomy
from ssb_timeseries.meta.taxonomy import permutations
