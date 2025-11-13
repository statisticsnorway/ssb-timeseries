"""The :py:mod:`ssb_timeseries.meta` module is responsible for metadata maintenance.

It re-exports functionality from submodules for backward compatibility and a unified API.
"""

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
from ssb_timeseries.meta.taxonomy import KlassTaxonomy
from ssb_timeseries.meta.taxonomy import Taxonomy
from ssb_timeseries.meta.taxonomy import permutations
