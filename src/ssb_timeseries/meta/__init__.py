"""The :py:mod:`ssb_timeseries.meta` module is responsible for metadata maintenance.

It re-exports functionality from submodules for backward compatibility and a unified API.
"""

from ssb_timeseries.meta.formatters import camel_to_snake
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
from ssb_timeseries.meta.tags import rm_tag_values
from ssb_timeseries.meta.tags import rm_tags
from ssb_timeseries.meta.tags import search_by_tags
from ssb_timeseries.meta.tags import series_tag_dict_edit
from ssb_timeseries.meta.tags import to_tag_value
from ssb_timeseries.meta.tags import unique_tag_values
from ssb_timeseries.meta.taxonomy import DEFAULT_ROOT_NODE
from ssb_timeseries.meta.taxonomy import KLASS_ITEM_SCHEMA
from ssb_timeseries.meta.taxonomy import KlassTaxonomy
from ssb_timeseries.meta.taxonomy import MissingAttributeError
from ssb_timeseries.meta.taxonomy import Taxonomy
from ssb_timeseries.meta.taxonomy import klass_classification
from ssb_timeseries.meta.taxonomy import permutations
from ssb_timeseries.meta.taxonomy import records_to_arrow
