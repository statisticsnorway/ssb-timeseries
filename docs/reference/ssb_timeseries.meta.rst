:py:mod:`ssb_timeseries.meta`
==============================

The :py:mod:`ssb_timeseries.meta` module provides tools for managing metadata associated with datasets and time series. It exposes the core data structures for tags (defined in :py:mod:`ssb_timeseries.types`) and functionality for creating and manipulating taxonomies.

.. automodule:: ssb_timeseries.meta
   :noindex:

Public API
----------

The following classes and functions are exposed as the public API of the `meta` module.

**Classes**
~~~~~~~~~~~

.. autoclass:: ssb_timeseries.meta.Taxonomy
   :members:
   :undoc-members:
   :show-inheritance:

.. autodata:: ssb_timeseries.meta.KlassTaxonomy

**Type Aliases**
~~~~~~~~~~~~~~~~

.. py:data:: DatasetTags
   :annotation: = dict[str, Any]

   A TypedDict representing the full dataset metadata, including 'series'.

.. py:data:: SeriesTags
   :annotation: = dict[str, Tags]

   A dictionary mapping series names to their tags.

.. py:data:: Tags
   :annotation: = dict[str, TagValue]

   A dictionary of tags, where keys are tag keys and values are TagValues.

.. py:data:: TagValue
   :annotation: = str | list[str]

   A tag value is a single string or a list of strings.

.. py:data:: TagKey
   :annotation: = str

   A tag key is an attribute name represented as a string.

**Functions**
~~~~~~~~~~~~~

.. autofunction:: ssb_timeseries.meta.add_tag_values

.. autofunction:: ssb_timeseries.meta.delete_dataset_tags

.. autofunction:: ssb_timeseries.meta.delete_series_tags

.. autofunction:: ssb_timeseries.meta.filter_tags

.. autofunction:: ssb_timeseries.meta.inherit_set_tags

.. autofunction:: ssb_timeseries.meta.matches_criteria

.. autofunction:: ssb_timeseries.meta.replace_dataset_tags

.. autofunction:: ssb_timeseries.meta.search_by_tags

.. autofunction:: ssb_timeseries.meta.permutations
