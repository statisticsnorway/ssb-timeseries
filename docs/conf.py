"""Sphinx configuration."""

# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys

# from tkinter import W


sys.path.insert(0, os.path.abspath("../src"))

# -- Project information -----------------------------------------------------

project = "SSB Timeseries"
copyright = "2024, Statistics Norway"
author = "Bernhard Ryeng"

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx.ext.doctest",
    "sphinx_autodoc_typehints",
    "sphinx_copybutton",
    "sphinx_togglebutton",
    "myst_parser",
    "sphinx_changelog",
    # "sphinx_marimo", # tested, but found it buggy and unmaintained
]

myst_enable_extensions = [
    "colon_fence",
]

# marimo(-sphinx) configs ------------------------------------------------------------
# marimo_notebook_dir = 'marimo'  # Directory containing .py Marimo notebooks
# marimo_default_height = '600px'
# marimo_default_width = '100%'

# because calc-with-metadata.md is in both calc and meta toctrees
suppress_warnings = ["toc.duplicate"]

# ---------------------------------------------------------------------------

# Add any paths that contain templates here, relative to this directory.
templates_path: list[str] = []

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = "zenburn"
pygments_dark_style = "zenburn"

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.

ssb_green_1 = "#ecfeed"
ssb_green_2 = "#b6e8b8"
ssb_green_3 = "#1a9d49"
ssb_green_4 = "#00824d"
ssb_green_5 = "#075745"

ssb_dark_1 = "#f0f8f9"
ssb_dark_2 = "#c3dcdc"
ssb_dark_3 = "#62919a"
ssb_dark_4 = "#2d6975"
ssb_dark_5 = "#274247"
ssb_dark_6 = "#162327"

ssb_purple_1 = "#f2f0ff"
ssb_purple_2 = "#D2CEFF"
ssb_purple_3 = "#9272fc"

ssb_red_1 = "#fdede7"
ssb_red_2 = "#ff896b"
ssb_red_3 = "#dc3400"
ssb_red_4 = "#cb3713"

ssb_white = "#ffffff"
ssb_blue_3 = "#3396d2"
negative_red = "#f8a67d"

html_theme = "furo"
html_theme_options = {
    # pick from https://profil.ssb.no/ after identifying which options to use
    "light_logo": "SSB_logo_black.svg",
    "light_css_variables": {
        "color-sidebar-background": ssb_dark_1,
        "color-sidebar-background-border": ssb_dark_5,
        "color-sidebar-search-background": "transparent",
        "color-sidebar-search-background--focus": ssb_dark_2,
        "color-brand-primary": ssb_dark_5,  # menu text level 1
        "color-foreground-primary": ssb_dark_6,  # menu text level 1
        "color-background-hover": ssb_green_2,
        "color-background--hover": ssb_green_2,  # RYEsource experiments
        "color-foreground--hover": "blue",  # RYE experiments
        "color-foreground-primary--focus": ssb_dark_5,
        # ...
        "color-background-secondary": ssb_white,  # kind of works, but on both sidebar second level AND content keywords
        "color-toc-background": "transparent",
        "color-guilabel-background": "red",
        "color-background-item": "orange",
        "color-card-item": "blue",
        # ...
        # expand/collapse at menu border
        "color-sidebar-item-expander-background": ssb_dark_1,
        "color-sidebar-item-expander-background--hover": ssb_green_1,
        "color-admonition-background": ssb_white,
        "color-admonition-title-background": ssb_green_4,
        "color-admonition-title-background-border": ssb_green_4,
    },
    "dark_logo": "SSB_logo_white.svg",
    "dark_css_variables": {
        "color-sidebar-background": "black",
        "color-sidebar-background-border": ssb_white,
        "color-sidebar-text": ssb_white,
        "color-sidebar-text--focus": "orange",
        "color-brand-primary": ssb_dark_1,  # menu text level 1
        "color-brand-secondary": "yellow",  # menu text level ..?
        "color-foreground-primary": ssb_green_1,  #
        "color-foreground-secondary": ssb_dark_5,
        "color-foreground-secondary--focus": ssb_dark_6,
        "color-foreground-secondary--hover": ssb_dark_6,
        # "color-foreground-secondary--focus": ssb_green_2,  # ssb_dark_1,
        "color-foreground-secondary--current": ssb_dark_1,
        "color-background--hover": ssb_green_2,
        "color-foreground--hover": ssb_dark_6,
        "color-sidebar-search-background": ssb_dark_3,
        "color-sidebar-search-foreground": ssb_dark_6,
        "color-sidebar-search-background--focus": ssb_dark_3,  # on click
        "color-sidebar-search-background-hover": "yellow",
        "color-sidebar-search-background--current": ssb_green_4,
        # "color-sidebar-search-text": ssb_dark_6,  # does not take effect
        # "color-sidebar-search-text--focus": ssb_dark_6,  # does not take effect
        # "color-content-background": "orange",
        # "color-background-primary": ssb_dark_5,  # (attempt) menu background "ssb dark 5"
        # "color-background-secondary":ssb_dark_5,  # (attempt) menu background "ssb dark 5"
        "color-sidebar-item-expander-background": ssb_dark_5,
        "color-sidebar-item-expander-background--hover": ssb_dark_4,
        # TO DO: Changelog icon is too dark. Fix.
        # TO DO: Blue reference headers / links are ugly/hard to read. Fix?
    },
    "navigation_with_keys": True,
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["ssb_static"]
# These paths are either relative to html_static_path
# or fully qualified paths (eg. https://...)
html_css_files = [
    "ssb.css",
]

# -- Other configuration ---------------------------------------------------

# Show typehints as content of the function or method
autodoc_typehints = "both"

# Do not prepend module names to object names
add_module_names = False

# Specific to SSB Timeseries -----------------------------------------------

# combine class and __init__ docstrings
autoclass_content = "class"

# Include special methods like __init__ and __call__ in the documentation
napoleon_include_special_with_doc = True
napoleon_include_init_with_doc = True

# put examples inside a box
napoleon_use_admonition_for_examples = False

# To fix "WARNING: local id not found in doc" "[myst.xref_missing]"
myst_heading_anchors = 3

autodoc_default_options = {
    "member-order": "alphabetical",
    # "special-members": "__init__",
    "undoc-members": True,
    "exclude-members": "__weakref__",  # not sure about this one?
}
copybutton_exclude = ".linenos, .gp"

rst_prolog = """
.. include:: <isogrk1.txt>
.. |br| raw:: html

   <br />

.. |p| replace:: |br| |br|

.. |tagging| replace:: There are several ways to maintain metadata (tags). \
        See the tagging guide for detailed information. \
        |p| |automatic_tagging| |p| |manual_tagging|

.. |automatic_tagging| replace:: Tagging functions\
    Automatic tagging with :py:meth:`~ssb_timeseries.dataset.Dataset.series_names_to_tags` is convenient when series names are constructed from metadata parts with a uniform pattern. \
    Then tags may be derived from series names by mappping name parts to ``attributes`` either by splitting on a ``separator`` or ``regex``.

.. |manual_tagging| replace:: Manually tagging a dataset with |tag_set| \
        will tag the set and propagate tags to all series in the set, while |tag_series| may be used to tag individual series.\
        If corrections need to be made, tags can be replaced with |replace_tags| or removed with |detag_set| and |detag_series|.

.. |tag_set| replace:: :py:meth:`~ssb_timeseries.dataset.Dataset.tag_dataset`
.. |tag_series| replace:: :py:meth:`~ssb_timeseries.dataset.Dataset.tag_series`

.. |retag_set| replace:: :py:meth:`~ssb_timeseries.dataset.Dataset.retag_dataset`
.. |retag_series| replace:: :py:meth:`~ssb_timeseries.dataset.Dataset.retag_series`

.. |replace_tags| replace:: :py:meth:`~ssb_timeseries.dataset.Dataset.replace_tags`

.. |detag_set| replace:: :py:meth:`~ssb_timeseries.dataset.Dataset.detag_dataset`
.. |detag_series| replace:: :py:meth:`~ssb_timeseries.dataset.Dataset.detag_series`

"""

# -- Options for autodoc ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html#configuration

# This mapping is used to resolve forward references in type annotations.
# It maps the string name to the fully qualified object name.
autodoc_typehint_aliases = {
    "Dataset": "ssb_timeseries.dataset.Dataset",
}
autodoc_typehints_format_aliases = True
autodoc_type_aliases = {
    "Any": "typing.Any",
    "DataFrame": "narwhals.DataFrame",
    "LazyFrame": "narwhals.LazyFrame",
    "Frame": "narwhals.typing.Frame",
    "FrameT": "narwhals.typing.FrameT",
    "IntoSeries": "narwhals.typing.IntoDataFrame",
    "IntoSeriesT": "narwhals.typing.IntoDataFrame",
    "IntoDataFrame": "narwhals.typing.IntoDataFrame",
    "IntoDataFrameT": "narwhals.typing.IntoDataFrameT",
    "IntoLazyFrame": "narwhals.typing.IntoLazyFrame",
    "IntoLazyFrameT": "narwhals.typing.IntoLazyFrameT",
}
