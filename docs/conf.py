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
]

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
color_ssb_dark_5 = "#274247"
color_ssb_dark_2 = "#C3DCDC"
color_ssb_dark_1 = "#F0F8F9"
color_ssb_green_4 = "#00824D"
color_ssb_green_2 = "#B6E8B8"
color_ssb_green_1 = "#ECFEED"
color_ssb_white = "#ffffff"

html_theme = "furo"
html_theme_options = {
    # pick from https://profil.ssb.no/ after identifying which options to use
    "light_logo": "SSB_logo_black.svg",
    "light_css_variables": {
        "color-sidebar-background": color_ssb_dark_1,
        "color-sidebar-background-border": color_ssb_dark_5,
        "color-sidebar-search-background": color_ssb_dark_2,
        "color-sidebar-search-background--focus": color_ssb_green_1,
        "color-brand-primary": color_ssb_dark_5,  # menu text level 1
        "color-background-hover": color_ssb_dark_2,
        "color-foreground-primary": color_ssb_dark_5,
        "color-background-primary": color_ssb_white,
        # TO DO: fix background colours for second level menu
        # ...
        "color-background-secondary": color_ssb_white,  # kind of works, but on both sidebar second level AND content keywords
        # "color-toc-background": "white", # = -bakcground-primary
        "color-guilabel-background": "red",
        "color-background-item": "orange",
        "color-card-item": "orange",
        # ...
        "color-sidebar-item-expander-background": color_ssb_dark_1,
        "color-sidebar-item-expander-background--hover": color_ssb_green_1,
        "color-admonition-background": color_ssb_white,
        "color-admonition-title-background": color_ssb_green_4,
        "color-admonition-title-background-border": color_ssb_green_4,
    },
    "dark_logo": "SSB_logo_white.svg",
    "dark_css_variables": {
        "color-sidebar-background": "black",
        "color-sidebar-background-border": "white",
        "color-brand-primary": color_ssb_dark_1,  # menu text level 1
        "color-foreground-secondary": color_ssb_dark_1,
        "color-background-hover": color_ssb_dark_5,
        "color-sidebar-search-background": color_ssb_dark_5,
        # "color-sidebar-search-background--focus": color_ssb_green_4,
        "color-sidebar-search-text": "yellow",  # does not take effect
        "color_content_background": "orange",
        # "color-background-primary":color_ssb_dark_5,  # (attempt) menu background "ssb dark 5"
        # "color-background-secondary":color_ssb_dark_5,  # (attempt) menu background "ssb dark 5"
        "color-sidebar-item-expander-background": "black",  # color_ssb_dark_5,
        "color-sidebar-item-expander-background--hover": color_ssb_dark_5,
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
