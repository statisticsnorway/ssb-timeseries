---
title: Meta Search And Filtering
marimo-version: 0.24.0
---

# Metadata search and filtering
<!---->
Scope
-----

This guide demonstrates use of meta for finding data and for filtering.
<!---->
Prerequisites
-------------

``` {note}
The guide assumes that the SSB Timeseries library is installed and that a working configuration is active.
See [the quickstart guide](quickstart) for instructions to that.
```

```python {.marimo}
from ssb_timeseries import get_catalog
from ssb_timeseries.dataset import Dataset
```

The timeseries "catalog"
------------------------
<!---->
The catalog is the basis for search.
A catalog can consist of one or more repositories.
In the typical case, it is just accessed through the top level function `get_catalog` which will provide a catalog spanning all repositories in the active configuration.
The catalog aggegates all the tags for `Dataset` and `Series` to make them searchable in a single structure.

```python {.marimo}
timeseries_catalog = get_catalog()
```

To list the datasets in the catalog:

```python {.marimo}
all_sets = timeseries_catalog.datasets()
catalog_item_list_to_df(all_sets)
```

<!-- @output:Kclp -->

| repository_name | object_name | object_type | object_tags | parent | children |
| --- | --- | --- | --- | --- | --- |
| tutorials | AZ_drikkevarer | dataset | {4 set tags
+260 series} |  | None |
| tutorials | Prices and Volumes | dataset | {4 set tags
+12 series} |  | None |
| tutorials | A Sample Dataset | dataset | {4 set tags
+3 series} |  | None |
| tutorials | PQR | dataset | {6 set tags
+3 series} |  | None |
| tutorials | Sample Data | dataset | {6 set tags
+3 series} |  | None |
| tutorials | AZ Drinks | dataset | {4 set tags
+260 series} |  | None |
| tutorials | XYZ | dataset | {4 set tags
+3 series} |  | None |
| tutorials | More Prices and Volumes | dataset | {4 set tags
+636 series} |  | None |
| tutorials | AZ_omsetning | dataset | {5 set tags
+130 series} |  | None |
| tutorials | AZ_drinks | dataset | {4 set tags
+2080 series} |  | None |

Or the unique repositories:

```python {.marimo}
repositories = {s.repository_name for s in all_sets}
repositories
```

<!-- @output:Hstk -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">{&#x27;tutorials&#x27;}</pre>

The catalog has a `.repositories` property.
It returns a list of the actual repository objects.
Repositories and catalogs share the most important methods: `.datasets()`, `.series()` and `.items()` for both.
They all work the same way, taking the same parameters. The difference between the catalog and repository variants is simply that the catalog distributes the method calls to all its repositories.
<!---->
Call with no parameters to get all items:

```python {.marimo}
everything = timeseries_catalog.items()
```

<!-- @output:ROlb -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">3400</pre>

Or, to search for all `Datasets` that contain `Series` tagged with `{'product': 'coffee'}`, just return the `parent` for the `.series` search:

```python {.marimo}
sets_that_have_series_tagged_with = {result.parent for result in timeseries_catalog.series(tags={'product': 'coffee'})}
```

```python {.marimo}
[s.object_name for s in all_sets]
```

<!-- @output:Vxnm -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">&#91;&#x27;AZ_drikkevarer&#x27;,
 &#x27;Prices and Volumes&#x27;,
 &#x27;A Sample Dataset&#x27;,
 &#x27;PQR&#x27;,
 &#x27;Sample Data&#x27;,
 &#x27;AZ Drinks&#x27;,
 &#x27;XYZ&#x27;,
 &#x27;More Prices and Volumes&#x27;,
 &#x27;AZ_omsetning&#x27;,
 &#x27;AZ_drinks&#x27;&#93;</pre>

To get a global tag dictionary:

```python {.marimo}
all_sets_tag_dict = {s.object_name: s.object_tags for s in all_sets}
all_sets_tag_dict['Sample Data']
```

<!-- @output:ulZA -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">{&#x27;name&#x27;: &#x27;Sample Data&#x27;,
 &#x27;product group&#x27;: &#x27;essential&#x27;,
 &#x27;repository&#x27;: &#x27;tutorials&#x27;,
 &#x27;series&#x27;: {&#x27;x&#x27;: {&#x27;area&#x27;: &#x27;x&#x27;,
                  &#x27;dataset&#x27;: &#x27;Sample Data&#x27;,
                  &#x27;name&#x27;: &#x27;x&#x27;,
                  &#x27;product&#x27;: &#x27;coffee&#x27;,
                  &#x27;product group&#x27;: &#x27;essential&#x27;,
                  &#x27;repository&#x27;: &#x27;tutorials&#x27;,
                  &#x27;temporality&#x27;: &#x27;AT&#x27;,
                  &#x27;variable&#x27;: &#x27;price&#x27;,
                  &#x27;versioning&#x27;: &#x27;AS_OF&#x27;},
            &#x27;y&#x27;: {&#x27;area&#x27;: &#x27;y&#x27;,
                  &#x27;dataset&#x27;: &#x27;Sample Data&#x27;,
                  &#x27;name&#x27;: &#x27;y&#x27;,
                  &#x27;product&#x27;: &#x27;crispbread&#x27;,
                  &#x27;product group&#x27;: &#x27;essential&#x27;,
                  &#x27;repository&#x27;: &#x27;tutorials&#x27;,
                  &#x27;temporality&#x27;: &#x27;AT&#x27;,
                  &#x27;variable&#x27;: &#x27;price&#x27;,
                  &#x27;versioning&#x27;: &#x27;AS_OF&#x27;},
            &#x27;z&#x27;: {&#x27;area&#x27;: &#x27;z&#x27;,
                  &#x27;dataset&#x27;: &#x27;Sample Data&#x27;,
                  &#x27;name&#x27;: &#x27;z&#x27;,
                  &#x27;product&#x27;: &#x27;brown cheese&#x27;,
                  &#x27;product group&#x27;: &#x27;essential&#x27;,
                  &#x27;repository&#x27;: &#x27;tutorials&#x27;,
                  &#x27;temporality&#x27;: &#x27;AT&#x27;,
                  &#x27;variable&#x27;: &#x27;price&#x27;,
                  &#x27;versioning&#x27;: &#x27;AS_OF&#x27;}},
 &#x27;temporality&#x27;: &#x27;AT&#x27;,
 &#x27;variable&#x27;: &#x27;price&#x27;,
 &#x27;versioning&#x27;: &#x27;AS_OF&#x27;}</pre>

For each item, the `object_tags` in the global dictionary is the same as `Dataset.tags`:

```python {.marimo}
sample_data = Dataset('Sample Data')
sample_data.tags == all_sets_tag_dict['Sample Data']
```

<!-- @output:Pvdt -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">True</pre>

Searches that rely only on names and tags can be performed without accessing data:

```python {.marimo}
series = timeseries_catalog.series(tags={'dataset': ['Sample Data', 'XYZ']})
[f"{s.repository_name}/{s.parent}/{s.object_name}" for s in series]
```

<!-- @output:aLJB -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">&#91;&#x27;tutorials/Sample Data/x&#x27;,
 &#x27;tutorials/Sample Data/y&#x27;,
 &#x27;tutorials/Sample Data/z&#x27;,
 &#x27;tutorials/XYZ/x&#x27;,
 &#x27;tutorials/XYZ/y&#x27;,
 &#x27;tutorials/XYZ/z&#x27;&#93;</pre>

```python {.marimo}
catalog_item_list_to_df(everything)
```

<!-- @output:nHfw -->

| repository_name | object_name | object_type | object_tags | parent | children |
| --- | --- | --- | --- | --- | --- |
| tutorials | AZ_drikkevarer | dataset | {4 set tags
+260 series} |  | None |
| tutorials | a_antall_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | a_antall_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | a_antall_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | a_antall_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | a_antall_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | a_pris_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | a_pris_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | a_pris_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | a_pris_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | a_pris_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | b_antall_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | b_antall_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | b_antall_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | b_antall_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | b_antall_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | b_pris_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | b_pris_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | b_pris_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | b_pris_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | b_pris_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | c_antall_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | c_antall_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | c_antall_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | c_antall_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | c_antall_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | c_pris_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | c_pris_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | c_pris_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | c_pris_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | c_pris_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | d_antall_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | d_antall_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | d_antall_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | d_antall_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | d_antall_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | d_pris_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | d_pris_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | d_pris_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | d_pris_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | d_pris_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | e_antall_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | e_antall_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | e_antall_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | e_antall_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | e_antall_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | e_pris_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | e_pris_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | e_pris_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | e_pris_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | e_pris_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | f_antall_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | f_antall_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | f_antall_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | f_antall_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | f_antall_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | f_pris_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | f_pris_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | f_pris_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | f_pris_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | f_pris_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | g_antall_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | g_antall_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | g_antall_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | g_antall_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | g_antall_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | g_pris_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | g_pris_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | g_pris_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | g_pris_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | g_pris_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | h_antall_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | h_antall_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | h_antall_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | h_antall_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | h_antall_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | h_pris_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | h_pris_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | h_pris_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | h_pris_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | h_pris_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | i_antall_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | i_antall_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | i_antall_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | i_antall_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | i_antall_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | i_pris_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | i_pris_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | i_pris_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | i_pris_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | i_pris_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | j_antall_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | j_antall_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | j_antall_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | j_antall_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | j_antall_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | j_pris_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | j_pris_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | j_pris_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | j_pris_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | j_pris_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | k_antall_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | k_antall_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | k_antall_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | k_antall_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | k_antall_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | k_pris_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | k_pris_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | k_pris_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | k_pris_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | k_pris_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | l_antall_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | l_antall_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | l_antall_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | l_antall_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | l_antall_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | l_pris_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | l_pris_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | l_pris_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | l_pris_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | l_pris_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | m_antall_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | m_antall_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | m_antall_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | m_antall_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | m_antall_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | m_pris_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | m_pris_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | m_pris_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | m_pris_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | m_pris_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | n_antall_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | n_antall_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | n_antall_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | n_antall_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | n_antall_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | n_pris_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | n_pris_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | n_pris_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | n_pris_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | n_pris_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | o_antall_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | o_antall_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | o_antall_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | o_antall_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | o_antall_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | o_pris_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | o_pris_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | o_pris_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | o_pris_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | o_pris_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | p_antall_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | p_antall_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | p_antall_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | p_antall_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | p_antall_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | p_pris_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | p_pris_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | p_pris_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | p_pris_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | p_pris_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | q_antall_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | q_antall_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | q_antall_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | q_antall_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | q_antall_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | q_pris_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | q_pris_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | q_pris_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | q_pris_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | q_pris_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | r_antall_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | r_antall_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | r_antall_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | r_antall_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | r_antall_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | r_pris_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | r_pris_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | r_pris_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | r_pris_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | r_pris_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | s_antall_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | s_antall_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | s_antall_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | s_antall_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | s_antall_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | s_pris_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | s_pris_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | s_pris_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | s_pris_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | s_pris_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | t_antall_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | t_antall_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | t_antall_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | t_antall_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | t_antall_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | t_pris_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | t_pris_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | t_pris_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | t_pris_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | t_pris_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | u_antall_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | u_antall_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | u_antall_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | u_antall_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | u_antall_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | u_pris_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | u_pris_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | u_pris_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | u_pris_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | u_pris_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | v_antall_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | v_antall_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | v_antall_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | v_antall_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | v_antall_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | v_pris_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | v_pris_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | v_pris_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | v_pris_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | v_pris_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | w_antall_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | w_antall_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | w_antall_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | w_antall_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | w_antall_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | w_pris_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | w_pris_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | w_pris_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | w_pris_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | w_pris_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | x_antall_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | x_antall_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | x_antall_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | x_antall_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | x_antall_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | x_pris_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | x_pris_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | x_pris_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | x_pris_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | x_pris_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | y_antall_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | y_antall_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | y_antall_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | y_antall_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | y_antall_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | y_pris_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | y_pris_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | y_pris_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | y_pris_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | y_pris_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | z_antall_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | z_antall_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | z_antall_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | z_antall_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | z_antall_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | z_pris_brus | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | z_pris_kaffe | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | z_pris_te | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | z_pris_vin | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | z_pris_øl | series | {8 series tags} | AZ_drikkevarer | None |
| tutorials | Prices and Volumes | dataset | {4 set tags
+12 series} |  | None |
| tutorials | price_bread | series | {7 series tags} | Prices and Volumes | None |
| tutorials | price_cheese | series | {7 series tags} | Prices and Volumes | None |
| tutorials | price_eggs | series | {7 series tags} | Prices and Volumes | None |
| tutorials | price_ham | series | {7 series tags} | Prices and Volumes | None |
| tutorials | price_juice | series | {7 series tags} | Prices and Volumes | None |
| tutorials | price_milk | series | {7 series tags} | Prices and Volumes | None |
| tutorials | volume_bread | series | {7 series tags} | Prices and Volumes | None |
| tutorials | volume_cheese | series | {7 series tags} | Prices and Volumes | None |
| tutorials | volume_eggs | series | {7 series tags} | Prices and Volumes | None |
| tutorials | volume_ham | series | {7 series tags} | Prices and Volumes | None |
| tutorials | volume_juice | series | {7 series tags} | Prices and Volumes | None |
| tutorials | volume_milk | series | {7 series tags} | Prices and Volumes | None |
| tutorials | A Sample Dataset | dataset | {4 set tags
+3 series} |  | None |
| tutorials | x | series | {2 series tags} | A Sample Dataset | None |
| tutorials | y | series | {2 series tags} | A Sample Dataset | None |
| tutorials | z | series | {2 series tags} | A Sample Dataset | None |
| tutorials | PQR | dataset | {6 set tags
+3 series} |  | None |
| tutorials | p | series | {8 series tags} | PQR | None |
| tutorials | q | series | {8 series tags} | PQR | None |
| tutorials | r | series | {8 series tags} | PQR | None |
| tutorials | Sample Data | dataset | {6 set tags
+3 series} |  | None |
| tutorials | x | series | {9 series tags} | Sample Data | None |
| tutorials | y | series | {9 series tags} | Sample Data | None |
| tutorials | z | series | {9 series tags} | Sample Data | None |
| tutorials | AZ Drinks | dataset | {4 set tags
+260 series} |  | None |
| tutorials | a_price_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | a_price_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | a_price_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | a_price_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | a_price_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | a_volume_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | a_volume_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | a_volume_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | a_volume_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | a_volume_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | b_price_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | b_price_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | b_price_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | b_price_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | b_price_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | b_volume_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | b_volume_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | b_volume_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | b_volume_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | b_volume_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | c_price_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | c_price_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | c_price_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | c_price_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | c_price_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | c_volume_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | c_volume_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | c_volume_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | c_volume_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | c_volume_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | d_price_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | d_price_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | d_price_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | d_price_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | d_price_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | d_volume_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | d_volume_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | d_volume_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | d_volume_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | d_volume_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | e_price_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | e_price_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | e_price_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | e_price_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | e_price_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | e_volume_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | e_volume_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | e_volume_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | e_volume_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | e_volume_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | f_price_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | f_price_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | f_price_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | f_price_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | f_price_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | f_volume_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | f_volume_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | f_volume_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | f_volume_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | f_volume_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | g_price_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | g_price_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | g_price_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | g_price_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | g_price_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | g_volume_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | g_volume_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | g_volume_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | g_volume_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | g_volume_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | h_price_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | h_price_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | h_price_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | h_price_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | h_price_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | h_volume_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | h_volume_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | h_volume_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | h_volume_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | h_volume_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | i_price_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | i_price_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | i_price_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | i_price_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | i_price_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | i_volume_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | i_volume_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | i_volume_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | i_volume_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | i_volume_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | j_price_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | j_price_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | j_price_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | j_price_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | j_price_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | j_volume_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | j_volume_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | j_volume_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | j_volume_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | j_volume_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | k_price_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | k_price_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | k_price_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | k_price_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | k_price_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | k_volume_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | k_volume_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | k_volume_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | k_volume_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | k_volume_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | l_price_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | l_price_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | l_price_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | l_price_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | l_price_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | l_volume_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | l_volume_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | l_volume_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | l_volume_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | l_volume_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | m_price_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | m_price_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | m_price_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | m_price_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | m_price_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | m_volume_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | m_volume_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | m_volume_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | m_volume_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | m_volume_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | n_price_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | n_price_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | n_price_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | n_price_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | n_price_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | n_volume_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | n_volume_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | n_volume_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | n_volume_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | n_volume_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | o_price_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | o_price_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | o_price_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | o_price_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | o_price_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | o_volume_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | o_volume_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | o_volume_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | o_volume_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | o_volume_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | p_price_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | p_price_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | p_price_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | p_price_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | p_price_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | p_volume_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | p_volume_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | p_volume_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | p_volume_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | p_volume_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | q_price_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | q_price_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | q_price_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | q_price_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | q_price_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | q_volume_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | q_volume_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | q_volume_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | q_volume_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | q_volume_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | r_price_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | r_price_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | r_price_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | r_price_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | r_price_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | r_volume_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | r_volume_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | r_volume_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | r_volume_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | r_volume_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | s_price_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | s_price_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | s_price_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | s_price_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | s_price_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | s_volume_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | s_volume_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | s_volume_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | s_volume_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | s_volume_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | t_price_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | t_price_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | t_price_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | t_price_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | t_price_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | t_volume_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | t_volume_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | t_volume_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | t_volume_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | t_volume_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | u_price_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | u_price_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | u_price_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | u_price_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | u_price_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | u_volume_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | u_volume_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | u_volume_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | u_volume_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | u_volume_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | v_price_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | v_price_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | v_price_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | v_price_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | v_price_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | v_volume_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | v_volume_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | v_volume_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | v_volume_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | v_volume_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | w_price_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | w_price_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | w_price_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | w_price_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | w_price_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | w_volume_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | w_volume_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | w_volume_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | w_volume_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | w_volume_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | x_price_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | x_price_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | x_price_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | x_price_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | x_price_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | x_volume_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | x_volume_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | x_volume_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | x_volume_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | x_volume_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | y_price_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | y_price_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | y_price_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | y_price_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | y_price_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | y_volume_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | y_volume_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | y_volume_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | y_volume_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | y_volume_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | z_price_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | z_price_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | z_price_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | z_price_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | z_price_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | z_volume_beer | series | {8 series tags} | AZ Drinks | None |
| tutorials | z_volume_coffe | series | {8 series tags} | AZ Drinks | None |
| tutorials | z_volume_softdrinks | series | {8 series tags} | AZ Drinks | None |
| tutorials | z_volume_tea | series | {8 series tags} | AZ Drinks | None |
| tutorials | z_volume_wine | series | {8 series tags} | AZ Drinks | None |
| tutorials | XYZ | dataset | {4 set tags
+3 series} |  | None |
| tutorials | x | series | {2 series tags} | XYZ | None |
| tutorials | y | series | {2 series tags} | XYZ | None |
| tutorials | z | series | {2 series tags} | XYZ | None |
| tutorials | More Prices and Volumes | dataset | {4 set tags
+636 series} |  | None |
| tutorials | price_bread_1.1.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_1.1.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_1.1.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_1.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_11.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_11.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_12.1.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_12.1.10 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_12.1.11 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_12.1.12 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_12.1.13 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_12.1.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_12.1.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_12.1.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_12.1.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_12.1.6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_12.1.7 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_12.1.8 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_12.1.9 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_12.2.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_12.2.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_12.2.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_12.2.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_12.2.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_12.3.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_12.3.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_12.3.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_12.3.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_13 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_14 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_15 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_4.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_4.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_7.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_7.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_7.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_7.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_7.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_7.6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_8.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_8.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_8.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_8.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_8.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_8.6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_8.7 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_8.8 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_8.9 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_bread_9 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_1.1.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_1.1.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_1.1.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_1.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_11.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_11.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_12.1.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_12.1.10 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_12.1.11 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_12.1.12 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_12.1.13 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_12.1.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_12.1.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_12.1.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_12.1.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_12.1.6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_12.1.7 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_12.1.8 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_12.1.9 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_12.2.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_12.2.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_12.2.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_12.2.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_12.2.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_12.3.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_12.3.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_12.3.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_12.3.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_13 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_14 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_15 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_4.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_4.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_7.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_7.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_7.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_7.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_7.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_7.6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_8.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_8.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_8.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_8.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_8.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_8.6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_8.7 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_8.8 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_8.9 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_cheese_9 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_1.1.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_1.1.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_1.1.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_1.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_11.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_11.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_12.1.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_12.1.10 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_12.1.11 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_12.1.12 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_12.1.13 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_12.1.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_12.1.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_12.1.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_12.1.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_12.1.6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_12.1.7 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_12.1.8 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_12.1.9 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_12.2.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_12.2.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_12.2.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_12.2.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_12.2.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_12.3.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_12.3.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_12.3.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_12.3.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_13 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_14 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_15 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_4.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_4.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_7.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_7.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_7.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_7.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_7.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_7.6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_8.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_8.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_8.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_8.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_8.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_8.6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_8.7 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_8.8 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_8.9 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_eggs_9 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_1.1.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_1.1.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_1.1.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_1.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_11.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_11.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_12.1.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_12.1.10 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_12.1.11 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_12.1.12 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_12.1.13 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_12.1.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_12.1.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_12.1.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_12.1.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_12.1.6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_12.1.7 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_12.1.8 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_12.1.9 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_12.2.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_12.2.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_12.2.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_12.2.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_12.2.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_12.3.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_12.3.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_12.3.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_12.3.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_13 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_14 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_15 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_4.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_4.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_7.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_7.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_7.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_7.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_7.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_7.6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_8.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_8.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_8.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_8.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_8.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_8.6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_8.7 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_8.8 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_8.9 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_ham_9 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_1.1.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_1.1.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_1.1.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_1.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_11.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_11.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_12.1.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_12.1.10 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_12.1.11 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_12.1.12 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_12.1.13 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_12.1.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_12.1.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_12.1.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_12.1.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_12.1.6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_12.1.7 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_12.1.8 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_12.1.9 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_12.2.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_12.2.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_12.2.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_12.2.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_12.2.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_12.3.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_12.3.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_12.3.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_12.3.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_13 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_14 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_15 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_4.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_4.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_7.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_7.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_7.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_7.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_7.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_7.6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_8.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_8.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_8.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_8.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_8.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_8.6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_8.7 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_8.8 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_8.9 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_juice_9 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_1.1.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_1.1.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_1.1.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_1.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_11.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_11.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_12.1.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_12.1.10 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_12.1.11 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_12.1.12 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_12.1.13 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_12.1.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_12.1.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_12.1.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_12.1.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_12.1.6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_12.1.7 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_12.1.8 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_12.1.9 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_12.2.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_12.2.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_12.2.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_12.2.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_12.2.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_12.3.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_12.3.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_12.3.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_12.3.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_13 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_14 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_15 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_4.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_4.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_7.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_7.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_7.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_7.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_7.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_7.6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_8.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_8.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_8.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_8.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_8.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_8.6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_8.7 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_8.8 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_8.9 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | price_milk_9 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_1.1.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_1.1.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_1.1.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_1.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_11.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_11.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_12.1.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_12.1.10 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_12.1.11 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_12.1.12 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_12.1.13 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_12.1.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_12.1.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_12.1.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_12.1.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_12.1.6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_12.1.7 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_12.1.8 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_12.1.9 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_12.2.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_12.2.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_12.2.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_12.2.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_12.2.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_12.3.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_12.3.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_12.3.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_12.3.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_13 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_14 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_15 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_4.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_4.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_7.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_7.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_7.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_7.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_7.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_7.6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_8.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_8.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_8.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_8.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_8.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_8.6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_8.7 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_8.8 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_8.9 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_bread_9 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_1.1.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_1.1.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_1.1.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_1.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_11.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_11.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_12.1.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_12.1.10 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_12.1.11 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_12.1.12 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_12.1.13 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_12.1.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_12.1.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_12.1.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_12.1.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_12.1.6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_12.1.7 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_12.1.8 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_12.1.9 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_12.2.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_12.2.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_12.2.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_12.2.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_12.2.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_12.3.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_12.3.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_12.3.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_12.3.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_13 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_14 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_15 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_4.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_4.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_7.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_7.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_7.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_7.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_7.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_7.6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_8.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_8.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_8.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_8.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_8.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_8.6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_8.7 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_8.8 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_8.9 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_cheese_9 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_1.1.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_1.1.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_1.1.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_1.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_11.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_11.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_12.1.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_12.1.10 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_12.1.11 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_12.1.12 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_12.1.13 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_12.1.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_12.1.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_12.1.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_12.1.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_12.1.6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_12.1.7 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_12.1.8 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_12.1.9 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_12.2.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_12.2.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_12.2.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_12.2.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_12.2.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_12.3.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_12.3.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_12.3.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_12.3.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_13 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_14 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_15 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_4.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_4.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_7.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_7.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_7.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_7.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_7.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_7.6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_8.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_8.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_8.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_8.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_8.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_8.6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_8.7 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_8.8 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_8.9 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_eggs_9 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_1.1.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_1.1.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_1.1.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_1.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_11.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_11.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_12.1.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_12.1.10 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_12.1.11 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_12.1.12 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_12.1.13 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_12.1.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_12.1.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_12.1.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_12.1.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_12.1.6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_12.1.7 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_12.1.8 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_12.1.9 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_12.2.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_12.2.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_12.2.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_12.2.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_12.2.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_12.3.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_12.3.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_12.3.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_12.3.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_13 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_14 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_15 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_4.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_4.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_7.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_7.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_7.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_7.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_7.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_7.6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_8.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_8.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_8.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_8.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_8.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_8.6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_8.7 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_8.8 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_8.9 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_ham_9 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_1.1.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_1.1.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_1.1.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_1.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_11.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_11.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_12.1.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_12.1.10 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_12.1.11 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_12.1.12 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_12.1.13 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_12.1.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_12.1.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_12.1.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_12.1.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_12.1.6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_12.1.7 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_12.1.8 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_12.1.9 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_12.2.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_12.2.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_12.2.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_12.2.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_12.2.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_12.3.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_12.3.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_12.3.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_12.3.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_13 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_14 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_15 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_4.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_4.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_7.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_7.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_7.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_7.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_7.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_7.6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_8.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_8.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_8.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_8.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_8.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_8.6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_8.7 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_8.8 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_8.9 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_juice_9 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_1.1.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_1.1.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_1.1.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_1.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_11.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_11.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_12.1.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_12.1.10 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_12.1.11 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_12.1.12 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_12.1.13 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_12.1.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_12.1.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_12.1.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_12.1.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_12.1.6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_12.1.7 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_12.1.8 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_12.1.9 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_12.2.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_12.2.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_12.2.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_12.2.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_12.2.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_12.3.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_12.3.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_12.3.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_12.3.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_13 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_14 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_15 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_4.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_4.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_7.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_7.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_7.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_7.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_7.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_7.6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_8.1 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_8.2 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_8.3 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_8.4 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_8.5 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_8.6 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_8.7 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_8.8 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_8.9 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | volume_milk_9 | series | {8 series tags} | More Prices and Volumes | None |
| tutorials | AZ_omsetning | dataset | {5 set tags
+130 series} |  | None |
| tutorials | a_omsetning_brus | series | {8 series tags} | AZ_omsetning | None |
| tutorials | a_omsetning_kaffe | series | {8 series tags} | AZ_omsetning | None |
| tutorials | a_omsetning_te | series | {8 series tags} | AZ_omsetning | None |
| tutorials | a_omsetning_vin | series | {8 series tags} | AZ_omsetning | None |
| tutorials | a_omsetning_øl | series | {8 series tags} | AZ_omsetning | None |
| tutorials | b_omsetning_brus | series | {8 series tags} | AZ_omsetning | None |
| tutorials | b_omsetning_kaffe | series | {8 series tags} | AZ_omsetning | None |
| tutorials | b_omsetning_te | series | {8 series tags} | AZ_omsetning | None |
| tutorials | b_omsetning_vin | series | {8 series tags} | AZ_omsetning | None |
| tutorials | b_omsetning_øl | series | {8 series tags} | AZ_omsetning | None |
| tutorials | c_omsetning_brus | series | {8 series tags} | AZ_omsetning | None |
| tutorials | c_omsetning_kaffe | series | {8 series tags} | AZ_omsetning | None |
| tutorials | c_omsetning_te | series | {8 series tags} | AZ_omsetning | None |
| tutorials | c_omsetning_vin | series | {8 series tags} | AZ_omsetning | None |
| tutorials | c_omsetning_øl | series | {8 series tags} | AZ_omsetning | None |
| tutorials | d_omsetning_brus | series | {8 series tags} | AZ_omsetning | None |
| tutorials | d_omsetning_kaffe | series | {8 series tags} | AZ_omsetning | None |
| tutorials | d_omsetning_te | series | {8 series tags} | AZ_omsetning | None |
| tutorials | d_omsetning_vin | series | {8 series tags} | AZ_omsetning | None |
| tutorials | d_omsetning_øl | series | {8 series tags} | AZ_omsetning | None |
| tutorials | e_omsetning_brus | series | {8 series tags} | AZ_omsetning | None |
| tutorials | e_omsetning_kaffe | series | {8 series tags} | AZ_omsetning | None |
| tutorials | e_omsetning_te | series | {8 series tags} | AZ_omsetning | None |
| tutorials | e_omsetning_vin | series | {8 series tags} | AZ_omsetning | None |
| tutorials | e_omsetning_øl | series | {8 series tags} | AZ_omsetning | None |
| tutorials | f_omsetning_brus | series | {8 series tags} | AZ_omsetning | None |
| tutorials | f_omsetning_kaffe | series | {8 series tags} | AZ_omsetning | None |
| tutorials | f_omsetning_te | series | {8 series tags} | AZ_omsetning | None |
| tutorials | f_omsetning_vin | series | {8 series tags} | AZ_omsetning | None |
| tutorials | f_omsetning_øl | series | {8 series tags} | AZ_omsetning | None |
| tutorials | g_omsetning_brus | series | {8 series tags} | AZ_omsetning | None |
| tutorials | g_omsetning_kaffe | series | {8 series tags} | AZ_omsetning | None |
| tutorials | g_omsetning_te | series | {8 series tags} | AZ_omsetning | None |
| tutorials | g_omsetning_vin | series | {8 series tags} | AZ_omsetning | None |
| tutorials | g_omsetning_øl | series | {8 series tags} | AZ_omsetning | None |
| tutorials | h_omsetning_brus | series | {8 series tags} | AZ_omsetning | None |
| tutorials | h_omsetning_kaffe | series | {8 series tags} | AZ_omsetning | None |
| tutorials | h_omsetning_te | series | {8 series tags} | AZ_omsetning | None |
| tutorials | h_omsetning_vin | series | {8 series tags} | AZ_omsetning | None |
| tutorials | h_omsetning_øl | series | {8 series tags} | AZ_omsetning | None |
| tutorials | i_omsetning_brus | series | {8 series tags} | AZ_omsetning | None |
| tutorials | i_omsetning_kaffe | series | {8 series tags} | AZ_omsetning | None |
| tutorials | i_omsetning_te | series | {8 series tags} | AZ_omsetning | None |
| tutorials | i_omsetning_vin | series | {8 series tags} | AZ_omsetning | None |
| tutorials | i_omsetning_øl | series | {8 series tags} | AZ_omsetning | None |
| tutorials | j_omsetning_brus | series | {8 series tags} | AZ_omsetning | None |
| tutorials | j_omsetning_kaffe | series | {8 series tags} | AZ_omsetning | None |
| tutorials | j_omsetning_te | series | {8 series tags} | AZ_omsetning | None |
| tutorials | j_omsetning_vin | series | {8 series tags} | AZ_omsetning | None |
| tutorials | j_omsetning_øl | series | {8 series tags} | AZ_omsetning | None |
| tutorials | k_omsetning_brus | series | {8 series tags} | AZ_omsetning | None |
| tutorials | k_omsetning_kaffe | series | {8 series tags} | AZ_omsetning | None |
| tutorials | k_omsetning_te | series | {8 series tags} | AZ_omsetning | None |
| tutorials | k_omsetning_vin | series | {8 series tags} | AZ_omsetning | None |
| tutorials | k_omsetning_øl | series | {8 series tags} | AZ_omsetning | None |
| tutorials | l_omsetning_brus | series | {8 series tags} | AZ_omsetning | None |
| tutorials | l_omsetning_kaffe | series | {8 series tags} | AZ_omsetning | None |
| tutorials | l_omsetning_te | series | {8 series tags} | AZ_omsetning | None |
| tutorials | l_omsetning_vin | series | {8 series tags} | AZ_omsetning | None |
| tutorials | l_omsetning_øl | series | {8 series tags} | AZ_omsetning | None |
| tutorials | m_omsetning_brus | series | {8 series tags} | AZ_omsetning | None |
| tutorials | m_omsetning_kaffe | series | {8 series tags} | AZ_omsetning | None |
| tutorials | m_omsetning_te | series | {8 series tags} | AZ_omsetning | None |
| tutorials | m_omsetning_vin | series | {8 series tags} | AZ_omsetning | None |
| tutorials | m_omsetning_øl | series | {8 series tags} | AZ_omsetning | None |
| tutorials | n_omsetning_brus | series | {8 series tags} | AZ_omsetning | None |
| tutorials | n_omsetning_kaffe | series | {8 series tags} | AZ_omsetning | None |
| tutorials | n_omsetning_te | series | {8 series tags} | AZ_omsetning | None |
| tutorials | n_omsetning_vin | series | {8 series tags} | AZ_omsetning | None |
| tutorials | n_omsetning_øl | series | {8 series tags} | AZ_omsetning | None |
| tutorials | o_omsetning_brus | series | {8 series tags} | AZ_omsetning | None |
| tutorials | o_omsetning_kaffe | series | {8 series tags} | AZ_omsetning | None |
| tutorials | o_omsetning_te | series | {8 series tags} | AZ_omsetning | None |
| tutorials | o_omsetning_vin | series | {8 series tags} | AZ_omsetning | None |
| tutorials | o_omsetning_øl | series | {8 series tags} | AZ_omsetning | None |
| tutorials | p_omsetning_brus | series | {8 series tags} | AZ_omsetning | None |
| tutorials | p_omsetning_kaffe | series | {8 series tags} | AZ_omsetning | None |
| tutorials | p_omsetning_te | series | {8 series tags} | AZ_omsetning | None |
| tutorials | p_omsetning_vin | series | {8 series tags} | AZ_omsetning | None |
| tutorials | p_omsetning_øl | series | {8 series tags} | AZ_omsetning | None |
| tutorials | q_omsetning_brus | series | {8 series tags} | AZ_omsetning | None |
| tutorials | q_omsetning_kaffe | series | {8 series tags} | AZ_omsetning | None |
| tutorials | q_omsetning_te | series | {8 series tags} | AZ_omsetning | None |
| tutorials | q_omsetning_vin | series | {8 series tags} | AZ_omsetning | None |
| tutorials | q_omsetning_øl | series | {8 series tags} | AZ_omsetning | None |
| tutorials | r_omsetning_brus | series | {8 series tags} | AZ_omsetning | None |
| tutorials | r_omsetning_kaffe | series | {8 series tags} | AZ_omsetning | None |
| tutorials | r_omsetning_te | series | {8 series tags} | AZ_omsetning | None |
| tutorials | r_omsetning_vin | series | {8 series tags} | AZ_omsetning | None |
| tutorials | r_omsetning_øl | series | {8 series tags} | AZ_omsetning | None |
| tutorials | s_omsetning_brus | series | {8 series tags} | AZ_omsetning | None |
| tutorials | s_omsetning_kaffe | series | {8 series tags} | AZ_omsetning | None |
| tutorials | s_omsetning_te | series | {8 series tags} | AZ_omsetning | None |
| tutorials | s_omsetning_vin | series | {8 series tags} | AZ_omsetning | None |
| tutorials | s_omsetning_øl | series | {8 series tags} | AZ_omsetning | None |
| tutorials | t_omsetning_brus | series | {8 series tags} | AZ_omsetning | None |
| tutorials | t_omsetning_kaffe | series | {8 series tags} | AZ_omsetning | None |
| tutorials | t_omsetning_te | series | {8 series tags} | AZ_omsetning | None |
| tutorials | t_omsetning_vin | series | {8 series tags} | AZ_omsetning | None |
| tutorials | t_omsetning_øl | series | {8 series tags} | AZ_omsetning | None |
| tutorials | u_omsetning_brus | series | {8 series tags} | AZ_omsetning | None |
| tutorials | u_omsetning_kaffe | series | {8 series tags} | AZ_omsetning | None |
| tutorials | u_omsetning_te | series | {8 series tags} | AZ_omsetning | None |
| tutorials | u_omsetning_vin | series | {8 series tags} | AZ_omsetning | None |
| tutorials | u_omsetning_øl | series | {8 series tags} | AZ_omsetning | None |
| tutorials | v_omsetning_brus | series | {8 series tags} | AZ_omsetning | None |
| tutorials | v_omsetning_kaffe | series | {8 series tags} | AZ_omsetning | None |
| tutorials | v_omsetning_te | series | {8 series tags} | AZ_omsetning | None |
| tutorials | v_omsetning_vin | series | {8 series tags} | AZ_omsetning | None |
| tutorials | v_omsetning_øl | series | {8 series tags} | AZ_omsetning | None |
| tutorials | w_omsetning_brus | series | {8 series tags} | AZ_omsetning | None |
| tutorials | w_omsetning_kaffe | series | {8 series tags} | AZ_omsetning | None |
| tutorials | w_omsetning_te | series | {8 series tags} | AZ_omsetning | None |
| tutorials | w_omsetning_vin | series | {8 series tags} | AZ_omsetning | None |
| tutorials | w_omsetning_øl | series | {8 series tags} | AZ_omsetning | None |
| tutorials | x_omsetning_brus | series | {8 series tags} | AZ_omsetning | None |
| tutorials | x_omsetning_kaffe | series | {8 series tags} | AZ_omsetning | None |
| tutorials | x_omsetning_te | series | {8 series tags} | AZ_omsetning | None |
| tutorials | x_omsetning_vin | series | {8 series tags} | AZ_omsetning | None |
| tutorials | x_omsetning_øl | series | {8 series tags} | AZ_omsetning | None |
| tutorials | y_omsetning_brus | series | {8 series tags} | AZ_omsetning | None |
| tutorials | y_omsetning_kaffe | series | {8 series tags} | AZ_omsetning | None |
| tutorials | y_omsetning_te | series | {8 series tags} | AZ_omsetning | None |
| tutorials | y_omsetning_vin | series | {8 series tags} | AZ_omsetning | None |
| tutorials | y_omsetning_øl | series | {8 series tags} | AZ_omsetning | None |
| tutorials | z_omsetning_brus | series | {8 series tags} | AZ_omsetning | None |
| tutorials | z_omsetning_kaffe | series | {8 series tags} | AZ_omsetning | None |
| tutorials | z_omsetning_te | series | {8 series tags} | AZ_omsetning | None |
| tutorials | z_omsetning_vin | series | {8 series tags} | AZ_omsetning | None |
| tutorials | z_omsetning_øl | series | {8 series tags} | AZ_omsetning | None |
| tutorials | AZ_drinks | dataset | {4 set tags
+2080 series} |  | None |
| tutorials | a_price_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_price_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_price_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_price_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_price_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_price_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_price_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_price_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_price_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_price_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_price_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_price_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_price_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_price_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_price_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_price_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_price_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_price_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_price_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_price_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_price_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_price_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_price_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_price_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_price_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_price_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_price_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_price_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_price_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_price_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_price_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_price_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_price_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_price_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_price_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_price_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_price_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_price_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_price_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_price_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_volume_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_volume_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_volume_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_volume_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_volume_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_volume_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_volume_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_volume_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_volume_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_volume_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_volume_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_volume_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_volume_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_volume_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_volume_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_volume_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_volume_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_volume_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_volume_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_volume_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_volume_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_volume_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_volume_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_volume_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_volume_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_volume_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_volume_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_volume_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_volume_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_volume_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_volume_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_volume_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_volume_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_volume_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_volume_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_volume_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_volume_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_volume_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_volume_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | a_volume_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_price_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_price_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_price_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_price_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_price_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_price_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_price_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_price_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_price_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_price_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_price_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_price_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_price_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_price_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_price_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_price_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_price_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_price_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_price_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_price_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_price_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_price_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_price_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_price_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_price_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_price_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_price_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_price_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_price_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_price_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_price_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_price_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_price_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_price_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_price_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_price_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_price_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_price_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_price_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_price_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_volume_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_volume_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_volume_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_volume_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_volume_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_volume_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_volume_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_volume_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_volume_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_volume_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_volume_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_volume_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_volume_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_volume_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_volume_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_volume_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_volume_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_volume_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_volume_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_volume_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_volume_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_volume_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_volume_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_volume_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_volume_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_volume_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_volume_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_volume_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_volume_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_volume_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_volume_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_volume_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_volume_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_volume_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_volume_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_volume_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_volume_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_volume_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_volume_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | b_volume_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_price_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_price_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_price_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_price_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_price_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_price_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_price_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_price_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_price_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_price_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_price_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_price_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_price_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_price_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_price_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_price_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_price_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_price_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_price_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_price_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_price_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_price_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_price_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_price_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_price_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_price_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_price_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_price_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_price_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_price_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_price_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_price_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_price_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_price_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_price_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_price_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_price_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_price_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_price_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_price_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_volume_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_volume_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_volume_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_volume_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_volume_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_volume_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_volume_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_volume_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_volume_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_volume_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_volume_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_volume_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_volume_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_volume_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_volume_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_volume_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_volume_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_volume_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_volume_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_volume_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_volume_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_volume_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_volume_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_volume_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_volume_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_volume_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_volume_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_volume_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_volume_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_volume_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_volume_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_volume_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_volume_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_volume_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_volume_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_volume_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_volume_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_volume_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_volume_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | c_volume_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_price_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_price_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_price_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_price_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_price_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_price_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_price_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_price_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_price_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_price_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_price_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_price_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_price_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_price_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_price_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_price_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_price_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_price_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_price_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_price_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_price_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_price_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_price_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_price_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_price_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_price_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_price_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_price_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_price_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_price_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_price_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_price_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_price_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_price_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_price_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_price_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_price_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_price_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_price_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_price_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_volume_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_volume_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_volume_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_volume_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_volume_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_volume_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_volume_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_volume_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_volume_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_volume_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_volume_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_volume_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_volume_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_volume_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_volume_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_volume_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_volume_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_volume_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_volume_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_volume_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_volume_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_volume_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_volume_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_volume_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_volume_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_volume_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_volume_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_volume_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_volume_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_volume_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_volume_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_volume_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_volume_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_volume_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_volume_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_volume_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_volume_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_volume_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_volume_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | d_volume_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_price_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_price_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_price_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_price_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_price_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_price_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_price_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_price_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_price_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_price_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_price_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_price_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_price_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_price_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_price_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_price_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_price_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_price_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_price_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_price_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_price_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_price_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_price_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_price_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_price_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_price_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_price_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_price_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_price_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_price_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_price_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_price_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_price_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_price_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_price_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_price_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_price_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_price_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_price_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_price_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_volume_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_volume_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_volume_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_volume_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_volume_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_volume_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_volume_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_volume_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_volume_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_volume_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_volume_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_volume_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_volume_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_volume_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_volume_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_volume_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_volume_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_volume_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_volume_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_volume_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_volume_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_volume_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_volume_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_volume_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_volume_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_volume_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_volume_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_volume_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_volume_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_volume_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_volume_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_volume_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_volume_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_volume_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_volume_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_volume_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_volume_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_volume_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_volume_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | e_volume_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_price_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_price_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_price_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_price_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_price_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_price_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_price_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_price_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_price_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_price_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_price_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_price_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_price_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_price_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_price_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_price_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_price_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_price_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_price_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_price_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_price_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_price_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_price_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_price_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_price_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_price_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_price_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_price_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_price_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_price_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_price_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_price_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_price_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_price_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_price_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_price_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_price_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_price_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_price_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_price_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_volume_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_volume_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_volume_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_volume_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_volume_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_volume_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_volume_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_volume_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_volume_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_volume_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_volume_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_volume_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_volume_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_volume_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_volume_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_volume_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_volume_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_volume_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_volume_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_volume_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_volume_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_volume_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_volume_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_volume_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_volume_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_volume_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_volume_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_volume_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_volume_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_volume_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_volume_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_volume_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_volume_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_volume_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_volume_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_volume_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_volume_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_volume_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_volume_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | f_volume_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_price_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_price_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_price_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_price_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_price_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_price_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_price_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_price_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_price_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_price_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_price_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_price_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_price_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_price_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_price_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_price_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_price_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_price_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_price_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_price_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_price_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_price_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_price_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_price_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_price_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_price_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_price_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_price_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_price_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_price_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_price_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_price_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_price_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_price_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_price_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_price_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_price_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_price_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_price_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_price_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_volume_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_volume_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_volume_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_volume_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_volume_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_volume_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_volume_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_volume_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_volume_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_volume_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_volume_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_volume_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_volume_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_volume_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_volume_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_volume_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_volume_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_volume_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_volume_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_volume_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_volume_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_volume_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_volume_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_volume_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_volume_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_volume_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_volume_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_volume_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_volume_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_volume_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_volume_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_volume_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_volume_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_volume_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_volume_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_volume_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_volume_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_volume_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_volume_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | g_volume_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_price_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_price_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_price_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_price_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_price_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_price_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_price_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_price_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_price_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_price_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_price_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_price_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_price_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_price_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_price_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_price_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_price_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_price_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_price_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_price_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_price_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_price_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_price_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_price_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_price_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_price_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_price_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_price_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_price_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_price_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_price_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_price_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_price_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_price_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_price_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_price_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_price_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_price_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_price_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_price_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_volume_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_volume_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_volume_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_volume_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_volume_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_volume_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_volume_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_volume_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_volume_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_volume_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_volume_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_volume_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_volume_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_volume_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_volume_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_volume_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_volume_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_volume_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_volume_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_volume_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_volume_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_volume_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_volume_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_volume_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_volume_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_volume_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_volume_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_volume_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_volume_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_volume_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_volume_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_volume_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_volume_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_volume_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_volume_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_volume_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_volume_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_volume_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_volume_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | h_volume_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_price_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_price_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_price_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_price_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_price_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_price_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_price_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_price_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_price_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_price_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_price_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_price_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_price_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_price_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_price_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_price_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_price_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_price_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_price_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_price_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_price_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_price_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_price_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_price_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_price_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_price_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_price_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_price_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_price_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_price_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_price_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_price_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_price_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_price_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_price_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_price_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_price_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_price_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_price_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_price_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_volume_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_volume_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_volume_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_volume_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_volume_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_volume_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_volume_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_volume_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_volume_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_volume_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_volume_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_volume_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_volume_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_volume_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_volume_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_volume_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_volume_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_volume_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_volume_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_volume_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_volume_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_volume_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_volume_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_volume_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_volume_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_volume_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_volume_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_volume_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_volume_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_volume_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_volume_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_volume_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_volume_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_volume_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_volume_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_volume_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_volume_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_volume_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_volume_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | i_volume_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_price_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_price_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_price_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_price_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_price_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_price_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_price_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_price_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_price_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_price_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_price_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_price_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_price_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_price_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_price_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_price_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_price_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_price_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_price_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_price_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_price_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_price_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_price_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_price_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_price_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_price_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_price_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_price_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_price_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_price_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_price_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_price_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_price_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_price_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_price_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_price_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_price_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_price_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_price_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_price_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_volume_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_volume_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_volume_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_volume_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_volume_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_volume_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_volume_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_volume_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_volume_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_volume_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_volume_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_volume_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_volume_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_volume_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_volume_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_volume_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_volume_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_volume_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_volume_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_volume_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_volume_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_volume_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_volume_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_volume_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_volume_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_volume_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_volume_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_volume_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_volume_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_volume_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_volume_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_volume_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_volume_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_volume_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_volume_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_volume_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_volume_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_volume_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_volume_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | j_volume_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_price_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_price_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_price_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_price_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_price_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_price_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_price_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_price_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_price_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_price_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_price_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_price_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_price_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_price_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_price_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_price_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_price_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_price_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_price_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_price_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_price_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_price_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_price_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_price_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_price_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_price_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_price_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_price_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_price_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_price_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_price_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_price_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_price_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_price_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_price_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_price_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_price_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_price_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_price_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_price_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_volume_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_volume_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_volume_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_volume_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_volume_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_volume_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_volume_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_volume_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_volume_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_volume_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_volume_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_volume_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_volume_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_volume_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_volume_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_volume_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_volume_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_volume_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_volume_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_volume_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_volume_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_volume_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_volume_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_volume_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_volume_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_volume_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_volume_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_volume_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_volume_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_volume_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_volume_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_volume_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_volume_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_volume_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_volume_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_volume_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_volume_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_volume_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_volume_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | k_volume_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_price_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_price_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_price_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_price_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_price_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_price_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_price_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_price_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_price_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_price_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_price_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_price_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_price_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_price_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_price_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_price_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_price_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_price_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_price_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_price_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_price_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_price_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_price_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_price_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_price_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_price_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_price_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_price_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_price_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_price_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_price_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_price_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_price_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_price_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_price_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_price_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_price_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_price_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_price_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_price_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_volume_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_volume_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_volume_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_volume_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_volume_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_volume_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_volume_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_volume_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_volume_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_volume_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_volume_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_volume_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_volume_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_volume_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_volume_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_volume_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_volume_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_volume_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_volume_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_volume_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_volume_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_volume_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_volume_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_volume_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_volume_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_volume_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_volume_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_volume_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_volume_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_volume_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_volume_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_volume_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_volume_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_volume_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_volume_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_volume_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_volume_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_volume_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_volume_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | l_volume_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_price_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_price_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_price_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_price_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_price_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_price_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_price_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_price_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_price_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_price_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_price_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_price_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_price_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_price_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_price_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_price_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_price_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_price_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_price_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_price_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_price_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_price_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_price_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_price_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_price_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_price_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_price_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_price_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_price_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_price_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_price_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_price_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_price_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_price_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_price_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_price_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_price_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_price_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_price_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_price_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_volume_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_volume_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_volume_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_volume_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_volume_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_volume_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_volume_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_volume_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_volume_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_volume_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_volume_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_volume_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_volume_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_volume_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_volume_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_volume_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_volume_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_volume_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_volume_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_volume_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_volume_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_volume_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_volume_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_volume_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_volume_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_volume_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_volume_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_volume_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_volume_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_volume_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_volume_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_volume_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_volume_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_volume_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_volume_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_volume_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_volume_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_volume_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_volume_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | m_volume_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_price_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_price_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_price_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_price_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_price_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_price_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_price_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_price_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_price_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_price_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_price_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_price_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_price_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_price_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_price_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_price_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_price_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_price_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_price_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_price_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_price_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_price_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_price_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_price_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_price_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_price_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_price_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_price_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_price_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_price_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_price_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_price_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_price_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_price_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_price_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_price_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_price_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_price_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_price_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_price_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_volume_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_volume_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_volume_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_volume_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_volume_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_volume_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_volume_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_volume_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_volume_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_volume_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_volume_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_volume_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_volume_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_volume_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_volume_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_volume_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_volume_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_volume_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_volume_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_volume_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_volume_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_volume_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_volume_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_volume_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_volume_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_volume_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_volume_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_volume_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_volume_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_volume_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_volume_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_volume_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_volume_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_volume_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_volume_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_volume_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_volume_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_volume_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_volume_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | n_volume_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_price_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_price_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_price_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_price_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_price_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_price_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_price_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_price_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_price_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_price_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_price_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_price_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_price_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_price_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_price_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_price_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_price_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_price_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_price_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_price_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_price_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_price_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_price_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_price_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_price_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_price_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_price_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_price_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_price_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_price_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_price_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_price_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_price_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_price_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_price_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_price_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_price_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_price_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_price_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_price_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_volume_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_volume_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_volume_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_volume_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_volume_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_volume_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_volume_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_volume_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_volume_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_volume_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_volume_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_volume_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_volume_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_volume_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_volume_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_volume_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_volume_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_volume_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_volume_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_volume_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_volume_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_volume_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_volume_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_volume_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_volume_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_volume_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_volume_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_volume_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_volume_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_volume_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_volume_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_volume_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_volume_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_volume_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_volume_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_volume_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_volume_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_volume_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_volume_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | o_volume_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_price_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_price_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_price_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_price_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_price_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_price_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_price_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_price_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_price_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_price_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_price_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_price_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_price_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_price_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_price_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_price_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_price_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_price_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_price_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_price_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_price_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_price_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_price_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_price_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_price_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_price_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_price_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_price_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_price_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_price_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_price_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_price_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_price_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_price_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_price_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_price_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_price_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_price_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_price_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_price_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_volume_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_volume_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_volume_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_volume_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_volume_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_volume_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_volume_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_volume_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_volume_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_volume_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_volume_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_volume_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_volume_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_volume_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_volume_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_volume_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_volume_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_volume_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_volume_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_volume_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_volume_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_volume_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_volume_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_volume_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_volume_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_volume_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_volume_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_volume_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_volume_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_volume_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_volume_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_volume_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_volume_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_volume_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_volume_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_volume_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_volume_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_volume_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_volume_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | p_volume_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_price_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_price_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_price_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_price_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_price_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_price_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_price_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_price_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_price_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_price_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_price_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_price_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_price_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_price_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_price_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_price_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_price_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_price_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_price_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_price_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_price_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_price_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_price_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_price_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_price_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_price_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_price_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_price_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_price_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_price_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_price_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_price_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_price_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_price_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_price_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_price_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_price_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_price_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_price_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_price_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_volume_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_volume_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_volume_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_volume_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_volume_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_volume_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_volume_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_volume_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_volume_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_volume_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_volume_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_volume_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_volume_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_volume_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_volume_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_volume_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_volume_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_volume_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_volume_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_volume_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_volume_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_volume_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_volume_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_volume_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_volume_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_volume_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_volume_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_volume_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_volume_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_volume_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_volume_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_volume_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_volume_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_volume_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_volume_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_volume_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_volume_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_volume_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_volume_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | q_volume_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_price_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_price_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_price_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_price_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_price_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_price_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_price_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_price_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_price_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_price_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_price_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_price_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_price_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_price_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_price_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_price_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_price_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_price_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_price_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_price_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_price_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_price_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_price_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_price_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_price_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_price_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_price_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_price_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_price_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_price_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_price_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_price_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_price_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_price_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_price_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_price_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_price_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_price_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_price_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_price_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_volume_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_volume_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_volume_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_volume_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_volume_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_volume_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_volume_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_volume_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_volume_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_volume_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_volume_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_volume_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_volume_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_volume_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_volume_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_volume_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_volume_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_volume_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_volume_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_volume_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_volume_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_volume_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_volume_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_volume_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_volume_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_volume_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_volume_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_volume_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_volume_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_volume_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_volume_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_volume_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_volume_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_volume_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_volume_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_volume_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_volume_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_volume_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_volume_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | r_volume_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_price_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_price_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_price_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_price_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_price_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_price_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_price_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_price_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_price_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_price_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_price_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_price_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_price_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_price_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_price_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_price_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_price_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_price_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_price_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_price_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_price_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_price_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_price_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_price_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_price_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_price_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_price_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_price_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_price_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_price_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_price_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_price_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_price_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_price_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_price_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_price_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_price_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_price_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_price_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_price_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_volume_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_volume_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_volume_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_volume_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_volume_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_volume_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_volume_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_volume_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_volume_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_volume_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_volume_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_volume_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_volume_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_volume_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_volume_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_volume_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_volume_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_volume_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_volume_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_volume_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_volume_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_volume_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_volume_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_volume_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_volume_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_volume_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_volume_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_volume_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_volume_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_volume_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_volume_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_volume_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_volume_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_volume_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_volume_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_volume_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_volume_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_volume_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_volume_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | s_volume_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_price_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_price_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_price_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_price_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_price_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_price_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_price_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_price_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_price_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_price_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_price_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_price_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_price_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_price_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_price_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_price_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_price_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_price_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_price_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_price_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_price_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_price_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_price_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_price_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_price_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_price_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_price_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_price_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_price_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_price_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_price_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_price_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_price_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_price_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_price_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_price_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_price_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_price_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_price_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_price_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_volume_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_volume_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_volume_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_volume_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_volume_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_volume_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_volume_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_volume_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_volume_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_volume_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_volume_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_volume_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_volume_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_volume_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_volume_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_volume_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_volume_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_volume_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_volume_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_volume_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_volume_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_volume_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_volume_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_volume_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_volume_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_volume_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_volume_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_volume_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_volume_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_volume_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_volume_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_volume_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_volume_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_volume_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_volume_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_volume_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_volume_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_volume_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_volume_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | t_volume_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_price_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_price_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_price_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_price_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_price_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_price_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_price_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_price_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_price_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_price_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_price_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_price_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_price_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_price_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_price_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_price_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_price_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_price_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_price_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_price_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_price_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_price_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_price_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_price_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_price_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_price_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_price_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_price_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_price_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_price_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_price_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_price_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_price_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_price_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_price_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_price_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_price_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_price_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_price_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_price_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_volume_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_volume_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_volume_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_volume_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_volume_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_volume_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_volume_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_volume_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_volume_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_volume_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_volume_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_volume_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_volume_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_volume_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_volume_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_volume_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_volume_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_volume_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_volume_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_volume_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_volume_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_volume_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_volume_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_volume_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_volume_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_volume_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_volume_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_volume_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_volume_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_volume_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_volume_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_volume_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_volume_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_volume_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_volume_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_volume_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_volume_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_volume_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_volume_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | u_volume_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_price_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_price_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_price_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_price_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_price_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_price_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_price_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_price_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_price_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_price_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_price_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_price_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_price_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_price_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_price_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_price_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_price_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_price_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_price_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_price_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_price_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_price_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_price_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_price_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_price_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_price_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_price_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_price_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_price_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_price_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_price_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_price_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_price_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_price_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_price_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_price_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_price_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_price_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_price_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_price_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_volume_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_volume_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_volume_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_volume_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_volume_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_volume_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_volume_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_volume_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_volume_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_volume_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_volume_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_volume_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_volume_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_volume_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_volume_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_volume_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_volume_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_volume_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_volume_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_volume_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_volume_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_volume_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_volume_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_volume_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_volume_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_volume_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_volume_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_volume_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_volume_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_volume_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_volume_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_volume_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_volume_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_volume_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_volume_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_volume_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_volume_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_volume_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_volume_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | v_volume_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_price_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_price_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_price_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_price_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_price_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_price_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_price_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_price_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_price_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_price_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_price_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_price_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_price_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_price_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_price_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_price_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_price_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_price_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_price_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_price_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_price_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_price_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_price_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_price_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_price_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_price_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_price_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_price_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_price_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_price_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_price_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_price_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_price_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_price_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_price_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_price_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_price_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_price_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_price_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_price_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_volume_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_volume_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_volume_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_volume_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_volume_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_volume_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_volume_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_volume_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_volume_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_volume_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_volume_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_volume_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_volume_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_volume_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_volume_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_volume_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_volume_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_volume_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_volume_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_volume_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_volume_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_volume_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_volume_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_volume_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_volume_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_volume_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_volume_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_volume_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_volume_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_volume_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_volume_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_volume_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_volume_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_volume_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_volume_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_volume_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_volume_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_volume_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_volume_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | w_volume_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_price_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_price_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_price_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_price_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_price_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_price_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_price_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_price_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_price_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_price_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_price_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_price_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_price_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_price_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_price_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_price_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_price_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_price_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_price_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_price_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_price_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_price_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_price_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_price_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_price_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_price_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_price_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_price_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_price_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_price_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_price_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_price_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_price_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_price_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_price_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_price_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_price_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_price_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_price_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_price_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_volume_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_volume_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_volume_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_volume_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_volume_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_volume_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_volume_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_volume_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_volume_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_volume_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_volume_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_volume_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_volume_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_volume_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_volume_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_volume_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_volume_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_volume_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_volume_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_volume_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_volume_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_volume_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_volume_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_volume_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_volume_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_volume_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_volume_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_volume_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_volume_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_volume_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_volume_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_volume_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_volume_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_volume_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_volume_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_volume_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_volume_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_volume_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_volume_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | x_volume_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_price_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_price_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_price_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_price_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_price_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_price_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_price_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_price_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_price_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_price_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_price_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_price_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_price_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_price_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_price_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_price_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_price_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_price_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_price_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_price_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_price_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_price_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_price_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_price_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_price_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_price_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_price_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_price_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_price_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_price_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_price_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_price_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_price_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_price_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_price_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_price_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_price_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_price_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_price_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_price_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_volume_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_volume_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_volume_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_volume_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_volume_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_volume_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_volume_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_volume_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_volume_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_volume_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_volume_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_volume_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_volume_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_volume_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_volume_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_volume_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_volume_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_volume_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_volume_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_volume_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_volume_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_volume_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_volume_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_volume_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_volume_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_volume_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_volume_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_volume_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_volume_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_volume_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_volume_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_volume_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_volume_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_volume_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_volume_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_volume_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_volume_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_volume_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_volume_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | y_volume_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_price_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_price_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_price_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_price_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_price_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_price_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_price_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_price_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_price_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_price_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_price_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_price_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_price_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_price_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_price_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_price_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_price_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_price_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_price_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_price_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_price_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_price_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_price_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_price_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_price_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_price_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_price_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_price_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_price_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_price_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_price_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_price_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_price_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_price_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_price_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_price_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_price_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_price_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_price_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_price_wine_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_volume_beer_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_volume_beer_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_volume_beer_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_volume_beer_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_volume_beer_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_volume_beer_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_volume_beer_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_volume_beer_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_volume_coffee_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_volume_coffee_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_volume_coffee_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_volume_coffee_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_volume_coffee_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_volume_coffee_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_volume_coffee_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_volume_coffee_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_volume_soft-drinks_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_volume_soft-drinks_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_volume_soft-drinks_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_volume_soft-drinks_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_volume_soft-drinks_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_volume_soft-drinks_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_volume_soft-drinks_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_volume_soft-drinks_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_volume_tea_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_volume_tea_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_volume_tea_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_volume_tea_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_volume_tea_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_volume_tea_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_volume_tea_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_volume_tea_W | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_volume_wine_E | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_volume_wine_N | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_volume_wine_NE | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_volume_wine_NW | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_volume_wine_S | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_volume_wine_SE | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_volume_wine_SW | series | {9 series tags} | AZ_drinks | None |
| tutorials | z_volume_wine_W | series | {9 series tags} | AZ_drinks | None |

Filtering: column selection
----------------------------

```python {.marimo}
xyz = Dataset('Sample Data')
```

When initialising a variable for an existing `Dataset`, we automatically retrieve the previously stored metadata.

```python {.marimo}
xyz.tags
```

<!-- @output:NCOB -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">{&#x27;name&#x27;: &#x27;Sample Data&#x27;,
 &#x27;product group&#x27;: &#x27;essential&#x27;,
 &#x27;repository&#x27;: &#x27;tutorials&#x27;,
 &#x27;series&#x27;: {&#x27;x&#x27;: {&#x27;area&#x27;: &#x27;x&#x27;,
                  &#x27;dataset&#x27;: &#x27;Sample Data&#x27;,
                  &#x27;name&#x27;: &#x27;x&#x27;,
                  &#x27;product&#x27;: &#x27;coffee&#x27;,
                  &#x27;product group&#x27;: &#x27;essential&#x27;,
                  &#x27;repository&#x27;: &#x27;tutorials&#x27;,
                  &#x27;temporality&#x27;: &#x27;AT&#x27;,
                  &#x27;variable&#x27;: &#x27;price&#x27;,
                  &#x27;versioning&#x27;: &#x27;AS_OF&#x27;},
            &#x27;y&#x27;: {&#x27;area&#x27;: &#x27;y&#x27;,
                  &#x27;dataset&#x27;: &#x27;Sample Data&#x27;,
                  &#x27;name&#x27;: &#x27;y&#x27;,
                  &#x27;product&#x27;: &#x27;crispbread&#x27;,
                  &#x27;product group&#x27;: &#x27;essential&#x27;,
                  &#x27;repository&#x27;: &#x27;tutorials&#x27;,
                  &#x27;temporality&#x27;: &#x27;AT&#x27;,
                  &#x27;variable&#x27;: &#x27;price&#x27;,
                  &#x27;versioning&#x27;: &#x27;AS_OF&#x27;},
            &#x27;z&#x27;: {&#x27;area&#x27;: &#x27;z&#x27;,
                  &#x27;dataset&#x27;: &#x27;Sample Data&#x27;,
                  &#x27;name&#x27;: &#x27;z&#x27;,
                  &#x27;product&#x27;: &#x27;brown cheese&#x27;,
                  &#x27;product group&#x27;: &#x27;essential&#x27;,
                  &#x27;repository&#x27;: &#x27;tutorials&#x27;,
                  &#x27;temporality&#x27;: &#x27;AT&#x27;,
                  &#x27;variable&#x27;: &#x27;price&#x27;,
                  &#x27;versioning&#x27;: &#x27;AS_OF&#x27;}},
 &#x27;temporality&#x27;: &#x27;AT&#x27;,
 &#x27;variable&#x27;: &#x27;price&#x27;,
 &#x27;versioning&#x27;: &#x27;AS_OF&#x27;}</pre>

`Dataset.select()` picks columns.
Column selection may be done by names, simple patterns or regexes.

```python {.marimo}
xyz['x','y'].plot()
```

<!-- @output:TRpd -->

![png](meta-search-and-filtering_assets/figure-1.png)

Or by metadata tags:

```python {.marimo}
xyz[{'area':'z'}].plot()
```

<!-- @output:dNNg -->

![png](meta-search-and-filtering_assets/figure-2.png)

Selection by tags becomes very powerful for bigger datasets.

```python {.marimo}
bigger_data = mock_interval_data_from_file_or_query(start='2025-01-01', end='2025-06-01')
```

```python {.marimo}
from ssb_timeseries.types import SeriesType
```

```python {.marimo}
az = Dataset(
    name = 'AZ_drinks',
    data_type = SeriesType('NONE', 'FROM_TO'),
    data = bigger_data,
    attributes=['store','variable','product', 'region'],
)
az.save()
```

<!-- @output:dGlV -->

The `az` set has 2080 series. Let us zoom in:

```python {.marimo}
criteria = {
    'region':['NE','NW'],
    'variable': 'price',
    'store': 'q',}
az_selection = az[criteria]
```

THe criteria reads like

```python {.marimo}
az_selection.series
```

<!-- @output:yOPj -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">&#91;&#x27;q_price_beer_NE&#x27;,
 &#x27;q_price_beer_NW&#x27;,
 &#x27;q_price_coffee_NE&#x27;,
 &#x27;q_price_coffee_NW&#x27;,
 &#x27;q_price_soft-drinks_NE&#x27;,
 &#x27;q_price_soft-drinks_NW&#x27;,
 &#x27;q_price_tea_NE&#x27;,
 &#x27;q_price_tea_NW&#x27;,
 &#x27;q_price_wine_NE&#x27;,
 &#x27;q_price_wine_NW&#x27;&#93;</pre>

...

<!-- @output:LJZf -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">&#91;32m.&#91;0m&#91;32m                                                                        &#91;100%&#93;&#91;0m
=================================== Overview ===================================
Passed Tests:
&#91;1m&#91;32m&#91;22m✓&#91;0m&#91;0m notebooks/meta-search-and-filtering.py::test_true

Summary:
Total: 1, Passed: 1, Failed: 0, Errors: 0, Skipped: 0
</pre>
