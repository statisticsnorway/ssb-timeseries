---
title: Meta Search And Filtering
marimo-version: 0.24.2
---

```python {.marimo}
from mdtools import catalog_item_list_to_df
```

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

The catalog searches below need data to find.
This guide creates the two datasets it queries, so it can be read on its own.

```python {.marimo}
# Aliased to avoid clashing with the SeriesType and create_df definitions
# further down, which marimo rejects as multiple definitions.
import ssb_timeseries.sample_data as _sample_data
import ssb_timeseries.types as _types

searchable_data = Dataset(
    name = 'PQR',
    data_type = _types.SeriesType(_types.Versioning.NONE, _types.Temporality.AT),
    data = _sample_data.create_df(['p','q','r'], start_date='2020-01-01', end_date='2025-06-01', freq='D'),
)
searchable_data.tag_series('p', tags={'product': 'coffee'})
searchable_data.tag_series('q', tags={'product': 'crispbread'})
searchable_data.tag_series('r', tags={'product': 'brown cheese'})
searchable_data.save()

xyz_data = Dataset(
    name = 'XYZ',
    data_type = _types.SeriesType(_types.Versioning.NONE, _types.Temporality.AT),
    data = _sample_data.xyz_at(),
)
xyz_data.save()
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

<!-- @output:Hstk -->

| repository_name | object_name | object_type | object_tags |
| --- | --- | --- | --- |
| tutorials | A Sample Dataset | dataset | {4 set tags + 3 series} |
| tutorials | AZ_drikkevarer | dataset | {4 set tags + 260 series} |
| tutorials | AZ_drinks | dataset | {4 set tags + 2080 series} |
| tutorials | AZ_omsetning | dataset | {5 set tags + 130 series} |
| tutorials | More Prices and Volumes | dataset | {4 set tags + 636 series} |
| tutorials | POPU06 | dataset | {4 set tags + 5 series} |
| tutorials | PQR | dataset | {8 set tags + 3 series} |
| tutorials | Prices and Volumes | dataset | {4 set tags + 12 series} |
| tutorials | XYZ | dataset | {4 set tags + 3 series} |

Or the unique repositories:

```python {.marimo}
repositories = {s.repository_name for s in all_sets}
repositories
```

<!-- @output:iLit -->

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

<!-- @output:TqIu -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">3141</pre>

Or, to search for all `Datasets` that contain `Series` tagged with `{'product': 'coffee'}`, just return the `parent` for the `.series` search:

```python {.marimo}
sets_that_have_series_tagged_with = {result.parent for result in timeseries_catalog.series(tags={'product': 'coffee'})}
```

```python {.marimo}
[s.object_name for s in all_sets]
```

<!-- @output:ulZA -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">&#91;&#x27;A Sample Dataset&#x27;,
 &#x27;AZ_drikkevarer&#x27;,
 &#x27;AZ_drinks&#x27;,
 &#x27;AZ_omsetning&#x27;,
 &#x27;More Prices and Volumes&#x27;,
 &#x27;POPU06&#x27;,
 &#x27;PQR&#x27;,
 &#x27;Prices and Volumes&#x27;,
 &#x27;XYZ&#x27;&#93;</pre>

To get a global tag dictionary:

```python {.marimo}
all_sets_tag_dict = {s.object_name: s.object_tags for s in all_sets}
all_sets_tag_dict['PQR']
```

<!-- @output:Pvdt -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">{&#x27;name&#x27;: &#x27;PQR&#x27;,
 &#x27;product group&#x27;: &#x27;essential&#x27;,
 &#x27;repository&#x27;: &#x27;tutorials&#x27;,
 &#x27;series&#x27;: {&#x27;p&#x27;: {&#x27;dataset&#x27;: &#x27;PQR&#x27;,
                  &#x27;name&#x27;: &#x27;p&#x27;,
                  &#x27;product&#x27;: &#x27;coffee&#x27;,
                  &#x27;product group&#x27;: &#x27;essential&#x27;,
                  &#x27;repository&#x27;: &#x27;tutorials&#x27;,
                  &#x27;temporality&#x27;: &#x27;AT&#x27;,
                  &#x27;vare&#x27;: &#x27;kaffe&#x27;,
                  &#x27;varegruppe&#x27;: &#x27;nødvendigheter&#x27;,
                  &#x27;variabel&#x27;: &#x27;pris&#x27;,
                  &#x27;variable&#x27;: &#x27;price&#x27;,
                  &#x27;versioning&#x27;: &#x27;NONE&#x27;},
            &#x27;q&#x27;: {&#x27;dataset&#x27;: &#x27;PQR&#x27;,
                  &#x27;name&#x27;: &#x27;q&#x27;,
                  &#x27;product&#x27;: &#x27;crispbread&#x27;,
                  &#x27;product group&#x27;: &#x27;essential&#x27;,
                  &#x27;repository&#x27;: &#x27;tutorials&#x27;,
                  &#x27;temporality&#x27;: &#x27;AT&#x27;,
                  &#x27;vare&#x27;: &#x27;knekkebrød&#x27;,
                  &#x27;varegruppe&#x27;: &#x27;nødvendigheter&#x27;,
                  &#x27;variabel&#x27;: &#x27;pris&#x27;,
                  &#x27;variable&#x27;: &#x27;price&#x27;,
                  &#x27;versioning&#x27;: &#x27;NONE&#x27;},
            &#x27;r&#x27;: {&#x27;dataset&#x27;: &#x27;PQR&#x27;,
                  &#x27;name&#x27;: &#x27;r&#x27;,
                  &#x27;product&#x27;: &#x27;brown cheese&#x27;,
                  &#x27;product group&#x27;: &#x27;essential&#x27;,
                  &#x27;repository&#x27;: &#x27;tutorials&#x27;,
                  &#x27;temporality&#x27;: &#x27;AT&#x27;,
                  &#x27;vare&#x27;: &#x27;brunost&#x27;,
                  &#x27;varegruppe&#x27;: &#x27;nødvendigheter&#x27;,
                  &#x27;variabel&#x27;: &#x27;pris&#x27;,
                  &#x27;variable&#x27;: &#x27;price&#x27;,
                  &#x27;versioning&#x27;: &#x27;NONE&#x27;}},
 &#x27;temporality&#x27;: &#x27;AT&#x27;,
 &#x27;varegruppe&#x27;: &#x27;nødvendigheter&#x27;,
 &#x27;variabel&#x27;: &#x27;pris&#x27;,
 &#x27;variable&#x27;: &#x27;price&#x27;,
 &#x27;versioning&#x27;: &#x27;NONE&#x27;}</pre>

For each item, the `object_tags` in the global dictionary is the same as `Dataset.tags`:

```python {.marimo}
sample_data = Dataset('PQR')
sample_data.tags == all_sets_tag_dict['PQR']
```

<!-- @output:aLJB -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">True</pre>

Searches that rely only on names and tags can be performed without accessing data:

```python {.marimo}
series = timeseries_catalog.series(tags={'dataset': ['PQR', 'XYZ']})
[f"{s.repository_name}/{s.parent}/{s.object_name}" for s in series]
```

<!-- @output:xXTn -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">&#91;&#x27;tutorials/PQR/p&#x27;,
 &#x27;tutorials/PQR/q&#x27;,
 &#x27;tutorials/PQR/r&#x27;,
 &#x27;tutorials/XYZ/x&#x27;,
 &#x27;tutorials/XYZ/y&#x27;,
 &#x27;tutorials/XYZ/z&#x27;&#93;</pre>

```python {.marimo}
catalog_item_list_to_df(everything)
```

<!-- @output:AjVT -->

| repository_name | object_name | object_type | object_tags |
| --- | --- | --- | --- |
| tutorials | A Sample Dataset | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {2 series tags} |
| tutorials | y | series | {2 series tags} |
| tutorials | z | series | {2 series tags} |
| tutorials | AZ_drikkevarer | dataset | {4 set tags + 260 series} |
| tutorials | a_antall_brus | series | {8 series tags} |
| tutorials | a_antall_kaffe | series | {8 series tags} |
| tutorials | a_antall_te | series | {8 series tags} |
| tutorials | a_antall_vin | series | {8 series tags} |
| tutorials | a_antall_øl | series | {8 series tags} |
| tutorials | a_pris_brus | series | {8 series tags} |
| tutorials | a_pris_kaffe | series | {8 series tags} |
| tutorials | a_pris_te | series | {8 series tags} |
| tutorials | a_pris_vin | series | {8 series tags} |
| tutorials | a_pris_øl | series | {8 series tags} |
| tutorials | b_antall_brus | series | {8 series tags} |
| tutorials | b_antall_kaffe | series | {8 series tags} |
| tutorials | b_antall_te | series | {8 series tags} |
| tutorials | b_antall_vin | series | {8 series tags} |
| tutorials | b_antall_øl | series | {8 series tags} |
| tutorials | b_pris_brus | series | {8 series tags} |
| tutorials | b_pris_kaffe | series | {8 series tags} |
| tutorials | b_pris_te | series | {8 series tags} |
| tutorials | b_pris_vin | series | {8 series tags} |
| tutorials | b_pris_øl | series | {8 series tags} |
| tutorials | c_antall_brus | series | {8 series tags} |
| tutorials | c_antall_kaffe | series | {8 series tags} |
| tutorials | c_antall_te | series | {8 series tags} |
| tutorials | c_antall_vin | series | {8 series tags} |
| tutorials | c_antall_øl | series | {8 series tags} |
| tutorials | c_pris_brus | series | {8 series tags} |
| tutorials | c_pris_kaffe | series | {8 series tags} |
| tutorials | c_pris_te | series | {8 series tags} |
| tutorials | c_pris_vin | series | {8 series tags} |
| tutorials | c_pris_øl | series | {8 series tags} |
| tutorials | d_antall_brus | series | {8 series tags} |
| tutorials | d_antall_kaffe | series | {8 series tags} |
| tutorials | d_antall_te | series | {8 series tags} |
| tutorials | d_antall_vin | series | {8 series tags} |
| tutorials | d_antall_øl | series | {8 series tags} |
| tutorials | d_pris_brus | series | {8 series tags} |
| tutorials | d_pris_kaffe | series | {8 series tags} |
| tutorials | d_pris_te | series | {8 series tags} |
| tutorials | d_pris_vin | series | {8 series tags} |
| tutorials | d_pris_øl | series | {8 series tags} |
| tutorials | e_antall_brus | series | {8 series tags} |
| tutorials | e_antall_kaffe | series | {8 series tags} |
| tutorials | e_antall_te | series | {8 series tags} |
| tutorials | e_antall_vin | series | {8 series tags} |
| tutorials | e_antall_øl | series | {8 series tags} |
| tutorials | e_pris_brus | series | {8 series tags} |
| tutorials | e_pris_kaffe | series | {8 series tags} |
| tutorials | e_pris_te | series | {8 series tags} |
| tutorials | e_pris_vin | series | {8 series tags} |
| tutorials | e_pris_øl | series | {8 series tags} |
| tutorials | f_antall_brus | series | {8 series tags} |
| tutorials | f_antall_kaffe | series | {8 series tags} |
| tutorials | f_antall_te | series | {8 series tags} |
| tutorials | f_antall_vin | series | {8 series tags} |
| tutorials | f_antall_øl | series | {8 series tags} |
| tutorials | f_pris_brus | series | {8 series tags} |
| tutorials | f_pris_kaffe | series | {8 series tags} |
| tutorials | f_pris_te | series | {8 series tags} |
| tutorials | f_pris_vin | series | {8 series tags} |
| tutorials | f_pris_øl | series | {8 series tags} |
| tutorials | g_antall_brus | series | {8 series tags} |
| tutorials | g_antall_kaffe | series | {8 series tags} |
| tutorials | g_antall_te | series | {8 series tags} |
| tutorials | g_antall_vin | series | {8 series tags} |
| tutorials | g_antall_øl | series | {8 series tags} |
| tutorials | g_pris_brus | series | {8 series tags} |
| tutorials | g_pris_kaffe | series | {8 series tags} |
| tutorials | g_pris_te | series | {8 series tags} |
| tutorials | g_pris_vin | series | {8 series tags} |
| tutorials | g_pris_øl | series | {8 series tags} |
| tutorials | h_antall_brus | series | {8 series tags} |
| tutorials | h_antall_kaffe | series | {8 series tags} |
| tutorials | h_antall_te | series | {8 series tags} |
| tutorials | h_antall_vin | series | {8 series tags} |
| tutorials | h_antall_øl | series | {8 series tags} |
| tutorials | h_pris_brus | series | {8 series tags} |
| tutorials | h_pris_kaffe | series | {8 series tags} |
| tutorials | h_pris_te | series | {8 series tags} |
| tutorials | h_pris_vin | series | {8 series tags} |
| tutorials | h_pris_øl | series | {8 series tags} |
| tutorials | i_antall_brus | series | {8 series tags} |
| tutorials | i_antall_kaffe | series | {8 series tags} |
| tutorials | i_antall_te | series | {8 series tags} |
| tutorials | i_antall_vin | series | {8 series tags} |
| tutorials | i_antall_øl | series | {8 series tags} |
| tutorials | i_pris_brus | series | {8 series tags} |
| tutorials | i_pris_kaffe | series | {8 series tags} |
| tutorials | i_pris_te | series | {8 series tags} |
| tutorials | i_pris_vin | series | {8 series tags} |
| tutorials | i_pris_øl | series | {8 series tags} |
| tutorials | j_antall_brus | series | {8 series tags} |
| tutorials | j_antall_kaffe | series | {8 series tags} |
| tutorials | j_antall_te | series | {8 series tags} |
| tutorials | j_antall_vin | series | {8 series tags} |
| tutorials | j_antall_øl | series | {8 series tags} |
| tutorials | j_pris_brus | series | {8 series tags} |
| tutorials | j_pris_kaffe | series | {8 series tags} |
| tutorials | j_pris_te | series | {8 series tags} |
| tutorials | j_pris_vin | series | {8 series tags} |
| tutorials | j_pris_øl | series | {8 series tags} |
| tutorials | k_antall_brus | series | {8 series tags} |
| tutorials | k_antall_kaffe | series | {8 series tags} |
| tutorials | k_antall_te | series | {8 series tags} |
| tutorials | k_antall_vin | series | {8 series tags} |
| tutorials | k_antall_øl | series | {8 series tags} |
| tutorials | k_pris_brus | series | {8 series tags} |
| tutorials | k_pris_kaffe | series | {8 series tags} |
| tutorials | k_pris_te | series | {8 series tags} |
| tutorials | k_pris_vin | series | {8 series tags} |
| tutorials | k_pris_øl | series | {8 series tags} |
| tutorials | l_antall_brus | series | {8 series tags} |
| tutorials | l_antall_kaffe | series | {8 series tags} |
| tutorials | l_antall_te | series | {8 series tags} |
| tutorials | l_antall_vin | series | {8 series tags} |
| tutorials | l_antall_øl | series | {8 series tags} |
| tutorials | l_pris_brus | series | {8 series tags} |
| tutorials | l_pris_kaffe | series | {8 series tags} |
| tutorials | l_pris_te | series | {8 series tags} |
| tutorials | l_pris_vin | series | {8 series tags} |
| tutorials | l_pris_øl | series | {8 series tags} |
| tutorials | m_antall_brus | series | {8 series tags} |
| tutorials | m_antall_kaffe | series | {8 series tags} |
| tutorials | m_antall_te | series | {8 series tags} |
| tutorials | m_antall_vin | series | {8 series tags} |
| tutorials | m_antall_øl | series | {8 series tags} |
| tutorials | m_pris_brus | series | {8 series tags} |
| tutorials | m_pris_kaffe | series | {8 series tags} |
| tutorials | m_pris_te | series | {8 series tags} |
| tutorials | m_pris_vin | series | {8 series tags} |
| tutorials | m_pris_øl | series | {8 series tags} |
| tutorials | n_antall_brus | series | {8 series tags} |
| tutorials | n_antall_kaffe | series | {8 series tags} |
| tutorials | n_antall_te | series | {8 series tags} |
| tutorials | n_antall_vin | series | {8 series tags} |
| tutorials | n_antall_øl | series | {8 series tags} |
| tutorials | n_pris_brus | series | {8 series tags} |
| tutorials | n_pris_kaffe | series | {8 series tags} |
| tutorials | n_pris_te | series | {8 series tags} |
| tutorials | n_pris_vin | series | {8 series tags} |
| tutorials | n_pris_øl | series | {8 series tags} |
| tutorials | o_antall_brus | series | {8 series tags} |
| tutorials | o_antall_kaffe | series | {8 series tags} |
| tutorials | o_antall_te | series | {8 series tags} |
| tutorials | o_antall_vin | series | {8 series tags} |
| tutorials | o_antall_øl | series | {8 series tags} |
| tutorials | o_pris_brus | series | {8 series tags} |
| tutorials | o_pris_kaffe | series | {8 series tags} |
| tutorials | o_pris_te | series | {8 series tags} |
| tutorials | o_pris_vin | series | {8 series tags} |
| tutorials | o_pris_øl | series | {8 series tags} |
| tutorials | p_antall_brus | series | {8 series tags} |
| tutorials | p_antall_kaffe | series | {8 series tags} |
| tutorials | p_antall_te | series | {8 series tags} |
| tutorials | p_antall_vin | series | {8 series tags} |
| tutorials | p_antall_øl | series | {8 series tags} |
| tutorials | p_pris_brus | series | {8 series tags} |
| tutorials | p_pris_kaffe | series | {8 series tags} |
| tutorials | p_pris_te | series | {8 series tags} |
| tutorials | p_pris_vin | series | {8 series tags} |
| tutorials | p_pris_øl | series | {8 series tags} |
| tutorials | q_antall_brus | series | {8 series tags} |
| tutorials | q_antall_kaffe | series | {8 series tags} |
| tutorials | q_antall_te | series | {8 series tags} |
| tutorials | q_antall_vin | series | {8 series tags} |
| tutorials | q_antall_øl | series | {8 series tags} |
| tutorials | q_pris_brus | series | {8 series tags} |
| tutorials | q_pris_kaffe | series | {8 series tags} |
| tutorials | q_pris_te | series | {8 series tags} |
| tutorials | q_pris_vin | series | {8 series tags} |
| tutorials | q_pris_øl | series | {8 series tags} |
| tutorials | r_antall_brus | series | {8 series tags} |
| tutorials | r_antall_kaffe | series | {8 series tags} |
| tutorials | r_antall_te | series | {8 series tags} |
| tutorials | r_antall_vin | series | {8 series tags} |
| tutorials | r_antall_øl | series | {8 series tags} |
| tutorials | r_pris_brus | series | {8 series tags} |
| tutorials | r_pris_kaffe | series | {8 series tags} |
| tutorials | r_pris_te | series | {8 series tags} |
| tutorials | r_pris_vin | series | {8 series tags} |
| tutorials | r_pris_øl | series | {8 series tags} |
| tutorials | s_antall_brus | series | {8 series tags} |
| tutorials | s_antall_kaffe | series | {8 series tags} |
| tutorials | s_antall_te | series | {8 series tags} |
| tutorials | s_antall_vin | series | {8 series tags} |
| tutorials | s_antall_øl | series | {8 series tags} |
| tutorials | s_pris_brus | series | {8 series tags} |
| tutorials | s_pris_kaffe | series | {8 series tags} |
| tutorials | s_pris_te | series | {8 series tags} |
| tutorials | s_pris_vin | series | {8 series tags} |
| tutorials | s_pris_øl | series | {8 series tags} |
| tutorials | t_antall_brus | series | {8 series tags} |
| tutorials | t_antall_kaffe | series | {8 series tags} |
| tutorials | t_antall_te | series | {8 series tags} |
| tutorials | t_antall_vin | series | {8 series tags} |
| tutorials | t_antall_øl | series | {8 series tags} |
| tutorials | t_pris_brus | series | {8 series tags} |
| tutorials | t_pris_kaffe | series | {8 series tags} |
| tutorials | t_pris_te | series | {8 series tags} |
| tutorials | t_pris_vin | series | {8 series tags} |
| tutorials | t_pris_øl | series | {8 series tags} |
| tutorials | u_antall_brus | series | {8 series tags} |
| tutorials | u_antall_kaffe | series | {8 series tags} |
| tutorials | u_antall_te | series | {8 series tags} |
| tutorials | u_antall_vin | series | {8 series tags} |
| tutorials | u_antall_øl | series | {8 series tags} |
| tutorials | u_pris_brus | series | {8 series tags} |
| tutorials | u_pris_kaffe | series | {8 series tags} |
| tutorials | u_pris_te | series | {8 series tags} |
| tutorials | u_pris_vin | series | {8 series tags} |
| tutorials | u_pris_øl | series | {8 series tags} |
| tutorials | v_antall_brus | series | {8 series tags} |
| tutorials | v_antall_kaffe | series | {8 series tags} |
| tutorials | v_antall_te | series | {8 series tags} |
| tutorials | v_antall_vin | series | {8 series tags} |
| tutorials | v_antall_øl | series | {8 series tags} |
| tutorials | v_pris_brus | series | {8 series tags} |
| tutorials | v_pris_kaffe | series | {8 series tags} |
| tutorials | v_pris_te | series | {8 series tags} |
| tutorials | v_pris_vin | series | {8 series tags} |
| tutorials | v_pris_øl | series | {8 series tags} |
| tutorials | w_antall_brus | series | {8 series tags} |
| tutorials | w_antall_kaffe | series | {8 series tags} |
| tutorials | w_antall_te | series | {8 series tags} |
| tutorials | w_antall_vin | series | {8 series tags} |
| tutorials | w_antall_øl | series | {8 series tags} |
| tutorials | w_pris_brus | series | {8 series tags} |
| tutorials | w_pris_kaffe | series | {8 series tags} |
| tutorials | w_pris_te | series | {8 series tags} |
| tutorials | w_pris_vin | series | {8 series tags} |
| tutorials | w_pris_øl | series | {8 series tags} |
| tutorials | x_antall_brus | series | {8 series tags} |
| tutorials | x_antall_kaffe | series | {8 series tags} |
| tutorials | x_antall_te | series | {8 series tags} |
| tutorials | x_antall_vin | series | {8 series tags} |
| tutorials | x_antall_øl | series | {8 series tags} |
| tutorials | x_pris_brus | series | {8 series tags} |
| tutorials | x_pris_kaffe | series | {8 series tags} |
| tutorials | x_pris_te | series | {8 series tags} |
| tutorials | x_pris_vin | series | {8 series tags} |
| tutorials | x_pris_øl | series | {8 series tags} |
| tutorials | y_antall_brus | series | {8 series tags} |
| tutorials | y_antall_kaffe | series | {8 series tags} |
| tutorials | y_antall_te | series | {8 series tags} |
| tutorials | y_antall_vin | series | {8 series tags} |
| tutorials | y_antall_øl | series | {8 series tags} |
| tutorials | y_pris_brus | series | {8 series tags} |
| tutorials | y_pris_kaffe | series | {8 series tags} |
| tutorials | y_pris_te | series | {8 series tags} |
| tutorials | y_pris_vin | series | {8 series tags} |
| tutorials | y_pris_øl | series | {8 series tags} |
| tutorials | z_antall_brus | series | {8 series tags} |
| tutorials | z_antall_kaffe | series | {8 series tags} |
| tutorials | z_antall_te | series | {8 series tags} |
| tutorials | z_antall_vin | series | {8 series tags} |
| tutorials | z_antall_øl | series | {8 series tags} |
| tutorials | z_pris_brus | series | {8 series tags} |
| tutorials | z_pris_kaffe | series | {8 series tags} |
| tutorials | z_pris_te | series | {8 series tags} |
| tutorials | z_pris_vin | series | {8 series tags} |
| tutorials | z_pris_øl | series | {8 series tags} |
| tutorials | AZ_drinks | dataset | {4 set tags + 2080 series} |
| tutorials | a_price_beer_E | series | {9 series tags} |
| tutorials | a_price_beer_N | series | {9 series tags} |
| tutorials | a_price_beer_NE | series | {9 series tags} |
| tutorials | a_price_beer_NW | series | {9 series tags} |
| tutorials | a_price_beer_S | series | {9 series tags} |
| tutorials | a_price_beer_SE | series | {9 series tags} |
| tutorials | a_price_beer_SW | series | {9 series tags} |
| tutorials | a_price_beer_W | series | {9 series tags} |
| tutorials | a_price_coffee_E | series | {9 series tags} |
| tutorials | a_price_coffee_N | series | {9 series tags} |
| tutorials | a_price_coffee_NE | series | {9 series tags} |
| tutorials | a_price_coffee_NW | series | {9 series tags} |
| tutorials | a_price_coffee_S | series | {9 series tags} |
| tutorials | a_price_coffee_SE | series | {9 series tags} |
| tutorials | a_price_coffee_SW | series | {9 series tags} |
| tutorials | a_price_coffee_W | series | {9 series tags} |
| tutorials | a_price_soft-drinks_E | series | {9 series tags} |
| tutorials | a_price_soft-drinks_N | series | {9 series tags} |
| tutorials | a_price_soft-drinks_NE | series | {9 series tags} |
| tutorials | a_price_soft-drinks_NW | series | {9 series tags} |
| tutorials | a_price_soft-drinks_S | series | {9 series tags} |
| tutorials | a_price_soft-drinks_SE | series | {9 series tags} |
| tutorials | a_price_soft-drinks_SW | series | {9 series tags} |
| tutorials | a_price_soft-drinks_W | series | {9 series tags} |
| tutorials | a_price_tea_E | series | {9 series tags} |
| tutorials | a_price_tea_N | series | {9 series tags} |
| tutorials | a_price_tea_NE | series | {9 series tags} |
| tutorials | a_price_tea_NW | series | {9 series tags} |
| tutorials | a_price_tea_S | series | {9 series tags} |
| tutorials | a_price_tea_SE | series | {9 series tags} |
| tutorials | a_price_tea_SW | series | {9 series tags} |
| tutorials | a_price_tea_W | series | {9 series tags} |
| tutorials | a_price_wine_E | series | {9 series tags} |
| tutorials | a_price_wine_N | series | {9 series tags} |
| tutorials | a_price_wine_NE | series | {9 series tags} |
| tutorials | a_price_wine_NW | series | {9 series tags} |
| tutorials | a_price_wine_S | series | {9 series tags} |
| tutorials | a_price_wine_SE | series | {9 series tags} |
| tutorials | a_price_wine_SW | series | {9 series tags} |
| tutorials | a_price_wine_W | series | {9 series tags} |
| tutorials | a_volume_beer_E | series | {9 series tags} |
| tutorials | a_volume_beer_N | series | {9 series tags} |
| tutorials | a_volume_beer_NE | series | {9 series tags} |
| tutorials | a_volume_beer_NW | series | {9 series tags} |
| tutorials | a_volume_beer_S | series | {9 series tags} |
| tutorials | a_volume_beer_SE | series | {9 series tags} |
| tutorials | a_volume_beer_SW | series | {9 series tags} |
| tutorials | a_volume_beer_W | series | {9 series tags} |
| tutorials | a_volume_coffee_E | series | {9 series tags} |
| tutorials | a_volume_coffee_N | series | {9 series tags} |
| tutorials | a_volume_coffee_NE | series | {9 series tags} |
| tutorials | a_volume_coffee_NW | series | {9 series tags} |
| tutorials | a_volume_coffee_S | series | {9 series tags} |
| tutorials | a_volume_coffee_SE | series | {9 series tags} |
| tutorials | a_volume_coffee_SW | series | {9 series tags} |
| tutorials | a_volume_coffee_W | series | {9 series tags} |
| tutorials | a_volume_soft-drinks_E | series | {9 series tags} |
| tutorials | a_volume_soft-drinks_N | series | {9 series tags} |
| tutorials | a_volume_soft-drinks_NE | series | {9 series tags} |
| tutorials | a_volume_soft-drinks_NW | series | {9 series tags} |
| tutorials | a_volume_soft-drinks_S | series | {9 series tags} |
| tutorials | a_volume_soft-drinks_SE | series | {9 series tags} |
| tutorials | a_volume_soft-drinks_SW | series | {9 series tags} |
| tutorials | a_volume_soft-drinks_W | series | {9 series tags} |
| tutorials | a_volume_tea_E | series | {9 series tags} |
| tutorials | a_volume_tea_N | series | {9 series tags} |
| tutorials | a_volume_tea_NE | series | {9 series tags} |
| tutorials | a_volume_tea_NW | series | {9 series tags} |
| tutorials | a_volume_tea_S | series | {9 series tags} |
| tutorials | a_volume_tea_SE | series | {9 series tags} |
| tutorials | a_volume_tea_SW | series | {9 series tags} |
| tutorials | a_volume_tea_W | series | {9 series tags} |
| tutorials | a_volume_wine_E | series | {9 series tags} |
| tutorials | a_volume_wine_N | series | {9 series tags} |
| tutorials | a_volume_wine_NE | series | {9 series tags} |
| tutorials | a_volume_wine_NW | series | {9 series tags} |
| tutorials | a_volume_wine_S | series | {9 series tags} |
| tutorials | a_volume_wine_SE | series | {9 series tags} |
| tutorials | a_volume_wine_SW | series | {9 series tags} |
| tutorials | a_volume_wine_W | series | {9 series tags} |
| tutorials | b_price_beer_E | series | {9 series tags} |
| tutorials | b_price_beer_N | series | {9 series tags} |
| tutorials | b_price_beer_NE | series | {9 series tags} |
| tutorials | b_price_beer_NW | series | {9 series tags} |
| tutorials | b_price_beer_S | series | {9 series tags} |
| tutorials | b_price_beer_SE | series | {9 series tags} |
| tutorials | b_price_beer_SW | series | {9 series tags} |
| tutorials | b_price_beer_W | series | {9 series tags} |
| tutorials | b_price_coffee_E | series | {9 series tags} |
| tutorials | b_price_coffee_N | series | {9 series tags} |
| tutorials | b_price_coffee_NE | series | {9 series tags} |
| tutorials | b_price_coffee_NW | series | {9 series tags} |
| tutorials | b_price_coffee_S | series | {9 series tags} |
| tutorials | b_price_coffee_SE | series | {9 series tags} |
| tutorials | b_price_coffee_SW | series | {9 series tags} |
| tutorials | b_price_coffee_W | series | {9 series tags} |
| tutorials | b_price_soft-drinks_E | series | {9 series tags} |
| tutorials | b_price_soft-drinks_N | series | {9 series tags} |
| tutorials | b_price_soft-drinks_NE | series | {9 series tags} |
| tutorials | b_price_soft-drinks_NW | series | {9 series tags} |
| tutorials | b_price_soft-drinks_S | series | {9 series tags} |
| tutorials | b_price_soft-drinks_SE | series | {9 series tags} |
| tutorials | b_price_soft-drinks_SW | series | {9 series tags} |
| tutorials | b_price_soft-drinks_W | series | {9 series tags} |
| tutorials | b_price_tea_E | series | {9 series tags} |
| tutorials | b_price_tea_N | series | {9 series tags} |
| tutorials | b_price_tea_NE | series | {9 series tags} |
| tutorials | b_price_tea_NW | series | {9 series tags} |
| tutorials | b_price_tea_S | series | {9 series tags} |
| tutorials | b_price_tea_SE | series | {9 series tags} |
| tutorials | b_price_tea_SW | series | {9 series tags} |
| tutorials | b_price_tea_W | series | {9 series tags} |
| tutorials | b_price_wine_E | series | {9 series tags} |
| tutorials | b_price_wine_N | series | {9 series tags} |
| tutorials | b_price_wine_NE | series | {9 series tags} |
| tutorials | b_price_wine_NW | series | {9 series tags} |
| tutorials | b_price_wine_S | series | {9 series tags} |
| tutorials | b_price_wine_SE | series | {9 series tags} |
| tutorials | b_price_wine_SW | series | {9 series tags} |
| tutorials | b_price_wine_W | series | {9 series tags} |
| tutorials | b_volume_beer_E | series | {9 series tags} |
| tutorials | b_volume_beer_N | series | {9 series tags} |
| tutorials | b_volume_beer_NE | series | {9 series tags} |
| tutorials | b_volume_beer_NW | series | {9 series tags} |
| tutorials | b_volume_beer_S | series | {9 series tags} |
| tutorials | b_volume_beer_SE | series | {9 series tags} |
| tutorials | b_volume_beer_SW | series | {9 series tags} |
| tutorials | b_volume_beer_W | series | {9 series tags} |
| tutorials | b_volume_coffee_E | series | {9 series tags} |
| tutorials | b_volume_coffee_N | series | {9 series tags} |
| tutorials | b_volume_coffee_NE | series | {9 series tags} |
| tutorials | b_volume_coffee_NW | series | {9 series tags} |
| tutorials | b_volume_coffee_S | series | {9 series tags} |
| tutorials | b_volume_coffee_SE | series | {9 series tags} |
| tutorials | b_volume_coffee_SW | series | {9 series tags} |
| tutorials | b_volume_coffee_W | series | {9 series tags} |
| tutorials | b_volume_soft-drinks_E | series | {9 series tags} |
| tutorials | b_volume_soft-drinks_N | series | {9 series tags} |
| tutorials | b_volume_soft-drinks_NE | series | {9 series tags} |
| tutorials | b_volume_soft-drinks_NW | series | {9 series tags} |
| tutorials | b_volume_soft-drinks_S | series | {9 series tags} |
| tutorials | b_volume_soft-drinks_SE | series | {9 series tags} |
| tutorials | b_volume_soft-drinks_SW | series | {9 series tags} |
| tutorials | b_volume_soft-drinks_W | series | {9 series tags} |
| tutorials | b_volume_tea_E | series | {9 series tags} |
| tutorials | b_volume_tea_N | series | {9 series tags} |
| tutorials | b_volume_tea_NE | series | {9 series tags} |
| tutorials | b_volume_tea_NW | series | {9 series tags} |
| tutorials | b_volume_tea_S | series | {9 series tags} |
| tutorials | b_volume_tea_SE | series | {9 series tags} |
| tutorials | b_volume_tea_SW | series | {9 series tags} |
| tutorials | b_volume_tea_W | series | {9 series tags} |
| tutorials | b_volume_wine_E | series | {9 series tags} |
| tutorials | b_volume_wine_N | series | {9 series tags} |
| tutorials | b_volume_wine_NE | series | {9 series tags} |
| tutorials | b_volume_wine_NW | series | {9 series tags} |
| tutorials | b_volume_wine_S | series | {9 series tags} |
| tutorials | b_volume_wine_SE | series | {9 series tags} |
| tutorials | b_volume_wine_SW | series | {9 series tags} |
| tutorials | b_volume_wine_W | series | {9 series tags} |
| tutorials | c_price_beer_E | series | {9 series tags} |
| tutorials | c_price_beer_N | series | {9 series tags} |
| tutorials | c_price_beer_NE | series | {9 series tags} |
| tutorials | c_price_beer_NW | series | {9 series tags} |
| tutorials | c_price_beer_S | series | {9 series tags} |
| tutorials | c_price_beer_SE | series | {9 series tags} |
| tutorials | c_price_beer_SW | series | {9 series tags} |
| tutorials | c_price_beer_W | series | {9 series tags} |
| tutorials | c_price_coffee_E | series | {9 series tags} |
| tutorials | c_price_coffee_N | series | {9 series tags} |
| tutorials | c_price_coffee_NE | series | {9 series tags} |
| tutorials | c_price_coffee_NW | series | {9 series tags} |
| tutorials | c_price_coffee_S | series | {9 series tags} |
| tutorials | c_price_coffee_SE | series | {9 series tags} |
| tutorials | c_price_coffee_SW | series | {9 series tags} |
| tutorials | c_price_coffee_W | series | {9 series tags} |
| tutorials | c_price_soft-drinks_E | series | {9 series tags} |
| tutorials | c_price_soft-drinks_N | series | {9 series tags} |
| tutorials | c_price_soft-drinks_NE | series | {9 series tags} |
| tutorials | c_price_soft-drinks_NW | series | {9 series tags} |
| tutorials | c_price_soft-drinks_S | series | {9 series tags} |
| tutorials | c_price_soft-drinks_SE | series | {9 series tags} |
| tutorials | c_price_soft-drinks_SW | series | {9 series tags} |
| tutorials | c_price_soft-drinks_W | series | {9 series tags} |
| tutorials | c_price_tea_E | series | {9 series tags} |
| tutorials | c_price_tea_N | series | {9 series tags} |
| tutorials | c_price_tea_NE | series | {9 series tags} |
| tutorials | c_price_tea_NW | series | {9 series tags} |
| tutorials | c_price_tea_S | series | {9 series tags} |
| tutorials | c_price_tea_SE | series | {9 series tags} |
| tutorials | c_price_tea_SW | series | {9 series tags} |
| tutorials | c_price_tea_W | series | {9 series tags} |
| tutorials | c_price_wine_E | series | {9 series tags} |
| tutorials | c_price_wine_N | series | {9 series tags} |
| tutorials | c_price_wine_NE | series | {9 series tags} |
| tutorials | c_price_wine_NW | series | {9 series tags} |
| tutorials | c_price_wine_S | series | {9 series tags} |
| tutorials | c_price_wine_SE | series | {9 series tags} |
| tutorials | c_price_wine_SW | series | {9 series tags} |
| tutorials | c_price_wine_W | series | {9 series tags} |
| tutorials | c_volume_beer_E | series | {9 series tags} |
| tutorials | c_volume_beer_N | series | {9 series tags} |
| tutorials | c_volume_beer_NE | series | {9 series tags} |
| tutorials | c_volume_beer_NW | series | {9 series tags} |
| tutorials | c_volume_beer_S | series | {9 series tags} |
| tutorials | c_volume_beer_SE | series | {9 series tags} |
| tutorials | c_volume_beer_SW | series | {9 series tags} |
| tutorials | c_volume_beer_W | series | {9 series tags} |
| tutorials | c_volume_coffee_E | series | {9 series tags} |
| tutorials | c_volume_coffee_N | series | {9 series tags} |
| tutorials | c_volume_coffee_NE | series | {9 series tags} |
| tutorials | c_volume_coffee_NW | series | {9 series tags} |
| tutorials | c_volume_coffee_S | series | {9 series tags} |
| tutorials | c_volume_coffee_SE | series | {9 series tags} |
| tutorials | c_volume_coffee_SW | series | {9 series tags} |
| tutorials | c_volume_coffee_W | series | {9 series tags} |
| tutorials | c_volume_soft-drinks_E | series | {9 series tags} |
| tutorials | c_volume_soft-drinks_N | series | {9 series tags} |
| tutorials | c_volume_soft-drinks_NE | series | {9 series tags} |
| tutorials | c_volume_soft-drinks_NW | series | {9 series tags} |
| tutorials | c_volume_soft-drinks_S | series | {9 series tags} |
| tutorials | c_volume_soft-drinks_SE | series | {9 series tags} |
| tutorials | c_volume_soft-drinks_SW | series | {9 series tags} |
| tutorials | c_volume_soft-drinks_W | series | {9 series tags} |
| tutorials | c_volume_tea_E | series | {9 series tags} |
| tutorials | c_volume_tea_N | series | {9 series tags} |
| tutorials | c_volume_tea_NE | series | {9 series tags} |
| tutorials | c_volume_tea_NW | series | {9 series tags} |
| tutorials | c_volume_tea_S | series | {9 series tags} |
| tutorials | c_volume_tea_SE | series | {9 series tags} |
| tutorials | c_volume_tea_SW | series | {9 series tags} |
| tutorials | c_volume_tea_W | series | {9 series tags} |
| tutorials | c_volume_wine_E | series | {9 series tags} |
| tutorials | c_volume_wine_N | series | {9 series tags} |
| tutorials | c_volume_wine_NE | series | {9 series tags} |
| tutorials | c_volume_wine_NW | series | {9 series tags} |
| tutorials | c_volume_wine_S | series | {9 series tags} |
| tutorials | c_volume_wine_SE | series | {9 series tags} |
| tutorials | c_volume_wine_SW | series | {9 series tags} |
| tutorials | c_volume_wine_W | series | {9 series tags} |
| tutorials | d_price_beer_E | series | {9 series tags} |
| tutorials | d_price_beer_N | series | {9 series tags} |
| tutorials | d_price_beer_NE | series | {9 series tags} |
| tutorials | d_price_beer_NW | series | {9 series tags} |
| tutorials | d_price_beer_S | series | {9 series tags} |
| tutorials | d_price_beer_SE | series | {9 series tags} |
| tutorials | d_price_beer_SW | series | {9 series tags} |
| tutorials | d_price_beer_W | series | {9 series tags} |
| tutorials | d_price_coffee_E | series | {9 series tags} |
| tutorials | d_price_coffee_N | series | {9 series tags} |
| tutorials | d_price_coffee_NE | series | {9 series tags} |
| tutorials | d_price_coffee_NW | series | {9 series tags} |
| tutorials | d_price_coffee_S | series | {9 series tags} |
| tutorials | d_price_coffee_SE | series | {9 series tags} |
| tutorials | d_price_coffee_SW | series | {9 series tags} |
| tutorials | d_price_coffee_W | series | {9 series tags} |
| tutorials | d_price_soft-drinks_E | series | {9 series tags} |
| tutorials | d_price_soft-drinks_N | series | {9 series tags} |
| tutorials | d_price_soft-drinks_NE | series | {9 series tags} |
| tutorials | d_price_soft-drinks_NW | series | {9 series tags} |
| tutorials | d_price_soft-drinks_S | series | {9 series tags} |
| tutorials | d_price_soft-drinks_SE | series | {9 series tags} |
| tutorials | d_price_soft-drinks_SW | series | {9 series tags} |
| tutorials | d_price_soft-drinks_W | series | {9 series tags} |
| tutorials | d_price_tea_E | series | {9 series tags} |
| tutorials | d_price_tea_N | series | {9 series tags} |
| tutorials | d_price_tea_NE | series | {9 series tags} |
| tutorials | d_price_tea_NW | series | {9 series tags} |
| tutorials | d_price_tea_S | series | {9 series tags} |
| tutorials | d_price_tea_SE | series | {9 series tags} |
| tutorials | d_price_tea_SW | series | {9 series tags} |
| tutorials | d_price_tea_W | series | {9 series tags} |
| tutorials | d_price_wine_E | series | {9 series tags} |
| tutorials | d_price_wine_N | series | {9 series tags} |
| tutorials | d_price_wine_NE | series | {9 series tags} |
| tutorials | d_price_wine_NW | series | {9 series tags} |
| tutorials | d_price_wine_S | series | {9 series tags} |
| tutorials | d_price_wine_SE | series | {9 series tags} |
| tutorials | d_price_wine_SW | series | {9 series tags} |
| tutorials | d_price_wine_W | series | {9 series tags} |
| tutorials | d_volume_beer_E | series | {9 series tags} |
| tutorials | d_volume_beer_N | series | {9 series tags} |
| tutorials | d_volume_beer_NE | series | {9 series tags} |
| tutorials | d_volume_beer_NW | series | {9 series tags} |
| tutorials | d_volume_beer_S | series | {9 series tags} |
| tutorials | d_volume_beer_SE | series | {9 series tags} |
| tutorials | d_volume_beer_SW | series | {9 series tags} |
| tutorials | d_volume_beer_W | series | {9 series tags} |
| tutorials | d_volume_coffee_E | series | {9 series tags} |
| tutorials | d_volume_coffee_N | series | {9 series tags} |
| tutorials | d_volume_coffee_NE | series | {9 series tags} |
| tutorials | d_volume_coffee_NW | series | {9 series tags} |
| tutorials | d_volume_coffee_S | series | {9 series tags} |
| tutorials | d_volume_coffee_SE | series | {9 series tags} |
| tutorials | d_volume_coffee_SW | series | {9 series tags} |
| tutorials | d_volume_coffee_W | series | {9 series tags} |
| tutorials | d_volume_soft-drinks_E | series | {9 series tags} |
| tutorials | d_volume_soft-drinks_N | series | {9 series tags} |
| tutorials | d_volume_soft-drinks_NE | series | {9 series tags} |
| tutorials | d_volume_soft-drinks_NW | series | {9 series tags} |
| tutorials | d_volume_soft-drinks_S | series | {9 series tags} |
| tutorials | d_volume_soft-drinks_SE | series | {9 series tags} |
| tutorials | d_volume_soft-drinks_SW | series | {9 series tags} |
| tutorials | d_volume_soft-drinks_W | series | {9 series tags} |
| tutorials | d_volume_tea_E | series | {9 series tags} |
| tutorials | d_volume_tea_N | series | {9 series tags} |
| tutorials | d_volume_tea_NE | series | {9 series tags} |
| tutorials | d_volume_tea_NW | series | {9 series tags} |
| tutorials | d_volume_tea_S | series | {9 series tags} |
| tutorials | d_volume_tea_SE | series | {9 series tags} |
| tutorials | d_volume_tea_SW | series | {9 series tags} |
| tutorials | d_volume_tea_W | series | {9 series tags} |
| tutorials | d_volume_wine_E | series | {9 series tags} |
| tutorials | d_volume_wine_N | series | {9 series tags} |
| tutorials | d_volume_wine_NE | series | {9 series tags} |
| tutorials | d_volume_wine_NW | series | {9 series tags} |
| tutorials | d_volume_wine_S | series | {9 series tags} |
| tutorials | d_volume_wine_SE | series | {9 series tags} |
| tutorials | d_volume_wine_SW | series | {9 series tags} |
| tutorials | d_volume_wine_W | series | {9 series tags} |
| tutorials | e_price_beer_E | series | {9 series tags} |
| tutorials | e_price_beer_N | series | {9 series tags} |
| tutorials | e_price_beer_NE | series | {9 series tags} |
| tutorials | e_price_beer_NW | series | {9 series tags} |
| tutorials | e_price_beer_S | series | {9 series tags} |
| tutorials | e_price_beer_SE | series | {9 series tags} |
| tutorials | e_price_beer_SW | series | {9 series tags} |
| tutorials | e_price_beer_W | series | {9 series tags} |
| tutorials | e_price_coffee_E | series | {9 series tags} |
| tutorials | e_price_coffee_N | series | {9 series tags} |
| tutorials | e_price_coffee_NE | series | {9 series tags} |
| tutorials | e_price_coffee_NW | series | {9 series tags} |
| tutorials | e_price_coffee_S | series | {9 series tags} |
| tutorials | e_price_coffee_SE | series | {9 series tags} |
| tutorials | e_price_coffee_SW | series | {9 series tags} |
| tutorials | e_price_coffee_W | series | {9 series tags} |
| tutorials | e_price_soft-drinks_E | series | {9 series tags} |
| tutorials | e_price_soft-drinks_N | series | {9 series tags} |
| tutorials | e_price_soft-drinks_NE | series | {9 series tags} |
| tutorials | e_price_soft-drinks_NW | series | {9 series tags} |
| tutorials | e_price_soft-drinks_S | series | {9 series tags} |
| tutorials | e_price_soft-drinks_SE | series | {9 series tags} |
| tutorials | e_price_soft-drinks_SW | series | {9 series tags} |
| tutorials | e_price_soft-drinks_W | series | {9 series tags} |
| tutorials | e_price_tea_E | series | {9 series tags} |
| tutorials | e_price_tea_N | series | {9 series tags} |
| tutorials | e_price_tea_NE | series | {9 series tags} |
| tutorials | e_price_tea_NW | series | {9 series tags} |
| tutorials | e_price_tea_S | series | {9 series tags} |
| tutorials | e_price_tea_SE | series | {9 series tags} |
| tutorials | e_price_tea_SW | series | {9 series tags} |
| tutorials | e_price_tea_W | series | {9 series tags} |
| tutorials | e_price_wine_E | series | {9 series tags} |
| tutorials | e_price_wine_N | series | {9 series tags} |
| tutorials | e_price_wine_NE | series | {9 series tags} |
| tutorials | e_price_wine_NW | series | {9 series tags} |
| tutorials | e_price_wine_S | series | {9 series tags} |
| tutorials | e_price_wine_SE | series | {9 series tags} |
| tutorials | e_price_wine_SW | series | {9 series tags} |
| tutorials | e_price_wine_W | series | {9 series tags} |
| tutorials | e_volume_beer_E | series | {9 series tags} |
| tutorials | e_volume_beer_N | series | {9 series tags} |
| tutorials | e_volume_beer_NE | series | {9 series tags} |
| tutorials | e_volume_beer_NW | series | {9 series tags} |
| tutorials | e_volume_beer_S | series | {9 series tags} |
| tutorials | e_volume_beer_SE | series | {9 series tags} |
| tutorials | e_volume_beer_SW | series | {9 series tags} |
| tutorials | e_volume_beer_W | series | {9 series tags} |
| tutorials | e_volume_coffee_E | series | {9 series tags} |
| tutorials | e_volume_coffee_N | series | {9 series tags} |
| tutorials | e_volume_coffee_NE | series | {9 series tags} |
| tutorials | e_volume_coffee_NW | series | {9 series tags} |
| tutorials | e_volume_coffee_S | series | {9 series tags} |
| tutorials | e_volume_coffee_SE | series | {9 series tags} |
| tutorials | e_volume_coffee_SW | series | {9 series tags} |
| tutorials | e_volume_coffee_W | series | {9 series tags} |
| tutorials | e_volume_soft-drinks_E | series | {9 series tags} |
| tutorials | e_volume_soft-drinks_N | series | {9 series tags} |
| tutorials | e_volume_soft-drinks_NE | series | {9 series tags} |
| tutorials | e_volume_soft-drinks_NW | series | {9 series tags} |
| tutorials | e_volume_soft-drinks_S | series | {9 series tags} |
| tutorials | e_volume_soft-drinks_SE | series | {9 series tags} |
| tutorials | e_volume_soft-drinks_SW | series | {9 series tags} |
| tutorials | e_volume_soft-drinks_W | series | {9 series tags} |
| tutorials | e_volume_tea_E | series | {9 series tags} |
| tutorials | e_volume_tea_N | series | {9 series tags} |
| tutorials | e_volume_tea_NE | series | {9 series tags} |
| tutorials | e_volume_tea_NW | series | {9 series tags} |
| tutorials | e_volume_tea_S | series | {9 series tags} |
| tutorials | e_volume_tea_SE | series | {9 series tags} |
| tutorials | e_volume_tea_SW | series | {9 series tags} |
| tutorials | e_volume_tea_W | series | {9 series tags} |
| tutorials | e_volume_wine_E | series | {9 series tags} |
| tutorials | e_volume_wine_N | series | {9 series tags} |
| tutorials | e_volume_wine_NE | series | {9 series tags} |
| tutorials | e_volume_wine_NW | series | {9 series tags} |
| tutorials | e_volume_wine_S | series | {9 series tags} |
| tutorials | e_volume_wine_SE | series | {9 series tags} |
| tutorials | e_volume_wine_SW | series | {9 series tags} |
| tutorials | e_volume_wine_W | series | {9 series tags} |
| tutorials | f_price_beer_E | series | {9 series tags} |
| tutorials | f_price_beer_N | series | {9 series tags} |
| tutorials | f_price_beer_NE | series | {9 series tags} |
| tutorials | f_price_beer_NW | series | {9 series tags} |
| tutorials | f_price_beer_S | series | {9 series tags} |
| tutorials | f_price_beer_SE | series | {9 series tags} |
| tutorials | f_price_beer_SW | series | {9 series tags} |
| tutorials | f_price_beer_W | series | {9 series tags} |
| tutorials | f_price_coffee_E | series | {9 series tags} |
| tutorials | f_price_coffee_N | series | {9 series tags} |
| tutorials | f_price_coffee_NE | series | {9 series tags} |
| tutorials | f_price_coffee_NW | series | {9 series tags} |
| tutorials | f_price_coffee_S | series | {9 series tags} |
| tutorials | f_price_coffee_SE | series | {9 series tags} |
| tutorials | f_price_coffee_SW | series | {9 series tags} |
| tutorials | f_price_coffee_W | series | {9 series tags} |
| tutorials | f_price_soft-drinks_E | series | {9 series tags} |
| tutorials | f_price_soft-drinks_N | series | {9 series tags} |
| tutorials | f_price_soft-drinks_NE | series | {9 series tags} |
| tutorials | f_price_soft-drinks_NW | series | {9 series tags} |
| tutorials | f_price_soft-drinks_S | series | {9 series tags} |
| tutorials | f_price_soft-drinks_SE | series | {9 series tags} |
| tutorials | f_price_soft-drinks_SW | series | {9 series tags} |
| tutorials | f_price_soft-drinks_W | series | {9 series tags} |
| tutorials | f_price_tea_E | series | {9 series tags} |
| tutorials | f_price_tea_N | series | {9 series tags} |
| tutorials | f_price_tea_NE | series | {9 series tags} |
| tutorials | f_price_tea_NW | series | {9 series tags} |
| tutorials | f_price_tea_S | series | {9 series tags} |
| tutorials | f_price_tea_SE | series | {9 series tags} |
| tutorials | f_price_tea_SW | series | {9 series tags} |
| tutorials | f_price_tea_W | series | {9 series tags} |
| tutorials | f_price_wine_E | series | {9 series tags} |
| tutorials | f_price_wine_N | series | {9 series tags} |
| tutorials | f_price_wine_NE | series | {9 series tags} |
| tutorials | f_price_wine_NW | series | {9 series tags} |
| tutorials | f_price_wine_S | series | {9 series tags} |
| tutorials | f_price_wine_SE | series | {9 series tags} |
| tutorials | f_price_wine_SW | series | {9 series tags} |
| tutorials | f_price_wine_W | series | {9 series tags} |
| tutorials | f_volume_beer_E | series | {9 series tags} |
| tutorials | f_volume_beer_N | series | {9 series tags} |
| tutorials | f_volume_beer_NE | series | {9 series tags} |
| tutorials | f_volume_beer_NW | series | {9 series tags} |
| tutorials | f_volume_beer_S | series | {9 series tags} |
| tutorials | f_volume_beer_SE | series | {9 series tags} |
| tutorials | f_volume_beer_SW | series | {9 series tags} |
| tutorials | f_volume_beer_W | series | {9 series tags} |
| tutorials | f_volume_coffee_E | series | {9 series tags} |
| tutorials | f_volume_coffee_N | series | {9 series tags} |
| tutorials | f_volume_coffee_NE | series | {9 series tags} |
| tutorials | f_volume_coffee_NW | series | {9 series tags} |
| tutorials | f_volume_coffee_S | series | {9 series tags} |
| tutorials | f_volume_coffee_SE | series | {9 series tags} |
| tutorials | f_volume_coffee_SW | series | {9 series tags} |
| tutorials | f_volume_coffee_W | series | {9 series tags} |
| tutorials | f_volume_soft-drinks_E | series | {9 series tags} |
| tutorials | f_volume_soft-drinks_N | series | {9 series tags} |
| tutorials | f_volume_soft-drinks_NE | series | {9 series tags} |
| tutorials | f_volume_soft-drinks_NW | series | {9 series tags} |
| tutorials | f_volume_soft-drinks_S | series | {9 series tags} |
| tutorials | f_volume_soft-drinks_SE | series | {9 series tags} |
| tutorials | f_volume_soft-drinks_SW | series | {9 series tags} |
| tutorials | f_volume_soft-drinks_W | series | {9 series tags} |
| tutorials | f_volume_tea_E | series | {9 series tags} |
| tutorials | f_volume_tea_N | series | {9 series tags} |
| tutorials | f_volume_tea_NE | series | {9 series tags} |
| tutorials | f_volume_tea_NW | series | {9 series tags} |
| tutorials | f_volume_tea_S | series | {9 series tags} |
| tutorials | f_volume_tea_SE | series | {9 series tags} |
| tutorials | f_volume_tea_SW | series | {9 series tags} |
| tutorials | f_volume_tea_W | series | {9 series tags} |
| tutorials | f_volume_wine_E | series | {9 series tags} |
| tutorials | f_volume_wine_N | series | {9 series tags} |
| tutorials | f_volume_wine_NE | series | {9 series tags} |
| tutorials | f_volume_wine_NW | series | {9 series tags} |
| tutorials | f_volume_wine_S | series | {9 series tags} |
| tutorials | f_volume_wine_SE | series | {9 series tags} |
| tutorials | f_volume_wine_SW | series | {9 series tags} |
| tutorials | f_volume_wine_W | series | {9 series tags} |
| tutorials | g_price_beer_E | series | {9 series tags} |
| tutorials | g_price_beer_N | series | {9 series tags} |
| tutorials | g_price_beer_NE | series | {9 series tags} |
| tutorials | g_price_beer_NW | series | {9 series tags} |
| tutorials | g_price_beer_S | series | {9 series tags} |
| tutorials | g_price_beer_SE | series | {9 series tags} |
| tutorials | g_price_beer_SW | series | {9 series tags} |
| tutorials | g_price_beer_W | series | {9 series tags} |
| tutorials | g_price_coffee_E | series | {9 series tags} |
| tutorials | g_price_coffee_N | series | {9 series tags} |
| tutorials | g_price_coffee_NE | series | {9 series tags} |
| tutorials | g_price_coffee_NW | series | {9 series tags} |
| tutorials | g_price_coffee_S | series | {9 series tags} |
| tutorials | g_price_coffee_SE | series | {9 series tags} |
| tutorials | g_price_coffee_SW | series | {9 series tags} |
| tutorials | g_price_coffee_W | series | {9 series tags} |
| tutorials | g_price_soft-drinks_E | series | {9 series tags} |
| tutorials | g_price_soft-drinks_N | series | {9 series tags} |
| tutorials | g_price_soft-drinks_NE | series | {9 series tags} |
| tutorials | g_price_soft-drinks_NW | series | {9 series tags} |
| tutorials | g_price_soft-drinks_S | series | {9 series tags} |
| tutorials | g_price_soft-drinks_SE | series | {9 series tags} |
| tutorials | g_price_soft-drinks_SW | series | {9 series tags} |
| tutorials | g_price_soft-drinks_W | series | {9 series tags} |
| tutorials | g_price_tea_E | series | {9 series tags} |
| tutorials | g_price_tea_N | series | {9 series tags} |
| tutorials | g_price_tea_NE | series | {9 series tags} |
| tutorials | g_price_tea_NW | series | {9 series tags} |
| tutorials | g_price_tea_S | series | {9 series tags} |
| tutorials | g_price_tea_SE | series | {9 series tags} |
| tutorials | g_price_tea_SW | series | {9 series tags} |
| tutorials | g_price_tea_W | series | {9 series tags} |
| tutorials | g_price_wine_E | series | {9 series tags} |
| tutorials | g_price_wine_N | series | {9 series tags} |
| tutorials | g_price_wine_NE | series | {9 series tags} |
| tutorials | g_price_wine_NW | series | {9 series tags} |
| tutorials | g_price_wine_S | series | {9 series tags} |
| tutorials | g_price_wine_SE | series | {9 series tags} |
| tutorials | g_price_wine_SW | series | {9 series tags} |
| tutorials | g_price_wine_W | series | {9 series tags} |
| tutorials | g_volume_beer_E | series | {9 series tags} |
| tutorials | g_volume_beer_N | series | {9 series tags} |
| tutorials | g_volume_beer_NE | series | {9 series tags} |
| tutorials | g_volume_beer_NW | series | {9 series tags} |
| tutorials | g_volume_beer_S | series | {9 series tags} |
| tutorials | g_volume_beer_SE | series | {9 series tags} |
| tutorials | g_volume_beer_SW | series | {9 series tags} |
| tutorials | g_volume_beer_W | series | {9 series tags} |
| tutorials | g_volume_coffee_E | series | {9 series tags} |
| tutorials | g_volume_coffee_N | series | {9 series tags} |
| tutorials | g_volume_coffee_NE | series | {9 series tags} |
| tutorials | g_volume_coffee_NW | series | {9 series tags} |
| tutorials | g_volume_coffee_S | series | {9 series tags} |
| tutorials | g_volume_coffee_SE | series | {9 series tags} |
| tutorials | g_volume_coffee_SW | series | {9 series tags} |
| tutorials | g_volume_coffee_W | series | {9 series tags} |
| tutorials | g_volume_soft-drinks_E | series | {9 series tags} |
| tutorials | g_volume_soft-drinks_N | series | {9 series tags} |
| tutorials | g_volume_soft-drinks_NE | series | {9 series tags} |
| tutorials | g_volume_soft-drinks_NW | series | {9 series tags} |
| tutorials | g_volume_soft-drinks_S | series | {9 series tags} |
| tutorials | g_volume_soft-drinks_SE | series | {9 series tags} |
| tutorials | g_volume_soft-drinks_SW | series | {9 series tags} |
| tutorials | g_volume_soft-drinks_W | series | {9 series tags} |
| tutorials | g_volume_tea_E | series | {9 series tags} |
| tutorials | g_volume_tea_N | series | {9 series tags} |
| tutorials | g_volume_tea_NE | series | {9 series tags} |
| tutorials | g_volume_tea_NW | series | {9 series tags} |
| tutorials | g_volume_tea_S | series | {9 series tags} |
| tutorials | g_volume_tea_SE | series | {9 series tags} |
| tutorials | g_volume_tea_SW | series | {9 series tags} |
| tutorials | g_volume_tea_W | series | {9 series tags} |
| tutorials | g_volume_wine_E | series | {9 series tags} |
| tutorials | g_volume_wine_N | series | {9 series tags} |
| tutorials | g_volume_wine_NE | series | {9 series tags} |
| tutorials | g_volume_wine_NW | series | {9 series tags} |
| tutorials | g_volume_wine_S | series | {9 series tags} |
| tutorials | g_volume_wine_SE | series | {9 series tags} |
| tutorials | g_volume_wine_SW | series | {9 series tags} |
| tutorials | g_volume_wine_W | series | {9 series tags} |
| tutorials | h_price_beer_E | series | {9 series tags} |
| tutorials | h_price_beer_N | series | {9 series tags} |
| tutorials | h_price_beer_NE | series | {9 series tags} |
| tutorials | h_price_beer_NW | series | {9 series tags} |
| tutorials | h_price_beer_S | series | {9 series tags} |
| tutorials | h_price_beer_SE | series | {9 series tags} |
| tutorials | h_price_beer_SW | series | {9 series tags} |
| tutorials | h_price_beer_W | series | {9 series tags} |
| tutorials | h_price_coffee_E | series | {9 series tags} |
| tutorials | h_price_coffee_N | series | {9 series tags} |
| tutorials | h_price_coffee_NE | series | {9 series tags} |
| tutorials | h_price_coffee_NW | series | {9 series tags} |
| tutorials | h_price_coffee_S | series | {9 series tags} |
| tutorials | h_price_coffee_SE | series | {9 series tags} |
| tutorials | h_price_coffee_SW | series | {9 series tags} |
| tutorials | h_price_coffee_W | series | {9 series tags} |
| tutorials | h_price_soft-drinks_E | series | {9 series tags} |
| tutorials | h_price_soft-drinks_N | series | {9 series tags} |
| tutorials | h_price_soft-drinks_NE | series | {9 series tags} |
| tutorials | h_price_soft-drinks_NW | series | {9 series tags} |
| tutorials | h_price_soft-drinks_S | series | {9 series tags} |
| tutorials | h_price_soft-drinks_SE | series | {9 series tags} |
| tutorials | h_price_soft-drinks_SW | series | {9 series tags} |
| tutorials | h_price_soft-drinks_W | series | {9 series tags} |
| tutorials | h_price_tea_E | series | {9 series tags} |
| tutorials | h_price_tea_N | series | {9 series tags} |
| tutorials | h_price_tea_NE | series | {9 series tags} |
| tutorials | h_price_tea_NW | series | {9 series tags} |
| tutorials | h_price_tea_S | series | {9 series tags} |
| tutorials | h_price_tea_SE | series | {9 series tags} |
| tutorials | h_price_tea_SW | series | {9 series tags} |
| tutorials | h_price_tea_W | series | {9 series tags} |
| tutorials | h_price_wine_E | series | {9 series tags} |
| tutorials | h_price_wine_N | series | {9 series tags} |
| tutorials | h_price_wine_NE | series | {9 series tags} |
| tutorials | h_price_wine_NW | series | {9 series tags} |
| tutorials | h_price_wine_S | series | {9 series tags} |
| tutorials | h_price_wine_SE | series | {9 series tags} |
| tutorials | h_price_wine_SW | series | {9 series tags} |
| tutorials | h_price_wine_W | series | {9 series tags} |
| tutorials | h_volume_beer_E | series | {9 series tags} |
| tutorials | h_volume_beer_N | series | {9 series tags} |
| tutorials | h_volume_beer_NE | series | {9 series tags} |
| tutorials | h_volume_beer_NW | series | {9 series tags} |
| tutorials | h_volume_beer_S | series | {9 series tags} |
| tutorials | h_volume_beer_SE | series | {9 series tags} |
| tutorials | h_volume_beer_SW | series | {9 series tags} |
| tutorials | h_volume_beer_W | series | {9 series tags} |
| tutorials | h_volume_coffee_E | series | {9 series tags} |
| tutorials | h_volume_coffee_N | series | {9 series tags} |
| tutorials | h_volume_coffee_NE | series | {9 series tags} |
| tutorials | h_volume_coffee_NW | series | {9 series tags} |
| tutorials | h_volume_coffee_S | series | {9 series tags} |
| tutorials | h_volume_coffee_SE | series | {9 series tags} |
| tutorials | h_volume_coffee_SW | series | {9 series tags} |
| tutorials | h_volume_coffee_W | series | {9 series tags} |
| tutorials | h_volume_soft-drinks_E | series | {9 series tags} |
| tutorials | h_volume_soft-drinks_N | series | {9 series tags} |
| tutorials | h_volume_soft-drinks_NE | series | {9 series tags} |
| tutorials | h_volume_soft-drinks_NW | series | {9 series tags} |
| tutorials | h_volume_soft-drinks_S | series | {9 series tags} |
| tutorials | h_volume_soft-drinks_SE | series | {9 series tags} |
| tutorials | h_volume_soft-drinks_SW | series | {9 series tags} |
| tutorials | h_volume_soft-drinks_W | series | {9 series tags} |
| tutorials | h_volume_tea_E | series | {9 series tags} |
| tutorials | h_volume_tea_N | series | {9 series tags} |
| tutorials | h_volume_tea_NE | series | {9 series tags} |
| tutorials | h_volume_tea_NW | series | {9 series tags} |
| tutorials | h_volume_tea_S | series | {9 series tags} |
| tutorials | h_volume_tea_SE | series | {9 series tags} |
| tutorials | h_volume_tea_SW | series | {9 series tags} |
| tutorials | h_volume_tea_W | series | {9 series tags} |
| tutorials | h_volume_wine_E | series | {9 series tags} |
| tutorials | h_volume_wine_N | series | {9 series tags} |
| tutorials | h_volume_wine_NE | series | {9 series tags} |
| tutorials | h_volume_wine_NW | series | {9 series tags} |
| tutorials | h_volume_wine_S | series | {9 series tags} |
| tutorials | h_volume_wine_SE | series | {9 series tags} |
| tutorials | h_volume_wine_SW | series | {9 series tags} |
| tutorials | h_volume_wine_W | series | {9 series tags} |
| tutorials | i_price_beer_E | series | {9 series tags} |
| tutorials | i_price_beer_N | series | {9 series tags} |
| tutorials | i_price_beer_NE | series | {9 series tags} |
| tutorials | i_price_beer_NW | series | {9 series tags} |
| tutorials | i_price_beer_S | series | {9 series tags} |
| tutorials | i_price_beer_SE | series | {9 series tags} |
| tutorials | i_price_beer_SW | series | {9 series tags} |
| tutorials | i_price_beer_W | series | {9 series tags} |
| tutorials | i_price_coffee_E | series | {9 series tags} |
| tutorials | i_price_coffee_N | series | {9 series tags} |
| tutorials | i_price_coffee_NE | series | {9 series tags} |
| tutorials | i_price_coffee_NW | series | {9 series tags} |
| tutorials | i_price_coffee_S | series | {9 series tags} |
| tutorials | i_price_coffee_SE | series | {9 series tags} |
| tutorials | i_price_coffee_SW | series | {9 series tags} |
| tutorials | i_price_coffee_W | series | {9 series tags} |
| tutorials | i_price_soft-drinks_E | series | {9 series tags} |
| tutorials | i_price_soft-drinks_N | series | {9 series tags} |
| tutorials | i_price_soft-drinks_NE | series | {9 series tags} |
| tutorials | i_price_soft-drinks_NW | series | {9 series tags} |
| tutorials | i_price_soft-drinks_S | series | {9 series tags} |
| tutorials | i_price_soft-drinks_SE | series | {9 series tags} |
| tutorials | i_price_soft-drinks_SW | series | {9 series tags} |
| tutorials | i_price_soft-drinks_W | series | {9 series tags} |
| tutorials | i_price_tea_E | series | {9 series tags} |
| tutorials | i_price_tea_N | series | {9 series tags} |
| tutorials | i_price_tea_NE | series | {9 series tags} |
| tutorials | i_price_tea_NW | series | {9 series tags} |
| tutorials | i_price_tea_S | series | {9 series tags} |
| tutorials | i_price_tea_SE | series | {9 series tags} |
| tutorials | i_price_tea_SW | series | {9 series tags} |
| tutorials | i_price_tea_W | series | {9 series tags} |
| tutorials | i_price_wine_E | series | {9 series tags} |
| tutorials | i_price_wine_N | series | {9 series tags} |
| tutorials | i_price_wine_NE | series | {9 series tags} |
| tutorials | i_price_wine_NW | series | {9 series tags} |
| tutorials | i_price_wine_S | series | {9 series tags} |
| tutorials | i_price_wine_SE | series | {9 series tags} |
| tutorials | i_price_wine_SW | series | {9 series tags} |
| tutorials | i_price_wine_W | series | {9 series tags} |
| tutorials | i_volume_beer_E | series | {9 series tags} |
| tutorials | i_volume_beer_N | series | {9 series tags} |
| tutorials | i_volume_beer_NE | series | {9 series tags} |
| tutorials | i_volume_beer_NW | series | {9 series tags} |
| tutorials | i_volume_beer_S | series | {9 series tags} |
| tutorials | i_volume_beer_SE | series | {9 series tags} |
| tutorials | i_volume_beer_SW | series | {9 series tags} |
| tutorials | i_volume_beer_W | series | {9 series tags} |
| tutorials | i_volume_coffee_E | series | {9 series tags} |
| tutorials | i_volume_coffee_N | series | {9 series tags} |
| tutorials | i_volume_coffee_NE | series | {9 series tags} |
| tutorials | i_volume_coffee_NW | series | {9 series tags} |
| tutorials | i_volume_coffee_S | series | {9 series tags} |
| tutorials | i_volume_coffee_SE | series | {9 series tags} |
| tutorials | i_volume_coffee_SW | series | {9 series tags} |
| tutorials | i_volume_coffee_W | series | {9 series tags} |
| tutorials | i_volume_soft-drinks_E | series | {9 series tags} |
| tutorials | i_volume_soft-drinks_N | series | {9 series tags} |
| tutorials | i_volume_soft-drinks_NE | series | {9 series tags} |
| tutorials | i_volume_soft-drinks_NW | series | {9 series tags} |
| tutorials | i_volume_soft-drinks_S | series | {9 series tags} |
| tutorials | i_volume_soft-drinks_SE | series | {9 series tags} |
| tutorials | i_volume_soft-drinks_SW | series | {9 series tags} |
| tutorials | i_volume_soft-drinks_W | series | {9 series tags} |
| tutorials | i_volume_tea_E | series | {9 series tags} |
| tutorials | i_volume_tea_N | series | {9 series tags} |
| tutorials | i_volume_tea_NE | series | {9 series tags} |
| tutorials | i_volume_tea_NW | series | {9 series tags} |
| tutorials | i_volume_tea_S | series | {9 series tags} |
| tutorials | i_volume_tea_SE | series | {9 series tags} |
| tutorials | i_volume_tea_SW | series | {9 series tags} |
| tutorials | i_volume_tea_W | series | {9 series tags} |
| tutorials | i_volume_wine_E | series | {9 series tags} |
| tutorials | i_volume_wine_N | series | {9 series tags} |
| tutorials | i_volume_wine_NE | series | {9 series tags} |
| tutorials | i_volume_wine_NW | series | {9 series tags} |
| tutorials | i_volume_wine_S | series | {9 series tags} |
| tutorials | i_volume_wine_SE | series | {9 series tags} |
| tutorials | i_volume_wine_SW | series | {9 series tags} |
| tutorials | i_volume_wine_W | series | {9 series tags} |
| tutorials | j_price_beer_E | series | {9 series tags} |
| tutorials | j_price_beer_N | series | {9 series tags} |
| tutorials | j_price_beer_NE | series | {9 series tags} |
| tutorials | j_price_beer_NW | series | {9 series tags} |
| tutorials | j_price_beer_S | series | {9 series tags} |
| tutorials | j_price_beer_SE | series | {9 series tags} |
| tutorials | j_price_beer_SW | series | {9 series tags} |
| tutorials | j_price_beer_W | series | {9 series tags} |
| tutorials | j_price_coffee_E | series | {9 series tags} |
| tutorials | j_price_coffee_N | series | {9 series tags} |
| tutorials | j_price_coffee_NE | series | {9 series tags} |
| tutorials | j_price_coffee_NW | series | {9 series tags} |
| tutorials | j_price_coffee_S | series | {9 series tags} |
| tutorials | j_price_coffee_SE | series | {9 series tags} |
| tutorials | j_price_coffee_SW | series | {9 series tags} |
| tutorials | j_price_coffee_W | series | {9 series tags} |
| tutorials | j_price_soft-drinks_E | series | {9 series tags} |
| tutorials | j_price_soft-drinks_N | series | {9 series tags} |
| tutorials | j_price_soft-drinks_NE | series | {9 series tags} |
| tutorials | j_price_soft-drinks_NW | series | {9 series tags} |
| tutorials | j_price_soft-drinks_S | series | {9 series tags} |
| tutorials | j_price_soft-drinks_SE | series | {9 series tags} |
| tutorials | j_price_soft-drinks_SW | series | {9 series tags} |
| tutorials | j_price_soft-drinks_W | series | {9 series tags} |
| tutorials | j_price_tea_E | series | {9 series tags} |
| tutorials | j_price_tea_N | series | {9 series tags} |
| tutorials | j_price_tea_NE | series | {9 series tags} |
| tutorials | j_price_tea_NW | series | {9 series tags} |
| tutorials | j_price_tea_S | series | {9 series tags} |
| tutorials | j_price_tea_SE | series | {9 series tags} |
| tutorials | j_price_tea_SW | series | {9 series tags} |
| tutorials | j_price_tea_W | series | {9 series tags} |
| tutorials | j_price_wine_E | series | {9 series tags} |
| tutorials | j_price_wine_N | series | {9 series tags} |
| tutorials | j_price_wine_NE | series | {9 series tags} |
| tutorials | j_price_wine_NW | series | {9 series tags} |
| tutorials | j_price_wine_S | series | {9 series tags} |
| tutorials | j_price_wine_SE | series | {9 series tags} |
| tutorials | j_price_wine_SW | series | {9 series tags} |
| tutorials | j_price_wine_W | series | {9 series tags} |
| tutorials | j_volume_beer_E | series | {9 series tags} |
| tutorials | j_volume_beer_N | series | {9 series tags} |
| tutorials | j_volume_beer_NE | series | {9 series tags} |
| tutorials | j_volume_beer_NW | series | {9 series tags} |
| tutorials | j_volume_beer_S | series | {9 series tags} |
| tutorials | j_volume_beer_SE | series | {9 series tags} |
| tutorials | j_volume_beer_SW | series | {9 series tags} |
| tutorials | j_volume_beer_W | series | {9 series tags} |
| tutorials | j_volume_coffee_E | series | {9 series tags} |
| tutorials | j_volume_coffee_N | series | {9 series tags} |
| tutorials | j_volume_coffee_NE | series | {9 series tags} |
| tutorials | j_volume_coffee_NW | series | {9 series tags} |
| tutorials | j_volume_coffee_S | series | {9 series tags} |
| tutorials | j_volume_coffee_SE | series | {9 series tags} |
| tutorials | j_volume_coffee_SW | series | {9 series tags} |
| tutorials | j_volume_coffee_W | series | {9 series tags} |
| tutorials | j_volume_soft-drinks_E | series | {9 series tags} |
| tutorials | j_volume_soft-drinks_N | series | {9 series tags} |
| tutorials | j_volume_soft-drinks_NE | series | {9 series tags} |
| tutorials | j_volume_soft-drinks_NW | series | {9 series tags} |
| tutorials | j_volume_soft-drinks_S | series | {9 series tags} |
| tutorials | j_volume_soft-drinks_SE | series | {9 series tags} |
| tutorials | j_volume_soft-drinks_SW | series | {9 series tags} |
| tutorials | j_volume_soft-drinks_W | series | {9 series tags} |
| tutorials | j_volume_tea_E | series | {9 series tags} |
| tutorials | j_volume_tea_N | series | {9 series tags} |
| tutorials | j_volume_tea_NE | series | {9 series tags} |
| tutorials | j_volume_tea_NW | series | {9 series tags} |
| tutorials | j_volume_tea_S | series | {9 series tags} |
| tutorials | j_volume_tea_SE | series | {9 series tags} |
| tutorials | j_volume_tea_SW | series | {9 series tags} |
| tutorials | j_volume_tea_W | series | {9 series tags} |
| tutorials | j_volume_wine_E | series | {9 series tags} |
| tutorials | j_volume_wine_N | series | {9 series tags} |
| tutorials | j_volume_wine_NE | series | {9 series tags} |
| tutorials | j_volume_wine_NW | series | {9 series tags} |
| tutorials | j_volume_wine_S | series | {9 series tags} |
| tutorials | j_volume_wine_SE | series | {9 series tags} |
| tutorials | j_volume_wine_SW | series | {9 series tags} |
| tutorials | j_volume_wine_W | series | {9 series tags} |
| tutorials | k_price_beer_E | series | {9 series tags} |
| tutorials | k_price_beer_N | series | {9 series tags} |
| tutorials | k_price_beer_NE | series | {9 series tags} |
| tutorials | k_price_beer_NW | series | {9 series tags} |
| tutorials | k_price_beer_S | series | {9 series tags} |
| tutorials | k_price_beer_SE | series | {9 series tags} |
| tutorials | k_price_beer_SW | series | {9 series tags} |
| tutorials | k_price_beer_W | series | {9 series tags} |
| tutorials | k_price_coffee_E | series | {9 series tags} |
| tutorials | k_price_coffee_N | series | {9 series tags} |
| tutorials | k_price_coffee_NE | series | {9 series tags} |
| tutorials | k_price_coffee_NW | series | {9 series tags} |
| tutorials | k_price_coffee_S | series | {9 series tags} |
| tutorials | k_price_coffee_SE | series | {9 series tags} |
| tutorials | k_price_coffee_SW | series | {9 series tags} |
| tutorials | k_price_coffee_W | series | {9 series tags} |
| tutorials | k_price_soft-drinks_E | series | {9 series tags} |
| tutorials | k_price_soft-drinks_N | series | {9 series tags} |
| tutorials | k_price_soft-drinks_NE | series | {9 series tags} |
| tutorials | k_price_soft-drinks_NW | series | {9 series tags} |
| tutorials | k_price_soft-drinks_S | series | {9 series tags} |
| tutorials | k_price_soft-drinks_SE | series | {9 series tags} |
| tutorials | k_price_soft-drinks_SW | series | {9 series tags} |
| tutorials | k_price_soft-drinks_W | series | {9 series tags} |
| tutorials | k_price_tea_E | series | {9 series tags} |
| tutorials | k_price_tea_N | series | {9 series tags} |
| tutorials | k_price_tea_NE | series | {9 series tags} |
| tutorials | k_price_tea_NW | series | {9 series tags} |
| tutorials | k_price_tea_S | series | {9 series tags} |
| tutorials | k_price_tea_SE | series | {9 series tags} |
| tutorials | k_price_tea_SW | series | {9 series tags} |
| tutorials | k_price_tea_W | series | {9 series tags} |
| tutorials | k_price_wine_E | series | {9 series tags} |
| tutorials | k_price_wine_N | series | {9 series tags} |
| tutorials | k_price_wine_NE | series | {9 series tags} |
| tutorials | k_price_wine_NW | series | {9 series tags} |
| tutorials | k_price_wine_S | series | {9 series tags} |
| tutorials | k_price_wine_SE | series | {9 series tags} |
| tutorials | k_price_wine_SW | series | {9 series tags} |
| tutorials | k_price_wine_W | series | {9 series tags} |
| tutorials | k_volume_beer_E | series | {9 series tags} |
| tutorials | k_volume_beer_N | series | {9 series tags} |
| tutorials | k_volume_beer_NE | series | {9 series tags} |
| tutorials | k_volume_beer_NW | series | {9 series tags} |
| tutorials | k_volume_beer_S | series | {9 series tags} |
| tutorials | k_volume_beer_SE | series | {9 series tags} |
| tutorials | k_volume_beer_SW | series | {9 series tags} |
| tutorials | k_volume_beer_W | series | {9 series tags} |
| tutorials | k_volume_coffee_E | series | {9 series tags} |
| tutorials | k_volume_coffee_N | series | {9 series tags} |
| tutorials | k_volume_coffee_NE | series | {9 series tags} |
| tutorials | k_volume_coffee_NW | series | {9 series tags} |
| tutorials | k_volume_coffee_S | series | {9 series tags} |
| tutorials | k_volume_coffee_SE | series | {9 series tags} |
| tutorials | k_volume_coffee_SW | series | {9 series tags} |
| tutorials | k_volume_coffee_W | series | {9 series tags} |
| tutorials | k_volume_soft-drinks_E | series | {9 series tags} |
| tutorials | k_volume_soft-drinks_N | series | {9 series tags} |
| tutorials | k_volume_soft-drinks_NE | series | {9 series tags} |
| tutorials | k_volume_soft-drinks_NW | series | {9 series tags} |
| tutorials | k_volume_soft-drinks_S | series | {9 series tags} |
| tutorials | k_volume_soft-drinks_SE | series | {9 series tags} |
| tutorials | k_volume_soft-drinks_SW | series | {9 series tags} |
| tutorials | k_volume_soft-drinks_W | series | {9 series tags} |
| tutorials | k_volume_tea_E | series | {9 series tags} |
| tutorials | k_volume_tea_N | series | {9 series tags} |
| tutorials | k_volume_tea_NE | series | {9 series tags} |
| tutorials | k_volume_tea_NW | series | {9 series tags} |
| tutorials | k_volume_tea_S | series | {9 series tags} |
| tutorials | k_volume_tea_SE | series | {9 series tags} |
| tutorials | k_volume_tea_SW | series | {9 series tags} |
| tutorials | k_volume_tea_W | series | {9 series tags} |
| tutorials | k_volume_wine_E | series | {9 series tags} |
| tutorials | k_volume_wine_N | series | {9 series tags} |
| tutorials | k_volume_wine_NE | series | {9 series tags} |
| tutorials | k_volume_wine_NW | series | {9 series tags} |
| tutorials | k_volume_wine_S | series | {9 series tags} |
| tutorials | k_volume_wine_SE | series | {9 series tags} |
| tutorials | k_volume_wine_SW | series | {9 series tags} |
| tutorials | k_volume_wine_W | series | {9 series tags} |
| tutorials | l_price_beer_E | series | {9 series tags} |
| tutorials | l_price_beer_N | series | {9 series tags} |
| tutorials | l_price_beer_NE | series | {9 series tags} |
| tutorials | l_price_beer_NW | series | {9 series tags} |
| tutorials | l_price_beer_S | series | {9 series tags} |
| tutorials | l_price_beer_SE | series | {9 series tags} |
| tutorials | l_price_beer_SW | series | {9 series tags} |
| tutorials | l_price_beer_W | series | {9 series tags} |
| tutorials | l_price_coffee_E | series | {9 series tags} |
| tutorials | l_price_coffee_N | series | {9 series tags} |
| tutorials | l_price_coffee_NE | series | {9 series tags} |
| tutorials | l_price_coffee_NW | series | {9 series tags} |
| tutorials | l_price_coffee_S | series | {9 series tags} |
| tutorials | l_price_coffee_SE | series | {9 series tags} |
| tutorials | l_price_coffee_SW | series | {9 series tags} |
| tutorials | l_price_coffee_W | series | {9 series tags} |
| tutorials | l_price_soft-drinks_E | series | {9 series tags} |
| tutorials | l_price_soft-drinks_N | series | {9 series tags} |
| tutorials | l_price_soft-drinks_NE | series | {9 series tags} |
| tutorials | l_price_soft-drinks_NW | series | {9 series tags} |
| tutorials | l_price_soft-drinks_S | series | {9 series tags} |
| tutorials | l_price_soft-drinks_SE | series | {9 series tags} |
| tutorials | l_price_soft-drinks_SW | series | {9 series tags} |
| tutorials | l_price_soft-drinks_W | series | {9 series tags} |
| tutorials | l_price_tea_E | series | {9 series tags} |
| tutorials | l_price_tea_N | series | {9 series tags} |
| tutorials | l_price_tea_NE | series | {9 series tags} |
| tutorials | l_price_tea_NW | series | {9 series tags} |
| tutorials | l_price_tea_S | series | {9 series tags} |
| tutorials | l_price_tea_SE | series | {9 series tags} |
| tutorials | l_price_tea_SW | series | {9 series tags} |
| tutorials | l_price_tea_W | series | {9 series tags} |
| tutorials | l_price_wine_E | series | {9 series tags} |
| tutorials | l_price_wine_N | series | {9 series tags} |
| tutorials | l_price_wine_NE | series | {9 series tags} |
| tutorials | l_price_wine_NW | series | {9 series tags} |
| tutorials | l_price_wine_S | series | {9 series tags} |
| tutorials | l_price_wine_SE | series | {9 series tags} |
| tutorials | l_price_wine_SW | series | {9 series tags} |
| tutorials | l_price_wine_W | series | {9 series tags} |
| tutorials | l_volume_beer_E | series | {9 series tags} |
| tutorials | l_volume_beer_N | series | {9 series tags} |
| tutorials | l_volume_beer_NE | series | {9 series tags} |
| tutorials | l_volume_beer_NW | series | {9 series tags} |
| tutorials | l_volume_beer_S | series | {9 series tags} |
| tutorials | l_volume_beer_SE | series | {9 series tags} |
| tutorials | l_volume_beer_SW | series | {9 series tags} |
| tutorials | l_volume_beer_W | series | {9 series tags} |
| tutorials | l_volume_coffee_E | series | {9 series tags} |
| tutorials | l_volume_coffee_N | series | {9 series tags} |
| tutorials | l_volume_coffee_NE | series | {9 series tags} |
| tutorials | l_volume_coffee_NW | series | {9 series tags} |
| tutorials | l_volume_coffee_S | series | {9 series tags} |
| tutorials | l_volume_coffee_SE | series | {9 series tags} |
| tutorials | l_volume_coffee_SW | series | {9 series tags} |
| tutorials | l_volume_coffee_W | series | {9 series tags} |
| tutorials | l_volume_soft-drinks_E | series | {9 series tags} |
| tutorials | l_volume_soft-drinks_N | series | {9 series tags} |
| tutorials | l_volume_soft-drinks_NE | series | {9 series tags} |
| tutorials | l_volume_soft-drinks_NW | series | {9 series tags} |
| tutorials | l_volume_soft-drinks_S | series | {9 series tags} |
| tutorials | l_volume_soft-drinks_SE | series | {9 series tags} |
| tutorials | l_volume_soft-drinks_SW | series | {9 series tags} |
| tutorials | l_volume_soft-drinks_W | series | {9 series tags} |
| tutorials | l_volume_tea_E | series | {9 series tags} |
| tutorials | l_volume_tea_N | series | {9 series tags} |
| tutorials | l_volume_tea_NE | series | {9 series tags} |
| tutorials | l_volume_tea_NW | series | {9 series tags} |
| tutorials | l_volume_tea_S | series | {9 series tags} |
| tutorials | l_volume_tea_SE | series | {9 series tags} |
| tutorials | l_volume_tea_SW | series | {9 series tags} |
| tutorials | l_volume_tea_W | series | {9 series tags} |
| tutorials | l_volume_wine_E | series | {9 series tags} |
| tutorials | l_volume_wine_N | series | {9 series tags} |
| tutorials | l_volume_wine_NE | series | {9 series tags} |
| tutorials | l_volume_wine_NW | series | {9 series tags} |
| tutorials | l_volume_wine_S | series | {9 series tags} |
| tutorials | l_volume_wine_SE | series | {9 series tags} |
| tutorials | l_volume_wine_SW | series | {9 series tags} |
| tutorials | l_volume_wine_W | series | {9 series tags} |
| tutorials | m_price_beer_E | series | {9 series tags} |
| tutorials | m_price_beer_N | series | {9 series tags} |
| tutorials | m_price_beer_NE | series | {9 series tags} |
| tutorials | m_price_beer_NW | series | {9 series tags} |
| tutorials | m_price_beer_S | series | {9 series tags} |
| tutorials | m_price_beer_SE | series | {9 series tags} |
| tutorials | m_price_beer_SW | series | {9 series tags} |
| tutorials | m_price_beer_W | series | {9 series tags} |
| tutorials | m_price_coffee_E | series | {9 series tags} |
| tutorials | m_price_coffee_N | series | {9 series tags} |
| tutorials | m_price_coffee_NE | series | {9 series tags} |
| tutorials | m_price_coffee_NW | series | {9 series tags} |
| tutorials | m_price_coffee_S | series | {9 series tags} |
| tutorials | m_price_coffee_SE | series | {9 series tags} |
| tutorials | m_price_coffee_SW | series | {9 series tags} |
| tutorials | m_price_coffee_W | series | {9 series tags} |
| tutorials | m_price_soft-drinks_E | series | {9 series tags} |
| tutorials | m_price_soft-drinks_N | series | {9 series tags} |
| tutorials | m_price_soft-drinks_NE | series | {9 series tags} |
| tutorials | m_price_soft-drinks_NW | series | {9 series tags} |
| tutorials | m_price_soft-drinks_S | series | {9 series tags} |
| tutorials | m_price_soft-drinks_SE | series | {9 series tags} |
| tutorials | m_price_soft-drinks_SW | series | {9 series tags} |
| tutorials | m_price_soft-drinks_W | series | {9 series tags} |
| tutorials | m_price_tea_E | series | {9 series tags} |
| tutorials | m_price_tea_N | series | {9 series tags} |
| tutorials | m_price_tea_NE | series | {9 series tags} |
| tutorials | m_price_tea_NW | series | {9 series tags} |
| tutorials | m_price_tea_S | series | {9 series tags} |
| tutorials | m_price_tea_SE | series | {9 series tags} |
| tutorials | m_price_tea_SW | series | {9 series tags} |
| tutorials | m_price_tea_W | series | {9 series tags} |
| tutorials | m_price_wine_E | series | {9 series tags} |
| tutorials | m_price_wine_N | series | {9 series tags} |
| tutorials | m_price_wine_NE | series | {9 series tags} |
| tutorials | m_price_wine_NW | series | {9 series tags} |
| tutorials | m_price_wine_S | series | {9 series tags} |
| tutorials | m_price_wine_SE | series | {9 series tags} |
| tutorials | m_price_wine_SW | series | {9 series tags} |
| tutorials | m_price_wine_W | series | {9 series tags} |
| tutorials | m_volume_beer_E | series | {9 series tags} |
| tutorials | m_volume_beer_N | series | {9 series tags} |
| tutorials | m_volume_beer_NE | series | {9 series tags} |
| tutorials | m_volume_beer_NW | series | {9 series tags} |
| tutorials | m_volume_beer_S | series | {9 series tags} |
| tutorials | m_volume_beer_SE | series | {9 series tags} |
| tutorials | m_volume_beer_SW | series | {9 series tags} |
| tutorials | m_volume_beer_W | series | {9 series tags} |
| tutorials | m_volume_coffee_E | series | {9 series tags} |
| tutorials | m_volume_coffee_N | series | {9 series tags} |
| tutorials | m_volume_coffee_NE | series | {9 series tags} |
| tutorials | m_volume_coffee_NW | series | {9 series tags} |
| tutorials | m_volume_coffee_S | series | {9 series tags} |
| tutorials | m_volume_coffee_SE | series | {9 series tags} |
| tutorials | m_volume_coffee_SW | series | {9 series tags} |
| tutorials | m_volume_coffee_W | series | {9 series tags} |
| tutorials | m_volume_soft-drinks_E | series | {9 series tags} |
| tutorials | m_volume_soft-drinks_N | series | {9 series tags} |
| tutorials | m_volume_soft-drinks_NE | series | {9 series tags} |
| tutorials | m_volume_soft-drinks_NW | series | {9 series tags} |
| tutorials | m_volume_soft-drinks_S | series | {9 series tags} |
| tutorials | m_volume_soft-drinks_SE | series | {9 series tags} |
| tutorials | m_volume_soft-drinks_SW | series | {9 series tags} |
| tutorials | m_volume_soft-drinks_W | series | {9 series tags} |
| tutorials | m_volume_tea_E | series | {9 series tags} |
| tutorials | m_volume_tea_N | series | {9 series tags} |
| tutorials | m_volume_tea_NE | series | {9 series tags} |
| tutorials | m_volume_tea_NW | series | {9 series tags} |
| tutorials | m_volume_tea_S | series | {9 series tags} |
| tutorials | m_volume_tea_SE | series | {9 series tags} |
| tutorials | m_volume_tea_SW | series | {9 series tags} |
| tutorials | m_volume_tea_W | series | {9 series tags} |
| tutorials | m_volume_wine_E | series | {9 series tags} |
| tutorials | m_volume_wine_N | series | {9 series tags} |
| tutorials | m_volume_wine_NE | series | {9 series tags} |
| tutorials | m_volume_wine_NW | series | {9 series tags} |
| tutorials | m_volume_wine_S | series | {9 series tags} |
| tutorials | m_volume_wine_SE | series | {9 series tags} |
| tutorials | m_volume_wine_SW | series | {9 series tags} |
| tutorials | m_volume_wine_W | series | {9 series tags} |
| tutorials | n_price_beer_E | series | {9 series tags} |
| tutorials | n_price_beer_N | series | {9 series tags} |
| tutorials | n_price_beer_NE | series | {9 series tags} |
| tutorials | n_price_beer_NW | series | {9 series tags} |
| tutorials | n_price_beer_S | series | {9 series tags} |
| tutorials | n_price_beer_SE | series | {9 series tags} |
| tutorials | n_price_beer_SW | series | {9 series tags} |
| tutorials | n_price_beer_W | series | {9 series tags} |
| tutorials | n_price_coffee_E | series | {9 series tags} |
| tutorials | n_price_coffee_N | series | {9 series tags} |
| tutorials | n_price_coffee_NE | series | {9 series tags} |
| tutorials | n_price_coffee_NW | series | {9 series tags} |
| tutorials | n_price_coffee_S | series | {9 series tags} |
| tutorials | n_price_coffee_SE | series | {9 series tags} |
| tutorials | n_price_coffee_SW | series | {9 series tags} |
| tutorials | n_price_coffee_W | series | {9 series tags} |
| tutorials | n_price_soft-drinks_E | series | {9 series tags} |
| tutorials | n_price_soft-drinks_N | series | {9 series tags} |
| tutorials | n_price_soft-drinks_NE | series | {9 series tags} |
| tutorials | n_price_soft-drinks_NW | series | {9 series tags} |
| tutorials | n_price_soft-drinks_S | series | {9 series tags} |
| tutorials | n_price_soft-drinks_SE | series | {9 series tags} |
| tutorials | n_price_soft-drinks_SW | series | {9 series tags} |
| tutorials | n_price_soft-drinks_W | series | {9 series tags} |
| tutorials | n_price_tea_E | series | {9 series tags} |
| tutorials | n_price_tea_N | series | {9 series tags} |
| tutorials | n_price_tea_NE | series | {9 series tags} |
| tutorials | n_price_tea_NW | series | {9 series tags} |
| tutorials | n_price_tea_S | series | {9 series tags} |
| tutorials | n_price_tea_SE | series | {9 series tags} |
| tutorials | n_price_tea_SW | series | {9 series tags} |
| tutorials | n_price_tea_W | series | {9 series tags} |
| tutorials | n_price_wine_E | series | {9 series tags} |
| tutorials | n_price_wine_N | series | {9 series tags} |
| tutorials | n_price_wine_NE | series | {9 series tags} |
| tutorials | n_price_wine_NW | series | {9 series tags} |
| tutorials | n_price_wine_S | series | {9 series tags} |
| tutorials | n_price_wine_SE | series | {9 series tags} |
| tutorials | n_price_wine_SW | series | {9 series tags} |
| tutorials | n_price_wine_W | series | {9 series tags} |
| tutorials | n_volume_beer_E | series | {9 series tags} |
| tutorials | n_volume_beer_N | series | {9 series tags} |
| tutorials | n_volume_beer_NE | series | {9 series tags} |
| tutorials | n_volume_beer_NW | series | {9 series tags} |
| tutorials | n_volume_beer_S | series | {9 series tags} |
| tutorials | n_volume_beer_SE | series | {9 series tags} |
| tutorials | n_volume_beer_SW | series | {9 series tags} |
| tutorials | n_volume_beer_W | series | {9 series tags} |
| tutorials | n_volume_coffee_E | series | {9 series tags} |
| tutorials | n_volume_coffee_N | series | {9 series tags} |
| tutorials | n_volume_coffee_NE | series | {9 series tags} |
| tutorials | n_volume_coffee_NW | series | {9 series tags} |
| tutorials | n_volume_coffee_S | series | {9 series tags} |
| tutorials | n_volume_coffee_SE | series | {9 series tags} |
| tutorials | n_volume_coffee_SW | series | {9 series tags} |
| tutorials | n_volume_coffee_W | series | {9 series tags} |
| tutorials | n_volume_soft-drinks_E | series | {9 series tags} |
| tutorials | n_volume_soft-drinks_N | series | {9 series tags} |
| tutorials | n_volume_soft-drinks_NE | series | {9 series tags} |
| tutorials | n_volume_soft-drinks_NW | series | {9 series tags} |
| tutorials | n_volume_soft-drinks_S | series | {9 series tags} |
| tutorials | n_volume_soft-drinks_SE | series | {9 series tags} |
| tutorials | n_volume_soft-drinks_SW | series | {9 series tags} |
| tutorials | n_volume_soft-drinks_W | series | {9 series tags} |
| tutorials | n_volume_tea_E | series | {9 series tags} |
| tutorials | n_volume_tea_N | series | {9 series tags} |
| tutorials | n_volume_tea_NE | series | {9 series tags} |
| tutorials | n_volume_tea_NW | series | {9 series tags} |
| tutorials | n_volume_tea_S | series | {9 series tags} |
| tutorials | n_volume_tea_SE | series | {9 series tags} |
| tutorials | n_volume_tea_SW | series | {9 series tags} |
| tutorials | n_volume_tea_W | series | {9 series tags} |
| tutorials | n_volume_wine_E | series | {9 series tags} |
| tutorials | n_volume_wine_N | series | {9 series tags} |
| tutorials | n_volume_wine_NE | series | {9 series tags} |
| tutorials | n_volume_wine_NW | series | {9 series tags} |
| tutorials | n_volume_wine_S | series | {9 series tags} |
| tutorials | n_volume_wine_SE | series | {9 series tags} |
| tutorials | n_volume_wine_SW | series | {9 series tags} |
| tutorials | n_volume_wine_W | series | {9 series tags} |
| tutorials | o_price_beer_E | series | {9 series tags} |
| tutorials | o_price_beer_N | series | {9 series tags} |
| tutorials | o_price_beer_NE | series | {9 series tags} |
| tutorials | o_price_beer_NW | series | {9 series tags} |
| tutorials | o_price_beer_S | series | {9 series tags} |
| tutorials | o_price_beer_SE | series | {9 series tags} |
| tutorials | o_price_beer_SW | series | {9 series tags} |
| tutorials | o_price_beer_W | series | {9 series tags} |
| tutorials | o_price_coffee_E | series | {9 series tags} |
| tutorials | o_price_coffee_N | series | {9 series tags} |
| tutorials | o_price_coffee_NE | series | {9 series tags} |
| tutorials | o_price_coffee_NW | series | {9 series tags} |
| tutorials | o_price_coffee_S | series | {9 series tags} |
| tutorials | o_price_coffee_SE | series | {9 series tags} |
| tutorials | o_price_coffee_SW | series | {9 series tags} |
| tutorials | o_price_coffee_W | series | {9 series tags} |
| tutorials | o_price_soft-drinks_E | series | {9 series tags} |
| tutorials | o_price_soft-drinks_N | series | {9 series tags} |
| tutorials | o_price_soft-drinks_NE | series | {9 series tags} |
| tutorials | o_price_soft-drinks_NW | series | {9 series tags} |
| tutorials | o_price_soft-drinks_S | series | {9 series tags} |
| tutorials | o_price_soft-drinks_SE | series | {9 series tags} |
| tutorials | o_price_soft-drinks_SW | series | {9 series tags} |
| tutorials | o_price_soft-drinks_W | series | {9 series tags} |
| tutorials | o_price_tea_E | series | {9 series tags} |
| tutorials | o_price_tea_N | series | {9 series tags} |
| tutorials | o_price_tea_NE | series | {9 series tags} |
| tutorials | o_price_tea_NW | series | {9 series tags} |
| tutorials | o_price_tea_S | series | {9 series tags} |
| tutorials | o_price_tea_SE | series | {9 series tags} |
| tutorials | o_price_tea_SW | series | {9 series tags} |
| tutorials | o_price_tea_W | series | {9 series tags} |
| tutorials | o_price_wine_E | series | {9 series tags} |
| tutorials | o_price_wine_N | series | {9 series tags} |
| tutorials | o_price_wine_NE | series | {9 series tags} |
| tutorials | o_price_wine_NW | series | {9 series tags} |
| tutorials | o_price_wine_S | series | {9 series tags} |
| tutorials | o_price_wine_SE | series | {9 series tags} |
| tutorials | o_price_wine_SW | series | {9 series tags} |
| tutorials | o_price_wine_W | series | {9 series tags} |
| tutorials | o_volume_beer_E | series | {9 series tags} |
| tutorials | o_volume_beer_N | series | {9 series tags} |
| tutorials | o_volume_beer_NE | series | {9 series tags} |
| tutorials | o_volume_beer_NW | series | {9 series tags} |
| tutorials | o_volume_beer_S | series | {9 series tags} |
| tutorials | o_volume_beer_SE | series | {9 series tags} |
| tutorials | o_volume_beer_SW | series | {9 series tags} |
| tutorials | o_volume_beer_W | series | {9 series tags} |
| tutorials | o_volume_coffee_E | series | {9 series tags} |
| tutorials | o_volume_coffee_N | series | {9 series tags} |
| tutorials | o_volume_coffee_NE | series | {9 series tags} |
| tutorials | o_volume_coffee_NW | series | {9 series tags} |
| tutorials | o_volume_coffee_S | series | {9 series tags} |
| tutorials | o_volume_coffee_SE | series | {9 series tags} |
| tutorials | o_volume_coffee_SW | series | {9 series tags} |
| tutorials | o_volume_coffee_W | series | {9 series tags} |
| tutorials | o_volume_soft-drinks_E | series | {9 series tags} |
| tutorials | o_volume_soft-drinks_N | series | {9 series tags} |
| tutorials | o_volume_soft-drinks_NE | series | {9 series tags} |
| tutorials | o_volume_soft-drinks_NW | series | {9 series tags} |
| tutorials | o_volume_soft-drinks_S | series | {9 series tags} |
| tutorials | o_volume_soft-drinks_SE | series | {9 series tags} |
| tutorials | o_volume_soft-drinks_SW | series | {9 series tags} |
| tutorials | o_volume_soft-drinks_W | series | {9 series tags} |
| tutorials | o_volume_tea_E | series | {9 series tags} |
| tutorials | o_volume_tea_N | series | {9 series tags} |
| tutorials | o_volume_tea_NE | series | {9 series tags} |
| tutorials | o_volume_tea_NW | series | {9 series tags} |
| tutorials | o_volume_tea_S | series | {9 series tags} |
| tutorials | o_volume_tea_SE | series | {9 series tags} |
| tutorials | o_volume_tea_SW | series | {9 series tags} |
| tutorials | o_volume_tea_W | series | {9 series tags} |
| tutorials | o_volume_wine_E | series | {9 series tags} |
| tutorials | o_volume_wine_N | series | {9 series tags} |
| tutorials | o_volume_wine_NE | series | {9 series tags} |
| tutorials | o_volume_wine_NW | series | {9 series tags} |
| tutorials | o_volume_wine_S | series | {9 series tags} |
| tutorials | o_volume_wine_SE | series | {9 series tags} |
| tutorials | o_volume_wine_SW | series | {9 series tags} |
| tutorials | o_volume_wine_W | series | {9 series tags} |
| tutorials | p_price_beer_E | series | {9 series tags} |
| tutorials | p_price_beer_N | series | {9 series tags} |
| tutorials | p_price_beer_NE | series | {9 series tags} |
| tutorials | p_price_beer_NW | series | {9 series tags} |
| tutorials | p_price_beer_S | series | {9 series tags} |
| tutorials | p_price_beer_SE | series | {9 series tags} |
| tutorials | p_price_beer_SW | series | {9 series tags} |
| tutorials | p_price_beer_W | series | {9 series tags} |
| tutorials | p_price_coffee_E | series | {9 series tags} |
| tutorials | p_price_coffee_N | series | {9 series tags} |
| tutorials | p_price_coffee_NE | series | {9 series tags} |
| tutorials | p_price_coffee_NW | series | {9 series tags} |
| tutorials | p_price_coffee_S | series | {9 series tags} |
| tutorials | p_price_coffee_SE | series | {9 series tags} |
| tutorials | p_price_coffee_SW | series | {9 series tags} |
| tutorials | p_price_coffee_W | series | {9 series tags} |
| tutorials | p_price_soft-drinks_E | series | {9 series tags} |
| tutorials | p_price_soft-drinks_N | series | {9 series tags} |
| tutorials | p_price_soft-drinks_NE | series | {9 series tags} |
| tutorials | p_price_soft-drinks_NW | series | {9 series tags} |
| tutorials | p_price_soft-drinks_S | series | {9 series tags} |
| tutorials | p_price_soft-drinks_SE | series | {9 series tags} |
| tutorials | p_price_soft-drinks_SW | series | {9 series tags} |
| tutorials | p_price_soft-drinks_W | series | {9 series tags} |
| tutorials | p_price_tea_E | series | {9 series tags} |
| tutorials | p_price_tea_N | series | {9 series tags} |
| tutorials | p_price_tea_NE | series | {9 series tags} |
| tutorials | p_price_tea_NW | series | {9 series tags} |
| tutorials | p_price_tea_S | series | {9 series tags} |
| tutorials | p_price_tea_SE | series | {9 series tags} |
| tutorials | p_price_tea_SW | series | {9 series tags} |
| tutorials | p_price_tea_W | series | {9 series tags} |
| tutorials | p_price_wine_E | series | {9 series tags} |
| tutorials | p_price_wine_N | series | {9 series tags} |
| tutorials | p_price_wine_NE | series | {9 series tags} |
| tutorials | p_price_wine_NW | series | {9 series tags} |
| tutorials | p_price_wine_S | series | {9 series tags} |
| tutorials | p_price_wine_SE | series | {9 series tags} |
| tutorials | p_price_wine_SW | series | {9 series tags} |
| tutorials | p_price_wine_W | series | {9 series tags} |
| tutorials | p_volume_beer_E | series | {9 series tags} |
| tutorials | p_volume_beer_N | series | {9 series tags} |
| tutorials | p_volume_beer_NE | series | {9 series tags} |
| tutorials | p_volume_beer_NW | series | {9 series tags} |
| tutorials | p_volume_beer_S | series | {9 series tags} |
| tutorials | p_volume_beer_SE | series | {9 series tags} |
| tutorials | p_volume_beer_SW | series | {9 series tags} |
| tutorials | p_volume_beer_W | series | {9 series tags} |
| tutorials | p_volume_coffee_E | series | {9 series tags} |
| tutorials | p_volume_coffee_N | series | {9 series tags} |
| tutorials | p_volume_coffee_NE | series | {9 series tags} |
| tutorials | p_volume_coffee_NW | series | {9 series tags} |
| tutorials | p_volume_coffee_S | series | {9 series tags} |
| tutorials | p_volume_coffee_SE | series | {9 series tags} |
| tutorials | p_volume_coffee_SW | series | {9 series tags} |
| tutorials | p_volume_coffee_W | series | {9 series tags} |
| tutorials | p_volume_soft-drinks_E | series | {9 series tags} |
| tutorials | p_volume_soft-drinks_N | series | {9 series tags} |
| tutorials | p_volume_soft-drinks_NE | series | {9 series tags} |
| tutorials | p_volume_soft-drinks_NW | series | {9 series tags} |
| tutorials | p_volume_soft-drinks_S | series | {9 series tags} |
| tutorials | p_volume_soft-drinks_SE | series | {9 series tags} |
| tutorials | p_volume_soft-drinks_SW | series | {9 series tags} |
| tutorials | p_volume_soft-drinks_W | series | {9 series tags} |
| tutorials | p_volume_tea_E | series | {9 series tags} |
| tutorials | p_volume_tea_N | series | {9 series tags} |
| tutorials | p_volume_tea_NE | series | {9 series tags} |
| tutorials | p_volume_tea_NW | series | {9 series tags} |
| tutorials | p_volume_tea_S | series | {9 series tags} |
| tutorials | p_volume_tea_SE | series | {9 series tags} |
| tutorials | p_volume_tea_SW | series | {9 series tags} |
| tutorials | p_volume_tea_W | series | {9 series tags} |
| tutorials | p_volume_wine_E | series | {9 series tags} |
| tutorials | p_volume_wine_N | series | {9 series tags} |
| tutorials | p_volume_wine_NE | series | {9 series tags} |
| tutorials | p_volume_wine_NW | series | {9 series tags} |
| tutorials | p_volume_wine_S | series | {9 series tags} |
| tutorials | p_volume_wine_SE | series | {9 series tags} |
| tutorials | p_volume_wine_SW | series | {9 series tags} |
| tutorials | p_volume_wine_W | series | {9 series tags} |
| tutorials | q_price_beer_E | series | {9 series tags} |
| tutorials | q_price_beer_N | series | {9 series tags} |
| tutorials | q_price_beer_NE | series | {9 series tags} |
| tutorials | q_price_beer_NW | series | {9 series tags} |
| tutorials | q_price_beer_S | series | {9 series tags} |
| tutorials | q_price_beer_SE | series | {9 series tags} |
| tutorials | q_price_beer_SW | series | {9 series tags} |
| tutorials | q_price_beer_W | series | {9 series tags} |
| tutorials | q_price_coffee_E | series | {9 series tags} |
| tutorials | q_price_coffee_N | series | {9 series tags} |
| tutorials | q_price_coffee_NE | series | {9 series tags} |
| tutorials | q_price_coffee_NW | series | {9 series tags} |
| tutorials | q_price_coffee_S | series | {9 series tags} |
| tutorials | q_price_coffee_SE | series | {9 series tags} |
| tutorials | q_price_coffee_SW | series | {9 series tags} |
| tutorials | q_price_coffee_W | series | {9 series tags} |
| tutorials | q_price_soft-drinks_E | series | {9 series tags} |
| tutorials | q_price_soft-drinks_N | series | {9 series tags} |
| tutorials | q_price_soft-drinks_NE | series | {9 series tags} |
| tutorials | q_price_soft-drinks_NW | series | {9 series tags} |
| tutorials | q_price_soft-drinks_S | series | {9 series tags} |
| tutorials | q_price_soft-drinks_SE | series | {9 series tags} |
| tutorials | q_price_soft-drinks_SW | series | {9 series tags} |
| tutorials | q_price_soft-drinks_W | series | {9 series tags} |
| tutorials | q_price_tea_E | series | {9 series tags} |
| tutorials | q_price_tea_N | series | {9 series tags} |
| tutorials | q_price_tea_NE | series | {9 series tags} |
| tutorials | q_price_tea_NW | series | {9 series tags} |
| tutorials | q_price_tea_S | series | {9 series tags} |
| tutorials | q_price_tea_SE | series | {9 series tags} |
| tutorials | q_price_tea_SW | series | {9 series tags} |
| tutorials | q_price_tea_W | series | {9 series tags} |
| tutorials | q_price_wine_E | series | {9 series tags} |
| tutorials | q_price_wine_N | series | {9 series tags} |
| tutorials | q_price_wine_NE | series | {9 series tags} |
| tutorials | q_price_wine_NW | series | {9 series tags} |
| tutorials | q_price_wine_S | series | {9 series tags} |
| tutorials | q_price_wine_SE | series | {9 series tags} |
| tutorials | q_price_wine_SW | series | {9 series tags} |
| tutorials | q_price_wine_W | series | {9 series tags} |
| tutorials | q_volume_beer_E | series | {9 series tags} |
| tutorials | q_volume_beer_N | series | {9 series tags} |
| tutorials | q_volume_beer_NE | series | {9 series tags} |
| tutorials | q_volume_beer_NW | series | {9 series tags} |
| tutorials | q_volume_beer_S | series | {9 series tags} |
| tutorials | q_volume_beer_SE | series | {9 series tags} |
| tutorials | q_volume_beer_SW | series | {9 series tags} |
| tutorials | q_volume_beer_W | series | {9 series tags} |
| tutorials | q_volume_coffee_E | series | {9 series tags} |
| tutorials | q_volume_coffee_N | series | {9 series tags} |
| tutorials | q_volume_coffee_NE | series | {9 series tags} |
| tutorials | q_volume_coffee_NW | series | {9 series tags} |
| tutorials | q_volume_coffee_S | series | {9 series tags} |
| tutorials | q_volume_coffee_SE | series | {9 series tags} |
| tutorials | q_volume_coffee_SW | series | {9 series tags} |
| tutorials | q_volume_coffee_W | series | {9 series tags} |
| tutorials | q_volume_soft-drinks_E | series | {9 series tags} |
| tutorials | q_volume_soft-drinks_N | series | {9 series tags} |
| tutorials | q_volume_soft-drinks_NE | series | {9 series tags} |
| tutorials | q_volume_soft-drinks_NW | series | {9 series tags} |
| tutorials | q_volume_soft-drinks_S | series | {9 series tags} |
| tutorials | q_volume_soft-drinks_SE | series | {9 series tags} |
| tutorials | q_volume_soft-drinks_SW | series | {9 series tags} |
| tutorials | q_volume_soft-drinks_W | series | {9 series tags} |
| tutorials | q_volume_tea_E | series | {9 series tags} |
| tutorials | q_volume_tea_N | series | {9 series tags} |
| tutorials | q_volume_tea_NE | series | {9 series tags} |
| tutorials | q_volume_tea_NW | series | {9 series tags} |
| tutorials | q_volume_tea_S | series | {9 series tags} |
| tutorials | q_volume_tea_SE | series | {9 series tags} |
| tutorials | q_volume_tea_SW | series | {9 series tags} |
| tutorials | q_volume_tea_W | series | {9 series tags} |
| tutorials | q_volume_wine_E | series | {9 series tags} |
| tutorials | q_volume_wine_N | series | {9 series tags} |
| tutorials | q_volume_wine_NE | series | {9 series tags} |
| tutorials | q_volume_wine_NW | series | {9 series tags} |
| tutorials | q_volume_wine_S | series | {9 series tags} |
| tutorials | q_volume_wine_SE | series | {9 series tags} |
| tutorials | q_volume_wine_SW | series | {9 series tags} |
| tutorials | q_volume_wine_W | series | {9 series tags} |
| tutorials | r_price_beer_E | series | {9 series tags} |
| tutorials | r_price_beer_N | series | {9 series tags} |
| tutorials | r_price_beer_NE | series | {9 series tags} |
| tutorials | r_price_beer_NW | series | {9 series tags} |
| tutorials | r_price_beer_S | series | {9 series tags} |
| tutorials | r_price_beer_SE | series | {9 series tags} |
| tutorials | r_price_beer_SW | series | {9 series tags} |
| tutorials | r_price_beer_W | series | {9 series tags} |
| tutorials | r_price_coffee_E | series | {9 series tags} |
| tutorials | r_price_coffee_N | series | {9 series tags} |
| tutorials | r_price_coffee_NE | series | {9 series tags} |
| tutorials | r_price_coffee_NW | series | {9 series tags} |
| tutorials | r_price_coffee_S | series | {9 series tags} |
| tutorials | r_price_coffee_SE | series | {9 series tags} |
| tutorials | r_price_coffee_SW | series | {9 series tags} |
| tutorials | r_price_coffee_W | series | {9 series tags} |
| tutorials | r_price_soft-drinks_E | series | {9 series tags} |
| tutorials | r_price_soft-drinks_N | series | {9 series tags} |
| tutorials | r_price_soft-drinks_NE | series | {9 series tags} |
| tutorials | r_price_soft-drinks_NW | series | {9 series tags} |
| tutorials | r_price_soft-drinks_S | series | {9 series tags} |
| tutorials | r_price_soft-drinks_SE | series | {9 series tags} |
| tutorials | r_price_soft-drinks_SW | series | {9 series tags} |
| tutorials | r_price_soft-drinks_W | series | {9 series tags} |
| tutorials | r_price_tea_E | series | {9 series tags} |
| tutorials | r_price_tea_N | series | {9 series tags} |
| tutorials | r_price_tea_NE | series | {9 series tags} |
| tutorials | r_price_tea_NW | series | {9 series tags} |
| tutorials | r_price_tea_S | series | {9 series tags} |
| tutorials | r_price_tea_SE | series | {9 series tags} |
| tutorials | r_price_tea_SW | series | {9 series tags} |
| tutorials | r_price_tea_W | series | {9 series tags} |
| tutorials | r_price_wine_E | series | {9 series tags} |
| tutorials | r_price_wine_N | series | {9 series tags} |
| tutorials | r_price_wine_NE | series | {9 series tags} |
| tutorials | r_price_wine_NW | series | {9 series tags} |
| tutorials | r_price_wine_S | series | {9 series tags} |
| tutorials | r_price_wine_SE | series | {9 series tags} |
| tutorials | r_price_wine_SW | series | {9 series tags} |
| tutorials | r_price_wine_W | series | {9 series tags} |
| tutorials | r_volume_beer_E | series | {9 series tags} |
| tutorials | r_volume_beer_N | series | {9 series tags} |
| tutorials | r_volume_beer_NE | series | {9 series tags} |
| tutorials | r_volume_beer_NW | series | {9 series tags} |
| tutorials | r_volume_beer_S | series | {9 series tags} |
| tutorials | r_volume_beer_SE | series | {9 series tags} |
| tutorials | r_volume_beer_SW | series | {9 series tags} |
| tutorials | r_volume_beer_W | series | {9 series tags} |
| tutorials | r_volume_coffee_E | series | {9 series tags} |
| tutorials | r_volume_coffee_N | series | {9 series tags} |
| tutorials | r_volume_coffee_NE | series | {9 series tags} |
| tutorials | r_volume_coffee_NW | series | {9 series tags} |
| tutorials | r_volume_coffee_S | series | {9 series tags} |
| tutorials | r_volume_coffee_SE | series | {9 series tags} |
| tutorials | r_volume_coffee_SW | series | {9 series tags} |
| tutorials | r_volume_coffee_W | series | {9 series tags} |
| tutorials | r_volume_soft-drinks_E | series | {9 series tags} |
| tutorials | r_volume_soft-drinks_N | series | {9 series tags} |
| tutorials | r_volume_soft-drinks_NE | series | {9 series tags} |
| tutorials | r_volume_soft-drinks_NW | series | {9 series tags} |
| tutorials | r_volume_soft-drinks_S | series | {9 series tags} |
| tutorials | r_volume_soft-drinks_SE | series | {9 series tags} |
| tutorials | r_volume_soft-drinks_SW | series | {9 series tags} |
| tutorials | r_volume_soft-drinks_W | series | {9 series tags} |
| tutorials | r_volume_tea_E | series | {9 series tags} |
| tutorials | r_volume_tea_N | series | {9 series tags} |
| tutorials | r_volume_tea_NE | series | {9 series tags} |
| tutorials | r_volume_tea_NW | series | {9 series tags} |
| tutorials | r_volume_tea_S | series | {9 series tags} |
| tutorials | r_volume_tea_SE | series | {9 series tags} |
| tutorials | r_volume_tea_SW | series | {9 series tags} |
| tutorials | r_volume_tea_W | series | {9 series tags} |
| tutorials | r_volume_wine_E | series | {9 series tags} |
| tutorials | r_volume_wine_N | series | {9 series tags} |
| tutorials | r_volume_wine_NE | series | {9 series tags} |
| tutorials | r_volume_wine_NW | series | {9 series tags} |
| tutorials | r_volume_wine_S | series | {9 series tags} |
| tutorials | r_volume_wine_SE | series | {9 series tags} |
| tutorials | r_volume_wine_SW | series | {9 series tags} |
| tutorials | r_volume_wine_W | series | {9 series tags} |
| tutorials | s_price_beer_E | series | {9 series tags} |
| tutorials | s_price_beer_N | series | {9 series tags} |
| tutorials | s_price_beer_NE | series | {9 series tags} |
| tutorials | s_price_beer_NW | series | {9 series tags} |
| tutorials | s_price_beer_S | series | {9 series tags} |
| tutorials | s_price_beer_SE | series | {9 series tags} |
| tutorials | s_price_beer_SW | series | {9 series tags} |
| tutorials | s_price_beer_W | series | {9 series tags} |
| tutorials | s_price_coffee_E | series | {9 series tags} |
| tutorials | s_price_coffee_N | series | {9 series tags} |
| tutorials | s_price_coffee_NE | series | {9 series tags} |
| tutorials | s_price_coffee_NW | series | {9 series tags} |
| tutorials | s_price_coffee_S | series | {9 series tags} |
| tutorials | s_price_coffee_SE | series | {9 series tags} |
| tutorials | s_price_coffee_SW | series | {9 series tags} |
| tutorials | s_price_coffee_W | series | {9 series tags} |
| tutorials | s_price_soft-drinks_E | series | {9 series tags} |
| tutorials | s_price_soft-drinks_N | series | {9 series tags} |
| tutorials | s_price_soft-drinks_NE | series | {9 series tags} |
| tutorials | s_price_soft-drinks_NW | series | {9 series tags} |
| tutorials | s_price_soft-drinks_S | series | {9 series tags} |
| tutorials | s_price_soft-drinks_SE | series | {9 series tags} |
| tutorials | s_price_soft-drinks_SW | series | {9 series tags} |
| tutorials | s_price_soft-drinks_W | series | {9 series tags} |
| tutorials | s_price_tea_E | series | {9 series tags} |
| tutorials | s_price_tea_N | series | {9 series tags} |
| tutorials | s_price_tea_NE | series | {9 series tags} |
| tutorials | s_price_tea_NW | series | {9 series tags} |
| tutorials | s_price_tea_S | series | {9 series tags} |
| tutorials | s_price_tea_SE | series | {9 series tags} |
| tutorials | s_price_tea_SW | series | {9 series tags} |
| tutorials | s_price_tea_W | series | {9 series tags} |
| tutorials | s_price_wine_E | series | {9 series tags} |
| tutorials | s_price_wine_N | series | {9 series tags} |
| tutorials | s_price_wine_NE | series | {9 series tags} |
| tutorials | s_price_wine_NW | series | {9 series tags} |
| tutorials | s_price_wine_S | series | {9 series tags} |
| tutorials | s_price_wine_SE | series | {9 series tags} |
| tutorials | s_price_wine_SW | series | {9 series tags} |
| tutorials | s_price_wine_W | series | {9 series tags} |
| tutorials | s_volume_beer_E | series | {9 series tags} |
| tutorials | s_volume_beer_N | series | {9 series tags} |
| tutorials | s_volume_beer_NE | series | {9 series tags} |
| tutorials | s_volume_beer_NW | series | {9 series tags} |
| tutorials | s_volume_beer_S | series | {9 series tags} |
| tutorials | s_volume_beer_SE | series | {9 series tags} |
| tutorials | s_volume_beer_SW | series | {9 series tags} |
| tutorials | s_volume_beer_W | series | {9 series tags} |
| tutorials | s_volume_coffee_E | series | {9 series tags} |
| tutorials | s_volume_coffee_N | series | {9 series tags} |
| tutorials | s_volume_coffee_NE | series | {9 series tags} |
| tutorials | s_volume_coffee_NW | series | {9 series tags} |
| tutorials | s_volume_coffee_S | series | {9 series tags} |
| tutorials | s_volume_coffee_SE | series | {9 series tags} |
| tutorials | s_volume_coffee_SW | series | {9 series tags} |
| tutorials | s_volume_coffee_W | series | {9 series tags} |
| tutorials | s_volume_soft-drinks_E | series | {9 series tags} |
| tutorials | s_volume_soft-drinks_N | series | {9 series tags} |
| tutorials | s_volume_soft-drinks_NE | series | {9 series tags} |
| tutorials | s_volume_soft-drinks_NW | series | {9 series tags} |
| tutorials | s_volume_soft-drinks_S | series | {9 series tags} |
| tutorials | s_volume_soft-drinks_SE | series | {9 series tags} |
| tutorials | s_volume_soft-drinks_SW | series | {9 series tags} |
| tutorials | s_volume_soft-drinks_W | series | {9 series tags} |
| tutorials | s_volume_tea_E | series | {9 series tags} |
| tutorials | s_volume_tea_N | series | {9 series tags} |
| tutorials | s_volume_tea_NE | series | {9 series tags} |
| tutorials | s_volume_tea_NW | series | {9 series tags} |
| tutorials | s_volume_tea_S | series | {9 series tags} |
| tutorials | s_volume_tea_SE | series | {9 series tags} |
| tutorials | s_volume_tea_SW | series | {9 series tags} |
| tutorials | s_volume_tea_W | series | {9 series tags} |
| tutorials | s_volume_wine_E | series | {9 series tags} |
| tutorials | s_volume_wine_N | series | {9 series tags} |
| tutorials | s_volume_wine_NE | series | {9 series tags} |
| tutorials | s_volume_wine_NW | series | {9 series tags} |
| tutorials | s_volume_wine_S | series | {9 series tags} |
| tutorials | s_volume_wine_SE | series | {9 series tags} |
| tutorials | s_volume_wine_SW | series | {9 series tags} |
| tutorials | s_volume_wine_W | series | {9 series tags} |
| tutorials | t_price_beer_E | series | {9 series tags} |
| tutorials | t_price_beer_N | series | {9 series tags} |
| tutorials | t_price_beer_NE | series | {9 series tags} |
| tutorials | t_price_beer_NW | series | {9 series tags} |
| tutorials | t_price_beer_S | series | {9 series tags} |
| tutorials | t_price_beer_SE | series | {9 series tags} |
| tutorials | t_price_beer_SW | series | {9 series tags} |
| tutorials | t_price_beer_W | series | {9 series tags} |
| tutorials | t_price_coffee_E | series | {9 series tags} |
| tutorials | t_price_coffee_N | series | {9 series tags} |
| tutorials | t_price_coffee_NE | series | {9 series tags} |
| tutorials | t_price_coffee_NW | series | {9 series tags} |
| tutorials | t_price_coffee_S | series | {9 series tags} |
| tutorials | t_price_coffee_SE | series | {9 series tags} |
| tutorials | t_price_coffee_SW | series | {9 series tags} |
| tutorials | t_price_coffee_W | series | {9 series tags} |
| tutorials | t_price_soft-drinks_E | series | {9 series tags} |
| tutorials | t_price_soft-drinks_N | series | {9 series tags} |
| tutorials | t_price_soft-drinks_NE | series | {9 series tags} |
| tutorials | t_price_soft-drinks_NW | series | {9 series tags} |
| tutorials | t_price_soft-drinks_S | series | {9 series tags} |
| tutorials | t_price_soft-drinks_SE | series | {9 series tags} |
| tutorials | t_price_soft-drinks_SW | series | {9 series tags} |
| tutorials | t_price_soft-drinks_W | series | {9 series tags} |
| tutorials | t_price_tea_E | series | {9 series tags} |
| tutorials | t_price_tea_N | series | {9 series tags} |
| tutorials | t_price_tea_NE | series | {9 series tags} |
| tutorials | t_price_tea_NW | series | {9 series tags} |
| tutorials | t_price_tea_S | series | {9 series tags} |
| tutorials | t_price_tea_SE | series | {9 series tags} |
| tutorials | t_price_tea_SW | series | {9 series tags} |
| tutorials | t_price_tea_W | series | {9 series tags} |
| tutorials | t_price_wine_E | series | {9 series tags} |
| tutorials | t_price_wine_N | series | {9 series tags} |
| tutorials | t_price_wine_NE | series | {9 series tags} |
| tutorials | t_price_wine_NW | series | {9 series tags} |
| tutorials | t_price_wine_S | series | {9 series tags} |
| tutorials | t_price_wine_SE | series | {9 series tags} |
| tutorials | t_price_wine_SW | series | {9 series tags} |
| tutorials | t_price_wine_W | series | {9 series tags} |
| tutorials | t_volume_beer_E | series | {9 series tags} |
| tutorials | t_volume_beer_N | series | {9 series tags} |
| tutorials | t_volume_beer_NE | series | {9 series tags} |
| tutorials | t_volume_beer_NW | series | {9 series tags} |
| tutorials | t_volume_beer_S | series | {9 series tags} |
| tutorials | t_volume_beer_SE | series | {9 series tags} |
| tutorials | t_volume_beer_SW | series | {9 series tags} |
| tutorials | t_volume_beer_W | series | {9 series tags} |
| tutorials | t_volume_coffee_E | series | {9 series tags} |
| tutorials | t_volume_coffee_N | series | {9 series tags} |
| tutorials | t_volume_coffee_NE | series | {9 series tags} |
| tutorials | t_volume_coffee_NW | series | {9 series tags} |
| tutorials | t_volume_coffee_S | series | {9 series tags} |
| tutorials | t_volume_coffee_SE | series | {9 series tags} |
| tutorials | t_volume_coffee_SW | series | {9 series tags} |
| tutorials | t_volume_coffee_W | series | {9 series tags} |
| tutorials | t_volume_soft-drinks_E | series | {9 series tags} |
| tutorials | t_volume_soft-drinks_N | series | {9 series tags} |
| tutorials | t_volume_soft-drinks_NE | series | {9 series tags} |
| tutorials | t_volume_soft-drinks_NW | series | {9 series tags} |
| tutorials | t_volume_soft-drinks_S | series | {9 series tags} |
| tutorials | t_volume_soft-drinks_SE | series | {9 series tags} |
| tutorials | t_volume_soft-drinks_SW | series | {9 series tags} |
| tutorials | t_volume_soft-drinks_W | series | {9 series tags} |
| tutorials | t_volume_tea_E | series | {9 series tags} |
| tutorials | t_volume_tea_N | series | {9 series tags} |
| tutorials | t_volume_tea_NE | series | {9 series tags} |
| tutorials | t_volume_tea_NW | series | {9 series tags} |
| tutorials | t_volume_tea_S | series | {9 series tags} |
| tutorials | t_volume_tea_SE | series | {9 series tags} |
| tutorials | t_volume_tea_SW | series | {9 series tags} |
| tutorials | t_volume_tea_W | series | {9 series tags} |
| tutorials | t_volume_wine_E | series | {9 series tags} |
| tutorials | t_volume_wine_N | series | {9 series tags} |
| tutorials | t_volume_wine_NE | series | {9 series tags} |
| tutorials | t_volume_wine_NW | series | {9 series tags} |
| tutorials | t_volume_wine_S | series | {9 series tags} |
| tutorials | t_volume_wine_SE | series | {9 series tags} |
| tutorials | t_volume_wine_SW | series | {9 series tags} |
| tutorials | t_volume_wine_W | series | {9 series tags} |
| tutorials | u_price_beer_E | series | {9 series tags} |
| tutorials | u_price_beer_N | series | {9 series tags} |
| tutorials | u_price_beer_NE | series | {9 series tags} |
| tutorials | u_price_beer_NW | series | {9 series tags} |
| tutorials | u_price_beer_S | series | {9 series tags} |
| tutorials | u_price_beer_SE | series | {9 series tags} |
| tutorials | u_price_beer_SW | series | {9 series tags} |
| tutorials | u_price_beer_W | series | {9 series tags} |
| tutorials | u_price_coffee_E | series | {9 series tags} |
| tutorials | u_price_coffee_N | series | {9 series tags} |
| tutorials | u_price_coffee_NE | series | {9 series tags} |
| tutorials | u_price_coffee_NW | series | {9 series tags} |
| tutorials | u_price_coffee_S | series | {9 series tags} |
| tutorials | u_price_coffee_SE | series | {9 series tags} |
| tutorials | u_price_coffee_SW | series | {9 series tags} |
| tutorials | u_price_coffee_W | series | {9 series tags} |
| tutorials | u_price_soft-drinks_E | series | {9 series tags} |
| tutorials | u_price_soft-drinks_N | series | {9 series tags} |
| tutorials | u_price_soft-drinks_NE | series | {9 series tags} |
| tutorials | u_price_soft-drinks_NW | series | {9 series tags} |
| tutorials | u_price_soft-drinks_S | series | {9 series tags} |
| tutorials | u_price_soft-drinks_SE | series | {9 series tags} |
| tutorials | u_price_soft-drinks_SW | series | {9 series tags} |
| tutorials | u_price_soft-drinks_W | series | {9 series tags} |
| tutorials | u_price_tea_E | series | {9 series tags} |
| tutorials | u_price_tea_N | series | {9 series tags} |
| tutorials | u_price_tea_NE | series | {9 series tags} |
| tutorials | u_price_tea_NW | series | {9 series tags} |
| tutorials | u_price_tea_S | series | {9 series tags} |
| tutorials | u_price_tea_SE | series | {9 series tags} |
| tutorials | u_price_tea_SW | series | {9 series tags} |
| tutorials | u_price_tea_W | series | {9 series tags} |
| tutorials | u_price_wine_E | series | {9 series tags} |
| tutorials | u_price_wine_N | series | {9 series tags} |
| tutorials | u_price_wine_NE | series | {9 series tags} |
| tutorials | u_price_wine_NW | series | {9 series tags} |
| tutorials | u_price_wine_S | series | {9 series tags} |
| tutorials | u_price_wine_SE | series | {9 series tags} |
| tutorials | u_price_wine_SW | series | {9 series tags} |
| tutorials | u_price_wine_W | series | {9 series tags} |
| tutorials | u_volume_beer_E | series | {9 series tags} |
| tutorials | u_volume_beer_N | series | {9 series tags} |
| tutorials | u_volume_beer_NE | series | {9 series tags} |
| tutorials | u_volume_beer_NW | series | {9 series tags} |
| tutorials | u_volume_beer_S | series | {9 series tags} |
| tutorials | u_volume_beer_SE | series | {9 series tags} |
| tutorials | u_volume_beer_SW | series | {9 series tags} |
| tutorials | u_volume_beer_W | series | {9 series tags} |
| tutorials | u_volume_coffee_E | series | {9 series tags} |
| tutorials | u_volume_coffee_N | series | {9 series tags} |
| tutorials | u_volume_coffee_NE | series | {9 series tags} |
| tutorials | u_volume_coffee_NW | series | {9 series tags} |
| tutorials | u_volume_coffee_S | series | {9 series tags} |
| tutorials | u_volume_coffee_SE | series | {9 series tags} |
| tutorials | u_volume_coffee_SW | series | {9 series tags} |
| tutorials | u_volume_coffee_W | series | {9 series tags} |
| tutorials | u_volume_soft-drinks_E | series | {9 series tags} |
| tutorials | u_volume_soft-drinks_N | series | {9 series tags} |
| tutorials | u_volume_soft-drinks_NE | series | {9 series tags} |
| tutorials | u_volume_soft-drinks_NW | series | {9 series tags} |
| tutorials | u_volume_soft-drinks_S | series | {9 series tags} |
| tutorials | u_volume_soft-drinks_SE | series | {9 series tags} |
| tutorials | u_volume_soft-drinks_SW | series | {9 series tags} |
| tutorials | u_volume_soft-drinks_W | series | {9 series tags} |
| tutorials | u_volume_tea_E | series | {9 series tags} |
| tutorials | u_volume_tea_N | series | {9 series tags} |
| tutorials | u_volume_tea_NE | series | {9 series tags} |
| tutorials | u_volume_tea_NW | series | {9 series tags} |
| tutorials | u_volume_tea_S | series | {9 series tags} |
| tutorials | u_volume_tea_SE | series | {9 series tags} |
| tutorials | u_volume_tea_SW | series | {9 series tags} |
| tutorials | u_volume_tea_W | series | {9 series tags} |
| tutorials | u_volume_wine_E | series | {9 series tags} |
| tutorials | u_volume_wine_N | series | {9 series tags} |
| tutorials | u_volume_wine_NE | series | {9 series tags} |
| tutorials | u_volume_wine_NW | series | {9 series tags} |
| tutorials | u_volume_wine_S | series | {9 series tags} |
| tutorials | u_volume_wine_SE | series | {9 series tags} |
| tutorials | u_volume_wine_SW | series | {9 series tags} |
| tutorials | u_volume_wine_W | series | {9 series tags} |
| tutorials | v_price_beer_E | series | {9 series tags} |
| tutorials | v_price_beer_N | series | {9 series tags} |
| tutorials | v_price_beer_NE | series | {9 series tags} |
| tutorials | v_price_beer_NW | series | {9 series tags} |
| tutorials | v_price_beer_S | series | {9 series tags} |
| tutorials | v_price_beer_SE | series | {9 series tags} |
| tutorials | v_price_beer_SW | series | {9 series tags} |
| tutorials | v_price_beer_W | series | {9 series tags} |
| tutorials | v_price_coffee_E | series | {9 series tags} |
| tutorials | v_price_coffee_N | series | {9 series tags} |
| tutorials | v_price_coffee_NE | series | {9 series tags} |
| tutorials | v_price_coffee_NW | series | {9 series tags} |
| tutorials | v_price_coffee_S | series | {9 series tags} |
| tutorials | v_price_coffee_SE | series | {9 series tags} |
| tutorials | v_price_coffee_SW | series | {9 series tags} |
| tutorials | v_price_coffee_W | series | {9 series tags} |
| tutorials | v_price_soft-drinks_E | series | {9 series tags} |
| tutorials | v_price_soft-drinks_N | series | {9 series tags} |
| tutorials | v_price_soft-drinks_NE | series | {9 series tags} |
| tutorials | v_price_soft-drinks_NW | series | {9 series tags} |
| tutorials | v_price_soft-drinks_S | series | {9 series tags} |
| tutorials | v_price_soft-drinks_SE | series | {9 series tags} |
| tutorials | v_price_soft-drinks_SW | series | {9 series tags} |
| tutorials | v_price_soft-drinks_W | series | {9 series tags} |
| tutorials | v_price_tea_E | series | {9 series tags} |
| tutorials | v_price_tea_N | series | {9 series tags} |
| tutorials | v_price_tea_NE | series | {9 series tags} |
| tutorials | v_price_tea_NW | series | {9 series tags} |
| tutorials | v_price_tea_S | series | {9 series tags} |
| tutorials | v_price_tea_SE | series | {9 series tags} |
| tutorials | v_price_tea_SW | series | {9 series tags} |
| tutorials | v_price_tea_W | series | {9 series tags} |
| tutorials | v_price_wine_E | series | {9 series tags} |
| tutorials | v_price_wine_N | series | {9 series tags} |
| tutorials | v_price_wine_NE | series | {9 series tags} |
| tutorials | v_price_wine_NW | series | {9 series tags} |
| tutorials | v_price_wine_S | series | {9 series tags} |
| tutorials | v_price_wine_SE | series | {9 series tags} |
| tutorials | v_price_wine_SW | series | {9 series tags} |
| tutorials | v_price_wine_W | series | {9 series tags} |
| tutorials | v_volume_beer_E | series | {9 series tags} |
| tutorials | v_volume_beer_N | series | {9 series tags} |
| tutorials | v_volume_beer_NE | series | {9 series tags} |
| tutorials | v_volume_beer_NW | series | {9 series tags} |
| tutorials | v_volume_beer_S | series | {9 series tags} |
| tutorials | v_volume_beer_SE | series | {9 series tags} |
| tutorials | v_volume_beer_SW | series | {9 series tags} |
| tutorials | v_volume_beer_W | series | {9 series tags} |
| tutorials | v_volume_coffee_E | series | {9 series tags} |
| tutorials | v_volume_coffee_N | series | {9 series tags} |
| tutorials | v_volume_coffee_NE | series | {9 series tags} |
| tutorials | v_volume_coffee_NW | series | {9 series tags} |
| tutorials | v_volume_coffee_S | series | {9 series tags} |
| tutorials | v_volume_coffee_SE | series | {9 series tags} |
| tutorials | v_volume_coffee_SW | series | {9 series tags} |
| tutorials | v_volume_coffee_W | series | {9 series tags} |
| tutorials | v_volume_soft-drinks_E | series | {9 series tags} |
| tutorials | v_volume_soft-drinks_N | series | {9 series tags} |
| tutorials | v_volume_soft-drinks_NE | series | {9 series tags} |
| tutorials | v_volume_soft-drinks_NW | series | {9 series tags} |
| tutorials | v_volume_soft-drinks_S | series | {9 series tags} |
| tutorials | v_volume_soft-drinks_SE | series | {9 series tags} |
| tutorials | v_volume_soft-drinks_SW | series | {9 series tags} |
| tutorials | v_volume_soft-drinks_W | series | {9 series tags} |
| tutorials | v_volume_tea_E | series | {9 series tags} |
| tutorials | v_volume_tea_N | series | {9 series tags} |
| tutorials | v_volume_tea_NE | series | {9 series tags} |
| tutorials | v_volume_tea_NW | series | {9 series tags} |
| tutorials | v_volume_tea_S | series | {9 series tags} |
| tutorials | v_volume_tea_SE | series | {9 series tags} |
| tutorials | v_volume_tea_SW | series | {9 series tags} |
| tutorials | v_volume_tea_W | series | {9 series tags} |
| tutorials | v_volume_wine_E | series | {9 series tags} |
| tutorials | v_volume_wine_N | series | {9 series tags} |
| tutorials | v_volume_wine_NE | series | {9 series tags} |
| tutorials | v_volume_wine_NW | series | {9 series tags} |
| tutorials | v_volume_wine_S | series | {9 series tags} |
| tutorials | v_volume_wine_SE | series | {9 series tags} |
| tutorials | v_volume_wine_SW | series | {9 series tags} |
| tutorials | v_volume_wine_W | series | {9 series tags} |
| tutorials | w_price_beer_E | series | {9 series tags} |
| tutorials | w_price_beer_N | series | {9 series tags} |
| tutorials | w_price_beer_NE | series | {9 series tags} |
| tutorials | w_price_beer_NW | series | {9 series tags} |
| tutorials | w_price_beer_S | series | {9 series tags} |
| tutorials | w_price_beer_SE | series | {9 series tags} |
| tutorials | w_price_beer_SW | series | {9 series tags} |
| tutorials | w_price_beer_W | series | {9 series tags} |
| tutorials | w_price_coffee_E | series | {9 series tags} |
| tutorials | w_price_coffee_N | series | {9 series tags} |
| tutorials | w_price_coffee_NE | series | {9 series tags} |
| tutorials | w_price_coffee_NW | series | {9 series tags} |
| tutorials | w_price_coffee_S | series | {9 series tags} |
| tutorials | w_price_coffee_SE | series | {9 series tags} |
| tutorials | w_price_coffee_SW | series | {9 series tags} |
| tutorials | w_price_coffee_W | series | {9 series tags} |
| tutorials | w_price_soft-drinks_E | series | {9 series tags} |
| tutorials | w_price_soft-drinks_N | series | {9 series tags} |
| tutorials | w_price_soft-drinks_NE | series | {9 series tags} |
| tutorials | w_price_soft-drinks_NW | series | {9 series tags} |
| tutorials | w_price_soft-drinks_S | series | {9 series tags} |
| tutorials | w_price_soft-drinks_SE | series | {9 series tags} |
| tutorials | w_price_soft-drinks_SW | series | {9 series tags} |
| tutorials | w_price_soft-drinks_W | series | {9 series tags} |
| tutorials | w_price_tea_E | series | {9 series tags} |
| tutorials | w_price_tea_N | series | {9 series tags} |
| tutorials | w_price_tea_NE | series | {9 series tags} |
| tutorials | w_price_tea_NW | series | {9 series tags} |
| tutorials | w_price_tea_S | series | {9 series tags} |
| tutorials | w_price_tea_SE | series | {9 series tags} |
| tutorials | w_price_tea_SW | series | {9 series tags} |
| tutorials | w_price_tea_W | series | {9 series tags} |
| tutorials | w_price_wine_E | series | {9 series tags} |
| tutorials | w_price_wine_N | series | {9 series tags} |
| tutorials | w_price_wine_NE | series | {9 series tags} |
| tutorials | w_price_wine_NW | series | {9 series tags} |
| tutorials | w_price_wine_S | series | {9 series tags} |
| tutorials | w_price_wine_SE | series | {9 series tags} |
| tutorials | w_price_wine_SW | series | {9 series tags} |
| tutorials | w_price_wine_W | series | {9 series tags} |
| tutorials | w_volume_beer_E | series | {9 series tags} |
| tutorials | w_volume_beer_N | series | {9 series tags} |
| tutorials | w_volume_beer_NE | series | {9 series tags} |
| tutorials | w_volume_beer_NW | series | {9 series tags} |
| tutorials | w_volume_beer_S | series | {9 series tags} |
| tutorials | w_volume_beer_SE | series | {9 series tags} |
| tutorials | w_volume_beer_SW | series | {9 series tags} |
| tutorials | w_volume_beer_W | series | {9 series tags} |
| tutorials | w_volume_coffee_E | series | {9 series tags} |
| tutorials | w_volume_coffee_N | series | {9 series tags} |
| tutorials | w_volume_coffee_NE | series | {9 series tags} |
| tutorials | w_volume_coffee_NW | series | {9 series tags} |
| tutorials | w_volume_coffee_S | series | {9 series tags} |
| tutorials | w_volume_coffee_SE | series | {9 series tags} |
| tutorials | w_volume_coffee_SW | series | {9 series tags} |
| tutorials | w_volume_coffee_W | series | {9 series tags} |
| tutorials | w_volume_soft-drinks_E | series | {9 series tags} |
| tutorials | w_volume_soft-drinks_N | series | {9 series tags} |
| tutorials | w_volume_soft-drinks_NE | series | {9 series tags} |
| tutorials | w_volume_soft-drinks_NW | series | {9 series tags} |
| tutorials | w_volume_soft-drinks_S | series | {9 series tags} |
| tutorials | w_volume_soft-drinks_SE | series | {9 series tags} |
| tutorials | w_volume_soft-drinks_SW | series | {9 series tags} |
| tutorials | w_volume_soft-drinks_W | series | {9 series tags} |
| tutorials | w_volume_tea_E | series | {9 series tags} |
| tutorials | w_volume_tea_N | series | {9 series tags} |
| tutorials | w_volume_tea_NE | series | {9 series tags} |
| tutorials | w_volume_tea_NW | series | {9 series tags} |
| tutorials | w_volume_tea_S | series | {9 series tags} |
| tutorials | w_volume_tea_SE | series | {9 series tags} |
| tutorials | w_volume_tea_SW | series | {9 series tags} |
| tutorials | w_volume_tea_W | series | {9 series tags} |
| tutorials | w_volume_wine_E | series | {9 series tags} |
| tutorials | w_volume_wine_N | series | {9 series tags} |
| tutorials | w_volume_wine_NE | series | {9 series tags} |
| tutorials | w_volume_wine_NW | series | {9 series tags} |
| tutorials | w_volume_wine_S | series | {9 series tags} |
| tutorials | w_volume_wine_SE | series | {9 series tags} |
| tutorials | w_volume_wine_SW | series | {9 series tags} |
| tutorials | w_volume_wine_W | series | {9 series tags} |
| tutorials | x_price_beer_E | series | {9 series tags} |
| tutorials | x_price_beer_N | series | {9 series tags} |
| tutorials | x_price_beer_NE | series | {9 series tags} |
| tutorials | x_price_beer_NW | series | {9 series tags} |
| tutorials | x_price_beer_S | series | {9 series tags} |
| tutorials | x_price_beer_SE | series | {9 series tags} |
| tutorials | x_price_beer_SW | series | {9 series tags} |
| tutorials | x_price_beer_W | series | {9 series tags} |
| tutorials | x_price_coffee_E | series | {9 series tags} |
| tutorials | x_price_coffee_N | series | {9 series tags} |
| tutorials | x_price_coffee_NE | series | {9 series tags} |
| tutorials | x_price_coffee_NW | series | {9 series tags} |
| tutorials | x_price_coffee_S | series | {9 series tags} |
| tutorials | x_price_coffee_SE | series | {9 series tags} |
| tutorials | x_price_coffee_SW | series | {9 series tags} |
| tutorials | x_price_coffee_W | series | {9 series tags} |
| tutorials | x_price_soft-drinks_E | series | {9 series tags} |
| tutorials | x_price_soft-drinks_N | series | {9 series tags} |
| tutorials | x_price_soft-drinks_NE | series | {9 series tags} |
| tutorials | x_price_soft-drinks_NW | series | {9 series tags} |
| tutorials | x_price_soft-drinks_S | series | {9 series tags} |
| tutorials | x_price_soft-drinks_SE | series | {9 series tags} |
| tutorials | x_price_soft-drinks_SW | series | {9 series tags} |
| tutorials | x_price_soft-drinks_W | series | {9 series tags} |
| tutorials | x_price_tea_E | series | {9 series tags} |
| tutorials | x_price_tea_N | series | {9 series tags} |
| tutorials | x_price_tea_NE | series | {9 series tags} |
| tutorials | x_price_tea_NW | series | {9 series tags} |
| tutorials | x_price_tea_S | series | {9 series tags} |
| tutorials | x_price_tea_SE | series | {9 series tags} |
| tutorials | x_price_tea_SW | series | {9 series tags} |
| tutorials | x_price_tea_W | series | {9 series tags} |
| tutorials | x_price_wine_E | series | {9 series tags} |
| tutorials | x_price_wine_N | series | {9 series tags} |
| tutorials | x_price_wine_NE | series | {9 series tags} |
| tutorials | x_price_wine_NW | series | {9 series tags} |
| tutorials | x_price_wine_S | series | {9 series tags} |
| tutorials | x_price_wine_SE | series | {9 series tags} |
| tutorials | x_price_wine_SW | series | {9 series tags} |
| tutorials | x_price_wine_W | series | {9 series tags} |
| tutorials | x_volume_beer_E | series | {9 series tags} |
| tutorials | x_volume_beer_N | series | {9 series tags} |
| tutorials | x_volume_beer_NE | series | {9 series tags} |
| tutorials | x_volume_beer_NW | series | {9 series tags} |
| tutorials | x_volume_beer_S | series | {9 series tags} |
| tutorials | x_volume_beer_SE | series | {9 series tags} |
| tutorials | x_volume_beer_SW | series | {9 series tags} |
| tutorials | x_volume_beer_W | series | {9 series tags} |
| tutorials | x_volume_coffee_E | series | {9 series tags} |
| tutorials | x_volume_coffee_N | series | {9 series tags} |
| tutorials | x_volume_coffee_NE | series | {9 series tags} |
| tutorials | x_volume_coffee_NW | series | {9 series tags} |
| tutorials | x_volume_coffee_S | series | {9 series tags} |
| tutorials | x_volume_coffee_SE | series | {9 series tags} |
| tutorials | x_volume_coffee_SW | series | {9 series tags} |
| tutorials | x_volume_coffee_W | series | {9 series tags} |
| tutorials | x_volume_soft-drinks_E | series | {9 series tags} |
| tutorials | x_volume_soft-drinks_N | series | {9 series tags} |
| tutorials | x_volume_soft-drinks_NE | series | {9 series tags} |
| tutorials | x_volume_soft-drinks_NW | series | {9 series tags} |
| tutorials | x_volume_soft-drinks_S | series | {9 series tags} |
| tutorials | x_volume_soft-drinks_SE | series | {9 series tags} |
| tutorials | x_volume_soft-drinks_SW | series | {9 series tags} |
| tutorials | x_volume_soft-drinks_W | series | {9 series tags} |
| tutorials | x_volume_tea_E | series | {9 series tags} |
| tutorials | x_volume_tea_N | series | {9 series tags} |
| tutorials | x_volume_tea_NE | series | {9 series tags} |
| tutorials | x_volume_tea_NW | series | {9 series tags} |
| tutorials | x_volume_tea_S | series | {9 series tags} |
| tutorials | x_volume_tea_SE | series | {9 series tags} |
| tutorials | x_volume_tea_SW | series | {9 series tags} |
| tutorials | x_volume_tea_W | series | {9 series tags} |
| tutorials | x_volume_wine_E | series | {9 series tags} |
| tutorials | x_volume_wine_N | series | {9 series tags} |
| tutorials | x_volume_wine_NE | series | {9 series tags} |
| tutorials | x_volume_wine_NW | series | {9 series tags} |
| tutorials | x_volume_wine_S | series | {9 series tags} |
| tutorials | x_volume_wine_SE | series | {9 series tags} |
| tutorials | x_volume_wine_SW | series | {9 series tags} |
| tutorials | x_volume_wine_W | series | {9 series tags} |
| tutorials | y_price_beer_E | series | {9 series tags} |
| tutorials | y_price_beer_N | series | {9 series tags} |
| tutorials | y_price_beer_NE | series | {9 series tags} |
| tutorials | y_price_beer_NW | series | {9 series tags} |
| tutorials | y_price_beer_S | series | {9 series tags} |
| tutorials | y_price_beer_SE | series | {9 series tags} |
| tutorials | y_price_beer_SW | series | {9 series tags} |
| tutorials | y_price_beer_W | series | {9 series tags} |
| tutorials | y_price_coffee_E | series | {9 series tags} |
| tutorials | y_price_coffee_N | series | {9 series tags} |
| tutorials | y_price_coffee_NE | series | {9 series tags} |
| tutorials | y_price_coffee_NW | series | {9 series tags} |
| tutorials | y_price_coffee_S | series | {9 series tags} |
| tutorials | y_price_coffee_SE | series | {9 series tags} |
| tutorials | y_price_coffee_SW | series | {9 series tags} |
| tutorials | y_price_coffee_W | series | {9 series tags} |
| tutorials | y_price_soft-drinks_E | series | {9 series tags} |
| tutorials | y_price_soft-drinks_N | series | {9 series tags} |
| tutorials | y_price_soft-drinks_NE | series | {9 series tags} |
| tutorials | y_price_soft-drinks_NW | series | {9 series tags} |
| tutorials | y_price_soft-drinks_S | series | {9 series tags} |
| tutorials | y_price_soft-drinks_SE | series | {9 series tags} |
| tutorials | y_price_soft-drinks_SW | series | {9 series tags} |
| tutorials | y_price_soft-drinks_W | series | {9 series tags} |
| tutorials | y_price_tea_E | series | {9 series tags} |
| tutorials | y_price_tea_N | series | {9 series tags} |
| tutorials | y_price_tea_NE | series | {9 series tags} |
| tutorials | y_price_tea_NW | series | {9 series tags} |
| tutorials | y_price_tea_S | series | {9 series tags} |
| tutorials | y_price_tea_SE | series | {9 series tags} |
| tutorials | y_price_tea_SW | series | {9 series tags} |
| tutorials | y_price_tea_W | series | {9 series tags} |
| tutorials | y_price_wine_E | series | {9 series tags} |
| tutorials | y_price_wine_N | series | {9 series tags} |
| tutorials | y_price_wine_NE | series | {9 series tags} |
| tutorials | y_price_wine_NW | series | {9 series tags} |
| tutorials | y_price_wine_S | series | {9 series tags} |
| tutorials | y_price_wine_SE | series | {9 series tags} |
| tutorials | y_price_wine_SW | series | {9 series tags} |
| tutorials | y_price_wine_W | series | {9 series tags} |
| tutorials | y_volume_beer_E | series | {9 series tags} |
| tutorials | y_volume_beer_N | series | {9 series tags} |
| tutorials | y_volume_beer_NE | series | {9 series tags} |
| tutorials | y_volume_beer_NW | series | {9 series tags} |
| tutorials | y_volume_beer_S | series | {9 series tags} |
| tutorials | y_volume_beer_SE | series | {9 series tags} |
| tutorials | y_volume_beer_SW | series | {9 series tags} |
| tutorials | y_volume_beer_W | series | {9 series tags} |
| tutorials | y_volume_coffee_E | series | {9 series tags} |
| tutorials | y_volume_coffee_N | series | {9 series tags} |
| tutorials | y_volume_coffee_NE | series | {9 series tags} |
| tutorials | y_volume_coffee_NW | series | {9 series tags} |
| tutorials | y_volume_coffee_S | series | {9 series tags} |
| tutorials | y_volume_coffee_SE | series | {9 series tags} |
| tutorials | y_volume_coffee_SW | series | {9 series tags} |
| tutorials | y_volume_coffee_W | series | {9 series tags} |
| tutorials | y_volume_soft-drinks_E | series | {9 series tags} |
| tutorials | y_volume_soft-drinks_N | series | {9 series tags} |
| tutorials | y_volume_soft-drinks_NE | series | {9 series tags} |
| tutorials | y_volume_soft-drinks_NW | series | {9 series tags} |
| tutorials | y_volume_soft-drinks_S | series | {9 series tags} |
| tutorials | y_volume_soft-drinks_SE | series | {9 series tags} |
| tutorials | y_volume_soft-drinks_SW | series | {9 series tags} |
| tutorials | y_volume_soft-drinks_W | series | {9 series tags} |
| tutorials | y_volume_tea_E | series | {9 series tags} |
| tutorials | y_volume_tea_N | series | {9 series tags} |
| tutorials | y_volume_tea_NE | series | {9 series tags} |
| tutorials | y_volume_tea_NW | series | {9 series tags} |
| tutorials | y_volume_tea_S | series | {9 series tags} |
| tutorials | y_volume_tea_SE | series | {9 series tags} |
| tutorials | y_volume_tea_SW | series | {9 series tags} |
| tutorials | y_volume_tea_W | series | {9 series tags} |
| tutorials | y_volume_wine_E | series | {9 series tags} |
| tutorials | y_volume_wine_N | series | {9 series tags} |
| tutorials | y_volume_wine_NE | series | {9 series tags} |
| tutorials | y_volume_wine_NW | series | {9 series tags} |
| tutorials | y_volume_wine_S | series | {9 series tags} |
| tutorials | y_volume_wine_SE | series | {9 series tags} |
| tutorials | y_volume_wine_SW | series | {9 series tags} |
| tutorials | y_volume_wine_W | series | {9 series tags} |
| tutorials | z_price_beer_E | series | {9 series tags} |
| tutorials | z_price_beer_N | series | {9 series tags} |
| tutorials | z_price_beer_NE | series | {9 series tags} |
| tutorials | z_price_beer_NW | series | {9 series tags} |
| tutorials | z_price_beer_S | series | {9 series tags} |
| tutorials | z_price_beer_SE | series | {9 series tags} |
| tutorials | z_price_beer_SW | series | {9 series tags} |
| tutorials | z_price_beer_W | series | {9 series tags} |
| tutorials | z_price_coffee_E | series | {9 series tags} |
| tutorials | z_price_coffee_N | series | {9 series tags} |
| tutorials | z_price_coffee_NE | series | {9 series tags} |
| tutorials | z_price_coffee_NW | series | {9 series tags} |
| tutorials | z_price_coffee_S | series | {9 series tags} |
| tutorials | z_price_coffee_SE | series | {9 series tags} |
| tutorials | z_price_coffee_SW | series | {9 series tags} |
| tutorials | z_price_coffee_W | series | {9 series tags} |
| tutorials | z_price_soft-drinks_E | series | {9 series tags} |
| tutorials | z_price_soft-drinks_N | series | {9 series tags} |
| tutorials | z_price_soft-drinks_NE | series | {9 series tags} |
| tutorials | z_price_soft-drinks_NW | series | {9 series tags} |
| tutorials | z_price_soft-drinks_S | series | {9 series tags} |
| tutorials | z_price_soft-drinks_SE | series | {9 series tags} |
| tutorials | z_price_soft-drinks_SW | series | {9 series tags} |
| tutorials | z_price_soft-drinks_W | series | {9 series tags} |
| tutorials | z_price_tea_E | series | {9 series tags} |
| tutorials | z_price_tea_N | series | {9 series tags} |
| tutorials | z_price_tea_NE | series | {9 series tags} |
| tutorials | z_price_tea_NW | series | {9 series tags} |
| tutorials | z_price_tea_S | series | {9 series tags} |
| tutorials | z_price_tea_SE | series | {9 series tags} |
| tutorials | z_price_tea_SW | series | {9 series tags} |
| tutorials | z_price_tea_W | series | {9 series tags} |
| tutorials | z_price_wine_E | series | {9 series tags} |
| tutorials | z_price_wine_N | series | {9 series tags} |
| tutorials | z_price_wine_NE | series | {9 series tags} |
| tutorials | z_price_wine_NW | series | {9 series tags} |
| tutorials | z_price_wine_S | series | {9 series tags} |
| tutorials | z_price_wine_SE | series | {9 series tags} |
| tutorials | z_price_wine_SW | series | {9 series tags} |
| tutorials | z_price_wine_W | series | {9 series tags} |
| tutorials | z_volume_beer_E | series | {9 series tags} |
| tutorials | z_volume_beer_N | series | {9 series tags} |
| tutorials | z_volume_beer_NE | series | {9 series tags} |
| tutorials | z_volume_beer_NW | series | {9 series tags} |
| tutorials | z_volume_beer_S | series | {9 series tags} |
| tutorials | z_volume_beer_SE | series | {9 series tags} |
| tutorials | z_volume_beer_SW | series | {9 series tags} |
| tutorials | z_volume_beer_W | series | {9 series tags} |
| tutorials | z_volume_coffee_E | series | {9 series tags} |
| tutorials | z_volume_coffee_N | series | {9 series tags} |
| tutorials | z_volume_coffee_NE | series | {9 series tags} |
| tutorials | z_volume_coffee_NW | series | {9 series tags} |
| tutorials | z_volume_coffee_S | series | {9 series tags} |
| tutorials | z_volume_coffee_SE | series | {9 series tags} |
| tutorials | z_volume_coffee_SW | series | {9 series tags} |
| tutorials | z_volume_coffee_W | series | {9 series tags} |
| tutorials | z_volume_soft-drinks_E | series | {9 series tags} |
| tutorials | z_volume_soft-drinks_N | series | {9 series tags} |
| tutorials | z_volume_soft-drinks_NE | series | {9 series tags} |
| tutorials | z_volume_soft-drinks_NW | series | {9 series tags} |
| tutorials | z_volume_soft-drinks_S | series | {9 series tags} |
| tutorials | z_volume_soft-drinks_SE | series | {9 series tags} |
| tutorials | z_volume_soft-drinks_SW | series | {9 series tags} |
| tutorials | z_volume_soft-drinks_W | series | {9 series tags} |
| tutorials | z_volume_tea_E | series | {9 series tags} |
| tutorials | z_volume_tea_N | series | {9 series tags} |
| tutorials | z_volume_tea_NE | series | {9 series tags} |
| tutorials | z_volume_tea_NW | series | {9 series tags} |
| tutorials | z_volume_tea_S | series | {9 series tags} |
| tutorials | z_volume_tea_SE | series | {9 series tags} |
| tutorials | z_volume_tea_SW | series | {9 series tags} |
| tutorials | z_volume_tea_W | series | {9 series tags} |
| tutorials | z_volume_wine_E | series | {9 series tags} |
| tutorials | z_volume_wine_N | series | {9 series tags} |
| tutorials | z_volume_wine_NE | series | {9 series tags} |
| tutorials | z_volume_wine_NW | series | {9 series tags} |
| tutorials | z_volume_wine_S | series | {9 series tags} |
| tutorials | z_volume_wine_SE | series | {9 series tags} |
| tutorials | z_volume_wine_SW | series | {9 series tags} |
| tutorials | z_volume_wine_W | series | {9 series tags} |
| tutorials | AZ_omsetning | dataset | {5 set tags + 130 series} |
| tutorials | a_omsetning_brus | series | {8 series tags} |
| tutorials | a_omsetning_kaffe | series | {8 series tags} |
| tutorials | a_omsetning_te | series | {8 series tags} |
| tutorials | a_omsetning_vin | series | {8 series tags} |
| tutorials | a_omsetning_øl | series | {8 series tags} |
| tutorials | b_omsetning_brus | series | {8 series tags} |
| tutorials | b_omsetning_kaffe | series | {8 series tags} |
| tutorials | b_omsetning_te | series | {8 series tags} |
| tutorials | b_omsetning_vin | series | {8 series tags} |
| tutorials | b_omsetning_øl | series | {8 series tags} |
| tutorials | c_omsetning_brus | series | {8 series tags} |
| tutorials | c_omsetning_kaffe | series | {8 series tags} |
| tutorials | c_omsetning_te | series | {8 series tags} |
| tutorials | c_omsetning_vin | series | {8 series tags} |
| tutorials | c_omsetning_øl | series | {8 series tags} |
| tutorials | d_omsetning_brus | series | {8 series tags} |
| tutorials | d_omsetning_kaffe | series | {8 series tags} |
| tutorials | d_omsetning_te | series | {8 series tags} |
| tutorials | d_omsetning_vin | series | {8 series tags} |
| tutorials | d_omsetning_øl | series | {8 series tags} |
| tutorials | e_omsetning_brus | series | {8 series tags} |
| tutorials | e_omsetning_kaffe | series | {8 series tags} |
| tutorials | e_omsetning_te | series | {8 series tags} |
| tutorials | e_omsetning_vin | series | {8 series tags} |
| tutorials | e_omsetning_øl | series | {8 series tags} |
| tutorials | f_omsetning_brus | series | {8 series tags} |
| tutorials | f_omsetning_kaffe | series | {8 series tags} |
| tutorials | f_omsetning_te | series | {8 series tags} |
| tutorials | f_omsetning_vin | series | {8 series tags} |
| tutorials | f_omsetning_øl | series | {8 series tags} |
| tutorials | g_omsetning_brus | series | {8 series tags} |
| tutorials | g_omsetning_kaffe | series | {8 series tags} |
| tutorials | g_omsetning_te | series | {8 series tags} |
| tutorials | g_omsetning_vin | series | {8 series tags} |
| tutorials | g_omsetning_øl | series | {8 series tags} |
| tutorials | h_omsetning_brus | series | {8 series tags} |
| tutorials | h_omsetning_kaffe | series | {8 series tags} |
| tutorials | h_omsetning_te | series | {8 series tags} |
| tutorials | h_omsetning_vin | series | {8 series tags} |
| tutorials | h_omsetning_øl | series | {8 series tags} |
| tutorials | i_omsetning_brus | series | {8 series tags} |
| tutorials | i_omsetning_kaffe | series | {8 series tags} |
| tutorials | i_omsetning_te | series | {8 series tags} |
| tutorials | i_omsetning_vin | series | {8 series tags} |
| tutorials | i_omsetning_øl | series | {8 series tags} |
| tutorials | j_omsetning_brus | series | {8 series tags} |
| tutorials | j_omsetning_kaffe | series | {8 series tags} |
| tutorials | j_omsetning_te | series | {8 series tags} |
| tutorials | j_omsetning_vin | series | {8 series tags} |
| tutorials | j_omsetning_øl | series | {8 series tags} |
| tutorials | k_omsetning_brus | series | {8 series tags} |
| tutorials | k_omsetning_kaffe | series | {8 series tags} |
| tutorials | k_omsetning_te | series | {8 series tags} |
| tutorials | k_omsetning_vin | series | {8 series tags} |
| tutorials | k_omsetning_øl | series | {8 series tags} |
| tutorials | l_omsetning_brus | series | {8 series tags} |
| tutorials | l_omsetning_kaffe | series | {8 series tags} |
| tutorials | l_omsetning_te | series | {8 series tags} |
| tutorials | l_omsetning_vin | series | {8 series tags} |
| tutorials | l_omsetning_øl | series | {8 series tags} |
| tutorials | m_omsetning_brus | series | {8 series tags} |
| tutorials | m_omsetning_kaffe | series | {8 series tags} |
| tutorials | m_omsetning_te | series | {8 series tags} |
| tutorials | m_omsetning_vin | series | {8 series tags} |
| tutorials | m_omsetning_øl | series | {8 series tags} |
| tutorials | n_omsetning_brus | series | {8 series tags} |
| tutorials | n_omsetning_kaffe | series | {8 series tags} |
| tutorials | n_omsetning_te | series | {8 series tags} |
| tutorials | n_omsetning_vin | series | {8 series tags} |
| tutorials | n_omsetning_øl | series | {8 series tags} |
| tutorials | o_omsetning_brus | series | {8 series tags} |
| tutorials | o_omsetning_kaffe | series | {8 series tags} |
| tutorials | o_omsetning_te | series | {8 series tags} |
| tutorials | o_omsetning_vin | series | {8 series tags} |
| tutorials | o_omsetning_øl | series | {8 series tags} |
| tutorials | p_omsetning_brus | series | {8 series tags} |
| tutorials | p_omsetning_kaffe | series | {8 series tags} |
| tutorials | p_omsetning_te | series | {8 series tags} |
| tutorials | p_omsetning_vin | series | {8 series tags} |
| tutorials | p_omsetning_øl | series | {8 series tags} |
| tutorials | q_omsetning_brus | series | {8 series tags} |
| tutorials | q_omsetning_kaffe | series | {8 series tags} |
| tutorials | q_omsetning_te | series | {8 series tags} |
| tutorials | q_omsetning_vin | series | {8 series tags} |
| tutorials | q_omsetning_øl | series | {8 series tags} |
| tutorials | r_omsetning_brus | series | {8 series tags} |
| tutorials | r_omsetning_kaffe | series | {8 series tags} |
| tutorials | r_omsetning_te | series | {8 series tags} |
| tutorials | r_omsetning_vin | series | {8 series tags} |
| tutorials | r_omsetning_øl | series | {8 series tags} |
| tutorials | s_omsetning_brus | series | {8 series tags} |
| tutorials | s_omsetning_kaffe | series | {8 series tags} |
| tutorials | s_omsetning_te | series | {8 series tags} |
| tutorials | s_omsetning_vin | series | {8 series tags} |
| tutorials | s_omsetning_øl | series | {8 series tags} |
| tutorials | t_omsetning_brus | series | {8 series tags} |
| tutorials | t_omsetning_kaffe | series | {8 series tags} |
| tutorials | t_omsetning_te | series | {8 series tags} |
| tutorials | t_omsetning_vin | series | {8 series tags} |
| tutorials | t_omsetning_øl | series | {8 series tags} |
| tutorials | u_omsetning_brus | series | {8 series tags} |
| tutorials | u_omsetning_kaffe | series | {8 series tags} |
| tutorials | u_omsetning_te | series | {8 series tags} |
| tutorials | u_omsetning_vin | series | {8 series tags} |
| tutorials | u_omsetning_øl | series | {8 series tags} |
| tutorials | v_omsetning_brus | series | {8 series tags} |
| tutorials | v_omsetning_kaffe | series | {8 series tags} |
| tutorials | v_omsetning_te | series | {8 series tags} |
| tutorials | v_omsetning_vin | series | {8 series tags} |
| tutorials | v_omsetning_øl | series | {8 series tags} |
| tutorials | w_omsetning_brus | series | {8 series tags} |
| tutorials | w_omsetning_kaffe | series | {8 series tags} |
| tutorials | w_omsetning_te | series | {8 series tags} |
| tutorials | w_omsetning_vin | series | {8 series tags} |
| tutorials | w_omsetning_øl | series | {8 series tags} |
| tutorials | x_omsetning_brus | series | {8 series tags} |
| tutorials | x_omsetning_kaffe | series | {8 series tags} |
| tutorials | x_omsetning_te | series | {8 series tags} |
| tutorials | x_omsetning_vin | series | {8 series tags} |
| tutorials | x_omsetning_øl | series | {8 series tags} |
| tutorials | y_omsetning_brus | series | {8 series tags} |
| tutorials | y_omsetning_kaffe | series | {8 series tags} |
| tutorials | y_omsetning_te | series | {8 series tags} |
| tutorials | y_omsetning_vin | series | {8 series tags} |
| tutorials | y_omsetning_øl | series | {8 series tags} |
| tutorials | z_omsetning_brus | series | {8 series tags} |
| tutorials | z_omsetning_kaffe | series | {8 series tags} |
| tutorials | z_omsetning_te | series | {8 series tags} |
| tutorials | z_omsetning_vin | series | {8 series tags} |
| tutorials | z_omsetning_øl | series | {8 series tags} |
| tutorials | More Prices and Volumes | dataset | {4 set tags + 636 series} |
| tutorials | price_bread_1.1.1 | series | {8 series tags} |
| tutorials | price_bread_1.1.2 | series | {8 series tags} |
| tutorials | price_bread_1.1.3 | series | {8 series tags} |
| tutorials | price_bread_1.2 | series | {8 series tags} |
| tutorials | price_bread_11.1 | series | {8 series tags} |
| tutorials | price_bread_11.2 | series | {8 series tags} |
| tutorials | price_bread_12.1.1 | series | {8 series tags} |
| tutorials | price_bread_12.1.10 | series | {8 series tags} |
| tutorials | price_bread_12.1.11 | series | {8 series tags} |
| tutorials | price_bread_12.1.12 | series | {8 series tags} |
| tutorials | price_bread_12.1.13 | series | {8 series tags} |
| tutorials | price_bread_12.1.2 | series | {8 series tags} |
| tutorials | price_bread_12.1.3 | series | {8 series tags} |
| tutorials | price_bread_12.1.4 | series | {8 series tags} |
| tutorials | price_bread_12.1.5 | series | {8 series tags} |
| tutorials | price_bread_12.1.6 | series | {8 series tags} |
| tutorials | price_bread_12.1.7 | series | {8 series tags} |
| tutorials | price_bread_12.1.8 | series | {8 series tags} |
| tutorials | price_bread_12.1.9 | series | {8 series tags} |
| tutorials | price_bread_12.2.1 | series | {8 series tags} |
| tutorials | price_bread_12.2.2 | series | {8 series tags} |
| tutorials | price_bread_12.2.3 | series | {8 series tags} |
| tutorials | price_bread_12.2.4 | series | {8 series tags} |
| tutorials | price_bread_12.2.5 | series | {8 series tags} |
| tutorials | price_bread_12.3.1 | series | {8 series tags} |
| tutorials | price_bread_12.3.2 | series | {8 series tags} |
| tutorials | price_bread_12.3.3 | series | {8 series tags} |
| tutorials | price_bread_12.3.4 | series | {8 series tags} |
| tutorials | price_bread_13 | series | {8 series tags} |
| tutorials | price_bread_14 | series | {8 series tags} |
| tutorials | price_bread_15 | series | {8 series tags} |
| tutorials | price_bread_2 | series | {8 series tags} |
| tutorials | price_bread_3 | series | {8 series tags} |
| tutorials | price_bread_4.1 | series | {8 series tags} |
| tutorials | price_bread_4.2 | series | {8 series tags} |
| tutorials | price_bread_5 | series | {8 series tags} |
| tutorials | price_bread_6 | series | {8 series tags} |
| tutorials | price_bread_7.1 | series | {8 series tags} |
| tutorials | price_bread_7.2 | series | {8 series tags} |
| tutorials | price_bread_7.3 | series | {8 series tags} |
| tutorials | price_bread_7.4 | series | {8 series tags} |
| tutorials | price_bread_7.5 | series | {8 series tags} |
| tutorials | price_bread_7.6 | series | {8 series tags} |
| tutorials | price_bread_8.1 | series | {8 series tags} |
| tutorials | price_bread_8.2 | series | {8 series tags} |
| tutorials | price_bread_8.3 | series | {8 series tags} |
| tutorials | price_bread_8.4 | series | {8 series tags} |
| tutorials | price_bread_8.5 | series | {8 series tags} |
| tutorials | price_bread_8.6 | series | {8 series tags} |
| tutorials | price_bread_8.7 | series | {8 series tags} |
| tutorials | price_bread_8.8 | series | {8 series tags} |
| tutorials | price_bread_8.9 | series | {8 series tags} |
| tutorials | price_bread_9 | series | {8 series tags} |
| tutorials | price_cheese_1.1.1 | series | {8 series tags} |
| tutorials | price_cheese_1.1.2 | series | {8 series tags} |
| tutorials | price_cheese_1.1.3 | series | {8 series tags} |
| tutorials | price_cheese_1.2 | series | {8 series tags} |
| tutorials | price_cheese_11.1 | series | {8 series tags} |
| tutorials | price_cheese_11.2 | series | {8 series tags} |
| tutorials | price_cheese_12.1.1 | series | {8 series tags} |
| tutorials | price_cheese_12.1.10 | series | {8 series tags} |
| tutorials | price_cheese_12.1.11 | series | {8 series tags} |
| tutorials | price_cheese_12.1.12 | series | {8 series tags} |
| tutorials | price_cheese_12.1.13 | series | {8 series tags} |
| tutorials | price_cheese_12.1.2 | series | {8 series tags} |
| tutorials | price_cheese_12.1.3 | series | {8 series tags} |
| tutorials | price_cheese_12.1.4 | series | {8 series tags} |
| tutorials | price_cheese_12.1.5 | series | {8 series tags} |
| tutorials | price_cheese_12.1.6 | series | {8 series tags} |
| tutorials | price_cheese_12.1.7 | series | {8 series tags} |
| tutorials | price_cheese_12.1.8 | series | {8 series tags} |
| tutorials | price_cheese_12.1.9 | series | {8 series tags} |
| tutorials | price_cheese_12.2.1 | series | {8 series tags} |
| tutorials | price_cheese_12.2.2 | series | {8 series tags} |
| tutorials | price_cheese_12.2.3 | series | {8 series tags} |
| tutorials | price_cheese_12.2.4 | series | {8 series tags} |
| tutorials | price_cheese_12.2.5 | series | {8 series tags} |
| tutorials | price_cheese_12.3.1 | series | {8 series tags} |
| tutorials | price_cheese_12.3.2 | series | {8 series tags} |
| tutorials | price_cheese_12.3.3 | series | {8 series tags} |
| tutorials | price_cheese_12.3.4 | series | {8 series tags} |
| tutorials | price_cheese_13 | series | {8 series tags} |
| tutorials | price_cheese_14 | series | {8 series tags} |
| tutorials | price_cheese_15 | series | {8 series tags} |
| tutorials | price_cheese_2 | series | {8 series tags} |
| tutorials | price_cheese_3 | series | {8 series tags} |
| tutorials | price_cheese_4.1 | series | {8 series tags} |
| tutorials | price_cheese_4.2 | series | {8 series tags} |
| tutorials | price_cheese_5 | series | {8 series tags} |
| tutorials | price_cheese_6 | series | {8 series tags} |
| tutorials | price_cheese_7.1 | series | {8 series tags} |
| tutorials | price_cheese_7.2 | series | {8 series tags} |
| tutorials | price_cheese_7.3 | series | {8 series tags} |
| tutorials | price_cheese_7.4 | series | {8 series tags} |
| tutorials | price_cheese_7.5 | series | {8 series tags} |
| tutorials | price_cheese_7.6 | series | {8 series tags} |
| tutorials | price_cheese_8.1 | series | {8 series tags} |
| tutorials | price_cheese_8.2 | series | {8 series tags} |
| tutorials | price_cheese_8.3 | series | {8 series tags} |
| tutorials | price_cheese_8.4 | series | {8 series tags} |
| tutorials | price_cheese_8.5 | series | {8 series tags} |
| tutorials | price_cheese_8.6 | series | {8 series tags} |
| tutorials | price_cheese_8.7 | series | {8 series tags} |
| tutorials | price_cheese_8.8 | series | {8 series tags} |
| tutorials | price_cheese_8.9 | series | {8 series tags} |
| tutorials | price_cheese_9 | series | {8 series tags} |
| tutorials | price_eggs_1.1.1 | series | {8 series tags} |
| tutorials | price_eggs_1.1.2 | series | {8 series tags} |
| tutorials | price_eggs_1.1.3 | series | {8 series tags} |
| tutorials | price_eggs_1.2 | series | {8 series tags} |
| tutorials | price_eggs_11.1 | series | {8 series tags} |
| tutorials | price_eggs_11.2 | series | {8 series tags} |
| tutorials | price_eggs_12.1.1 | series | {8 series tags} |
| tutorials | price_eggs_12.1.10 | series | {8 series tags} |
| tutorials | price_eggs_12.1.11 | series | {8 series tags} |
| tutorials | price_eggs_12.1.12 | series | {8 series tags} |
| tutorials | price_eggs_12.1.13 | series | {8 series tags} |
| tutorials | price_eggs_12.1.2 | series | {8 series tags} |
| tutorials | price_eggs_12.1.3 | series | {8 series tags} |
| tutorials | price_eggs_12.1.4 | series | {8 series tags} |
| tutorials | price_eggs_12.1.5 | series | {8 series tags} |
| tutorials | price_eggs_12.1.6 | series | {8 series tags} |
| tutorials | price_eggs_12.1.7 | series | {8 series tags} |
| tutorials | price_eggs_12.1.8 | series | {8 series tags} |
| tutorials | price_eggs_12.1.9 | series | {8 series tags} |
| tutorials | price_eggs_12.2.1 | series | {8 series tags} |
| tutorials | price_eggs_12.2.2 | series | {8 series tags} |
| tutorials | price_eggs_12.2.3 | series | {8 series tags} |
| tutorials | price_eggs_12.2.4 | series | {8 series tags} |
| tutorials | price_eggs_12.2.5 | series | {8 series tags} |
| tutorials | price_eggs_12.3.1 | series | {8 series tags} |
| tutorials | price_eggs_12.3.2 | series | {8 series tags} |
| tutorials | price_eggs_12.3.3 | series | {8 series tags} |
| tutorials | price_eggs_12.3.4 | series | {8 series tags} |
| tutorials | price_eggs_13 | series | {8 series tags} |
| tutorials | price_eggs_14 | series | {8 series tags} |
| tutorials | price_eggs_15 | series | {8 series tags} |
| tutorials | price_eggs_2 | series | {8 series tags} |
| tutorials | price_eggs_3 | series | {8 series tags} |
| tutorials | price_eggs_4.1 | series | {8 series tags} |
| tutorials | price_eggs_4.2 | series | {8 series tags} |
| tutorials | price_eggs_5 | series | {8 series tags} |
| tutorials | price_eggs_6 | series | {8 series tags} |
| tutorials | price_eggs_7.1 | series | {8 series tags} |
| tutorials | price_eggs_7.2 | series | {8 series tags} |
| tutorials | price_eggs_7.3 | series | {8 series tags} |
| tutorials | price_eggs_7.4 | series | {8 series tags} |
| tutorials | price_eggs_7.5 | series | {8 series tags} |
| tutorials | price_eggs_7.6 | series | {8 series tags} |
| tutorials | price_eggs_8.1 | series | {8 series tags} |
| tutorials | price_eggs_8.2 | series | {8 series tags} |
| tutorials | price_eggs_8.3 | series | {8 series tags} |
| tutorials | price_eggs_8.4 | series | {8 series tags} |
| tutorials | price_eggs_8.5 | series | {8 series tags} |
| tutorials | price_eggs_8.6 | series | {8 series tags} |
| tutorials | price_eggs_8.7 | series | {8 series tags} |
| tutorials | price_eggs_8.8 | series | {8 series tags} |
| tutorials | price_eggs_8.9 | series | {8 series tags} |
| tutorials | price_eggs_9 | series | {8 series tags} |
| tutorials | price_ham_1.1.1 | series | {8 series tags} |
| tutorials | price_ham_1.1.2 | series | {8 series tags} |
| tutorials | price_ham_1.1.3 | series | {8 series tags} |
| tutorials | price_ham_1.2 | series | {8 series tags} |
| tutorials | price_ham_11.1 | series | {8 series tags} |
| tutorials | price_ham_11.2 | series | {8 series tags} |
| tutorials | price_ham_12.1.1 | series | {8 series tags} |
| tutorials | price_ham_12.1.10 | series | {8 series tags} |
| tutorials | price_ham_12.1.11 | series | {8 series tags} |
| tutorials | price_ham_12.1.12 | series | {8 series tags} |
| tutorials | price_ham_12.1.13 | series | {8 series tags} |
| tutorials | price_ham_12.1.2 | series | {8 series tags} |
| tutorials | price_ham_12.1.3 | series | {8 series tags} |
| tutorials | price_ham_12.1.4 | series | {8 series tags} |
| tutorials | price_ham_12.1.5 | series | {8 series tags} |
| tutorials | price_ham_12.1.6 | series | {8 series tags} |
| tutorials | price_ham_12.1.7 | series | {8 series tags} |
| tutorials | price_ham_12.1.8 | series | {8 series tags} |
| tutorials | price_ham_12.1.9 | series | {8 series tags} |
| tutorials | price_ham_12.2.1 | series | {8 series tags} |
| tutorials | price_ham_12.2.2 | series | {8 series tags} |
| tutorials | price_ham_12.2.3 | series | {8 series tags} |
| tutorials | price_ham_12.2.4 | series | {8 series tags} |
| tutorials | price_ham_12.2.5 | series | {8 series tags} |
| tutorials | price_ham_12.3.1 | series | {8 series tags} |
| tutorials | price_ham_12.3.2 | series | {8 series tags} |
| tutorials | price_ham_12.3.3 | series | {8 series tags} |
| tutorials | price_ham_12.3.4 | series | {8 series tags} |
| tutorials | price_ham_13 | series | {8 series tags} |
| tutorials | price_ham_14 | series | {8 series tags} |
| tutorials | price_ham_15 | series | {8 series tags} |
| tutorials | price_ham_2 | series | {8 series tags} |
| tutorials | price_ham_3 | series | {8 series tags} |
| tutorials | price_ham_4.1 | series | {8 series tags} |
| tutorials | price_ham_4.2 | series | {8 series tags} |
| tutorials | price_ham_5 | series | {8 series tags} |
| tutorials | price_ham_6 | series | {8 series tags} |
| tutorials | price_ham_7.1 | series | {8 series tags} |
| tutorials | price_ham_7.2 | series | {8 series tags} |
| tutorials | price_ham_7.3 | series | {8 series tags} |
| tutorials | price_ham_7.4 | series | {8 series tags} |
| tutorials | price_ham_7.5 | series | {8 series tags} |
| tutorials | price_ham_7.6 | series | {8 series tags} |
| tutorials | price_ham_8.1 | series | {8 series tags} |
| tutorials | price_ham_8.2 | series | {8 series tags} |
| tutorials | price_ham_8.3 | series | {8 series tags} |
| tutorials | price_ham_8.4 | series | {8 series tags} |
| tutorials | price_ham_8.5 | series | {8 series tags} |
| tutorials | price_ham_8.6 | series | {8 series tags} |
| tutorials | price_ham_8.7 | series | {8 series tags} |
| tutorials | price_ham_8.8 | series | {8 series tags} |
| tutorials | price_ham_8.9 | series | {8 series tags} |
| tutorials | price_ham_9 | series | {8 series tags} |
| tutorials | price_juice_1.1.1 | series | {8 series tags} |
| tutorials | price_juice_1.1.2 | series | {8 series tags} |
| tutorials | price_juice_1.1.3 | series | {8 series tags} |
| tutorials | price_juice_1.2 | series | {8 series tags} |
| tutorials | price_juice_11.1 | series | {8 series tags} |
| tutorials | price_juice_11.2 | series | {8 series tags} |
| tutorials | price_juice_12.1.1 | series | {8 series tags} |
| tutorials | price_juice_12.1.10 | series | {8 series tags} |
| tutorials | price_juice_12.1.11 | series | {8 series tags} |
| tutorials | price_juice_12.1.12 | series | {8 series tags} |
| tutorials | price_juice_12.1.13 | series | {8 series tags} |
| tutorials | price_juice_12.1.2 | series | {8 series tags} |
| tutorials | price_juice_12.1.3 | series | {8 series tags} |
| tutorials | price_juice_12.1.4 | series | {8 series tags} |
| tutorials | price_juice_12.1.5 | series | {8 series tags} |
| tutorials | price_juice_12.1.6 | series | {8 series tags} |
| tutorials | price_juice_12.1.7 | series | {8 series tags} |
| tutorials | price_juice_12.1.8 | series | {8 series tags} |
| tutorials | price_juice_12.1.9 | series | {8 series tags} |
| tutorials | price_juice_12.2.1 | series | {8 series tags} |
| tutorials | price_juice_12.2.2 | series | {8 series tags} |
| tutorials | price_juice_12.2.3 | series | {8 series tags} |
| tutorials | price_juice_12.2.4 | series | {8 series tags} |
| tutorials | price_juice_12.2.5 | series | {8 series tags} |
| tutorials | price_juice_12.3.1 | series | {8 series tags} |
| tutorials | price_juice_12.3.2 | series | {8 series tags} |
| tutorials | price_juice_12.3.3 | series | {8 series tags} |
| tutorials | price_juice_12.3.4 | series | {8 series tags} |
| tutorials | price_juice_13 | series | {8 series tags} |
| tutorials | price_juice_14 | series | {8 series tags} |
| tutorials | price_juice_15 | series | {8 series tags} |
| tutorials | price_juice_2 | series | {8 series tags} |
| tutorials | price_juice_3 | series | {8 series tags} |
| tutorials | price_juice_4.1 | series | {8 series tags} |
| tutorials | price_juice_4.2 | series | {8 series tags} |
| tutorials | price_juice_5 | series | {8 series tags} |
| tutorials | price_juice_6 | series | {8 series tags} |
| tutorials | price_juice_7.1 | series | {8 series tags} |
| tutorials | price_juice_7.2 | series | {8 series tags} |
| tutorials | price_juice_7.3 | series | {8 series tags} |
| tutorials | price_juice_7.4 | series | {8 series tags} |
| tutorials | price_juice_7.5 | series | {8 series tags} |
| tutorials | price_juice_7.6 | series | {8 series tags} |
| tutorials | price_juice_8.1 | series | {8 series tags} |
| tutorials | price_juice_8.2 | series | {8 series tags} |
| tutorials | price_juice_8.3 | series | {8 series tags} |
| tutorials | price_juice_8.4 | series | {8 series tags} |
| tutorials | price_juice_8.5 | series | {8 series tags} |
| tutorials | price_juice_8.6 | series | {8 series tags} |
| tutorials | price_juice_8.7 | series | {8 series tags} |
| tutorials | price_juice_8.8 | series | {8 series tags} |
| tutorials | price_juice_8.9 | series | {8 series tags} |
| tutorials | price_juice_9 | series | {8 series tags} |
| tutorials | price_milk_1.1.1 | series | {8 series tags} |
| tutorials | price_milk_1.1.2 | series | {8 series tags} |
| tutorials | price_milk_1.1.3 | series | {8 series tags} |
| tutorials | price_milk_1.2 | series | {8 series tags} |
| tutorials | price_milk_11.1 | series | {8 series tags} |
| tutorials | price_milk_11.2 | series | {8 series tags} |
| tutorials | price_milk_12.1.1 | series | {8 series tags} |
| tutorials | price_milk_12.1.10 | series | {8 series tags} |
| tutorials | price_milk_12.1.11 | series | {8 series tags} |
| tutorials | price_milk_12.1.12 | series | {8 series tags} |
| tutorials | price_milk_12.1.13 | series | {8 series tags} |
| tutorials | price_milk_12.1.2 | series | {8 series tags} |
| tutorials | price_milk_12.1.3 | series | {8 series tags} |
| tutorials | price_milk_12.1.4 | series | {8 series tags} |
| tutorials | price_milk_12.1.5 | series | {8 series tags} |
| tutorials | price_milk_12.1.6 | series | {8 series tags} |
| tutorials | price_milk_12.1.7 | series | {8 series tags} |
| tutorials | price_milk_12.1.8 | series | {8 series tags} |
| tutorials | price_milk_12.1.9 | series | {8 series tags} |
| tutorials | price_milk_12.2.1 | series | {8 series tags} |
| tutorials | price_milk_12.2.2 | series | {8 series tags} |
| tutorials | price_milk_12.2.3 | series | {8 series tags} |
| tutorials | price_milk_12.2.4 | series | {8 series tags} |
| tutorials | price_milk_12.2.5 | series | {8 series tags} |
| tutorials | price_milk_12.3.1 | series | {8 series tags} |
| tutorials | price_milk_12.3.2 | series | {8 series tags} |
| tutorials | price_milk_12.3.3 | series | {8 series tags} |
| tutorials | price_milk_12.3.4 | series | {8 series tags} |
| tutorials | price_milk_13 | series | {8 series tags} |
| tutorials | price_milk_14 | series | {8 series tags} |
| tutorials | price_milk_15 | series | {8 series tags} |
| tutorials | price_milk_2 | series | {8 series tags} |
| tutorials | price_milk_3 | series | {8 series tags} |
| tutorials | price_milk_4.1 | series | {8 series tags} |
| tutorials | price_milk_4.2 | series | {8 series tags} |
| tutorials | price_milk_5 | series | {8 series tags} |
| tutorials | price_milk_6 | series | {8 series tags} |
| tutorials | price_milk_7.1 | series | {8 series tags} |
| tutorials | price_milk_7.2 | series | {8 series tags} |
| tutorials | price_milk_7.3 | series | {8 series tags} |
| tutorials | price_milk_7.4 | series | {8 series tags} |
| tutorials | price_milk_7.5 | series | {8 series tags} |
| tutorials | price_milk_7.6 | series | {8 series tags} |
| tutorials | price_milk_8.1 | series | {8 series tags} |
| tutorials | price_milk_8.2 | series | {8 series tags} |
| tutorials | price_milk_8.3 | series | {8 series tags} |
| tutorials | price_milk_8.4 | series | {8 series tags} |
| tutorials | price_milk_8.5 | series | {8 series tags} |
| tutorials | price_milk_8.6 | series | {8 series tags} |
| tutorials | price_milk_8.7 | series | {8 series tags} |
| tutorials | price_milk_8.8 | series | {8 series tags} |
| tutorials | price_milk_8.9 | series | {8 series tags} |
| tutorials | price_milk_9 | series | {8 series tags} |
| tutorials | volume_bread_1.1.1 | series | {8 series tags} |
| tutorials | volume_bread_1.1.2 | series | {8 series tags} |
| tutorials | volume_bread_1.1.3 | series | {8 series tags} |
| tutorials | volume_bread_1.2 | series | {8 series tags} |
| tutorials | volume_bread_11.1 | series | {8 series tags} |
| tutorials | volume_bread_11.2 | series | {8 series tags} |
| tutorials | volume_bread_12.1.1 | series | {8 series tags} |
| tutorials | volume_bread_12.1.10 | series | {8 series tags} |
| tutorials | volume_bread_12.1.11 | series | {8 series tags} |
| tutorials | volume_bread_12.1.12 | series | {8 series tags} |
| tutorials | volume_bread_12.1.13 | series | {8 series tags} |
| tutorials | volume_bread_12.1.2 | series | {8 series tags} |
| tutorials | volume_bread_12.1.3 | series | {8 series tags} |
| tutorials | volume_bread_12.1.4 | series | {8 series tags} |
| tutorials | volume_bread_12.1.5 | series | {8 series tags} |
| tutorials | volume_bread_12.1.6 | series | {8 series tags} |
| tutorials | volume_bread_12.1.7 | series | {8 series tags} |
| tutorials | volume_bread_12.1.8 | series | {8 series tags} |
| tutorials | volume_bread_12.1.9 | series | {8 series tags} |
| tutorials | volume_bread_12.2.1 | series | {8 series tags} |
| tutorials | volume_bread_12.2.2 | series | {8 series tags} |
| tutorials | volume_bread_12.2.3 | series | {8 series tags} |
| tutorials | volume_bread_12.2.4 | series | {8 series tags} |
| tutorials | volume_bread_12.2.5 | series | {8 series tags} |
| tutorials | volume_bread_12.3.1 | series | {8 series tags} |
| tutorials | volume_bread_12.3.2 | series | {8 series tags} |
| tutorials | volume_bread_12.3.3 | series | {8 series tags} |
| tutorials | volume_bread_12.3.4 | series | {8 series tags} |
| tutorials | volume_bread_13 | series | {8 series tags} |
| tutorials | volume_bread_14 | series | {8 series tags} |
| tutorials | volume_bread_15 | series | {8 series tags} |
| tutorials | volume_bread_2 | series | {8 series tags} |
| tutorials | volume_bread_3 | series | {8 series tags} |
| tutorials | volume_bread_4.1 | series | {8 series tags} |
| tutorials | volume_bread_4.2 | series | {8 series tags} |
| tutorials | volume_bread_5 | series | {8 series tags} |
| tutorials | volume_bread_6 | series | {8 series tags} |
| tutorials | volume_bread_7.1 | series | {8 series tags} |
| tutorials | volume_bread_7.2 | series | {8 series tags} |
| tutorials | volume_bread_7.3 | series | {8 series tags} |
| tutorials | volume_bread_7.4 | series | {8 series tags} |
| tutorials | volume_bread_7.5 | series | {8 series tags} |
| tutorials | volume_bread_7.6 | series | {8 series tags} |
| tutorials | volume_bread_8.1 | series | {8 series tags} |
| tutorials | volume_bread_8.2 | series | {8 series tags} |
| tutorials | volume_bread_8.3 | series | {8 series tags} |
| tutorials | volume_bread_8.4 | series | {8 series tags} |
| tutorials | volume_bread_8.5 | series | {8 series tags} |
| tutorials | volume_bread_8.6 | series | {8 series tags} |
| tutorials | volume_bread_8.7 | series | {8 series tags} |
| tutorials | volume_bread_8.8 | series | {8 series tags} |
| tutorials | volume_bread_8.9 | series | {8 series tags} |
| tutorials | volume_bread_9 | series | {8 series tags} |
| tutorials | volume_cheese_1.1.1 | series | {8 series tags} |
| tutorials | volume_cheese_1.1.2 | series | {8 series tags} |
| tutorials | volume_cheese_1.1.3 | series | {8 series tags} |
| tutorials | volume_cheese_1.2 | series | {8 series tags} |
| tutorials | volume_cheese_11.1 | series | {8 series tags} |
| tutorials | volume_cheese_11.2 | series | {8 series tags} |
| tutorials | volume_cheese_12.1.1 | series | {8 series tags} |
| tutorials | volume_cheese_12.1.10 | series | {8 series tags} |
| tutorials | volume_cheese_12.1.11 | series | {8 series tags} |
| tutorials | volume_cheese_12.1.12 | series | {8 series tags} |
| tutorials | volume_cheese_12.1.13 | series | {8 series tags} |
| tutorials | volume_cheese_12.1.2 | series | {8 series tags} |
| tutorials | volume_cheese_12.1.3 | series | {8 series tags} |
| tutorials | volume_cheese_12.1.4 | series | {8 series tags} |
| tutorials | volume_cheese_12.1.5 | series | {8 series tags} |
| tutorials | volume_cheese_12.1.6 | series | {8 series tags} |
| tutorials | volume_cheese_12.1.7 | series | {8 series tags} |
| tutorials | volume_cheese_12.1.8 | series | {8 series tags} |
| tutorials | volume_cheese_12.1.9 | series | {8 series tags} |
| tutorials | volume_cheese_12.2.1 | series | {8 series tags} |
| tutorials | volume_cheese_12.2.2 | series | {8 series tags} |
| tutorials | volume_cheese_12.2.3 | series | {8 series tags} |
| tutorials | volume_cheese_12.2.4 | series | {8 series tags} |
| tutorials | volume_cheese_12.2.5 | series | {8 series tags} |
| tutorials | volume_cheese_12.3.1 | series | {8 series tags} |
| tutorials | volume_cheese_12.3.2 | series | {8 series tags} |
| tutorials | volume_cheese_12.3.3 | series | {8 series tags} |
| tutorials | volume_cheese_12.3.4 | series | {8 series tags} |
| tutorials | volume_cheese_13 | series | {8 series tags} |
| tutorials | volume_cheese_14 | series | {8 series tags} |
| tutorials | volume_cheese_15 | series | {8 series tags} |
| tutorials | volume_cheese_2 | series | {8 series tags} |
| tutorials | volume_cheese_3 | series | {8 series tags} |
| tutorials | volume_cheese_4.1 | series | {8 series tags} |
| tutorials | volume_cheese_4.2 | series | {8 series tags} |
| tutorials | volume_cheese_5 | series | {8 series tags} |
| tutorials | volume_cheese_6 | series | {8 series tags} |
| tutorials | volume_cheese_7.1 | series | {8 series tags} |
| tutorials | volume_cheese_7.2 | series | {8 series tags} |
| tutorials | volume_cheese_7.3 | series | {8 series tags} |
| tutorials | volume_cheese_7.4 | series | {8 series tags} |
| tutorials | volume_cheese_7.5 | series | {8 series tags} |
| tutorials | volume_cheese_7.6 | series | {8 series tags} |
| tutorials | volume_cheese_8.1 | series | {8 series tags} |
| tutorials | volume_cheese_8.2 | series | {8 series tags} |
| tutorials | volume_cheese_8.3 | series | {8 series tags} |
| tutorials | volume_cheese_8.4 | series | {8 series tags} |
| tutorials | volume_cheese_8.5 | series | {8 series tags} |
| tutorials | volume_cheese_8.6 | series | {8 series tags} |
| tutorials | volume_cheese_8.7 | series | {8 series tags} |
| tutorials | volume_cheese_8.8 | series | {8 series tags} |
| tutorials | volume_cheese_8.9 | series | {8 series tags} |
| tutorials | volume_cheese_9 | series | {8 series tags} |
| tutorials | volume_eggs_1.1.1 | series | {8 series tags} |
| tutorials | volume_eggs_1.1.2 | series | {8 series tags} |
| tutorials | volume_eggs_1.1.3 | series | {8 series tags} |
| tutorials | volume_eggs_1.2 | series | {8 series tags} |
| tutorials | volume_eggs_11.1 | series | {8 series tags} |
| tutorials | volume_eggs_11.2 | series | {8 series tags} |
| tutorials | volume_eggs_12.1.1 | series | {8 series tags} |
| tutorials | volume_eggs_12.1.10 | series | {8 series tags} |
| tutorials | volume_eggs_12.1.11 | series | {8 series tags} |
| tutorials | volume_eggs_12.1.12 | series | {8 series tags} |
| tutorials | volume_eggs_12.1.13 | series | {8 series tags} |
| tutorials | volume_eggs_12.1.2 | series | {8 series tags} |
| tutorials | volume_eggs_12.1.3 | series | {8 series tags} |
| tutorials | volume_eggs_12.1.4 | series | {8 series tags} |
| tutorials | volume_eggs_12.1.5 | series | {8 series tags} |
| tutorials | volume_eggs_12.1.6 | series | {8 series tags} |
| tutorials | volume_eggs_12.1.7 | series | {8 series tags} |
| tutorials | volume_eggs_12.1.8 | series | {8 series tags} |
| tutorials | volume_eggs_12.1.9 | series | {8 series tags} |
| tutorials | volume_eggs_12.2.1 | series | {8 series tags} |
| tutorials | volume_eggs_12.2.2 | series | {8 series tags} |
| tutorials | volume_eggs_12.2.3 | series | {8 series tags} |
| tutorials | volume_eggs_12.2.4 | series | {8 series tags} |
| tutorials | volume_eggs_12.2.5 | series | {8 series tags} |
| tutorials | volume_eggs_12.3.1 | series | {8 series tags} |
| tutorials | volume_eggs_12.3.2 | series | {8 series tags} |
| tutorials | volume_eggs_12.3.3 | series | {8 series tags} |
| tutorials | volume_eggs_12.3.4 | series | {8 series tags} |
| tutorials | volume_eggs_13 | series | {8 series tags} |
| tutorials | volume_eggs_14 | series | {8 series tags} |
| tutorials | volume_eggs_15 | series | {8 series tags} |
| tutorials | volume_eggs_2 | series | {8 series tags} |
| tutorials | volume_eggs_3 | series | {8 series tags} |
| tutorials | volume_eggs_4.1 | series | {8 series tags} |
| tutorials | volume_eggs_4.2 | series | {8 series tags} |
| tutorials | volume_eggs_5 | series | {8 series tags} |
| tutorials | volume_eggs_6 | series | {8 series tags} |
| tutorials | volume_eggs_7.1 | series | {8 series tags} |
| tutorials | volume_eggs_7.2 | series | {8 series tags} |
| tutorials | volume_eggs_7.3 | series | {8 series tags} |
| tutorials | volume_eggs_7.4 | series | {8 series tags} |
| tutorials | volume_eggs_7.5 | series | {8 series tags} |
| tutorials | volume_eggs_7.6 | series | {8 series tags} |
| tutorials | volume_eggs_8.1 | series | {8 series tags} |
| tutorials | volume_eggs_8.2 | series | {8 series tags} |
| tutorials | volume_eggs_8.3 | series | {8 series tags} |
| tutorials | volume_eggs_8.4 | series | {8 series tags} |
| tutorials | volume_eggs_8.5 | series | {8 series tags} |
| tutorials | volume_eggs_8.6 | series | {8 series tags} |
| tutorials | volume_eggs_8.7 | series | {8 series tags} |
| tutorials | volume_eggs_8.8 | series | {8 series tags} |
| tutorials | volume_eggs_8.9 | series | {8 series tags} |
| tutorials | volume_eggs_9 | series | {8 series tags} |
| tutorials | volume_ham_1.1.1 | series | {8 series tags} |
| tutorials | volume_ham_1.1.2 | series | {8 series tags} |
| tutorials | volume_ham_1.1.3 | series | {8 series tags} |
| tutorials | volume_ham_1.2 | series | {8 series tags} |
| tutorials | volume_ham_11.1 | series | {8 series tags} |
| tutorials | volume_ham_11.2 | series | {8 series tags} |
| tutorials | volume_ham_12.1.1 | series | {8 series tags} |
| tutorials | volume_ham_12.1.10 | series | {8 series tags} |
| tutorials | volume_ham_12.1.11 | series | {8 series tags} |
| tutorials | volume_ham_12.1.12 | series | {8 series tags} |
| tutorials | volume_ham_12.1.13 | series | {8 series tags} |
| tutorials | volume_ham_12.1.2 | series | {8 series tags} |
| tutorials | volume_ham_12.1.3 | series | {8 series tags} |
| tutorials | volume_ham_12.1.4 | series | {8 series tags} |
| tutorials | volume_ham_12.1.5 | series | {8 series tags} |
| tutorials | volume_ham_12.1.6 | series | {8 series tags} |
| tutorials | volume_ham_12.1.7 | series | {8 series tags} |
| tutorials | volume_ham_12.1.8 | series | {8 series tags} |
| tutorials | volume_ham_12.1.9 | series | {8 series tags} |
| tutorials | volume_ham_12.2.1 | series | {8 series tags} |
| tutorials | volume_ham_12.2.2 | series | {8 series tags} |
| tutorials | volume_ham_12.2.3 | series | {8 series tags} |
| tutorials | volume_ham_12.2.4 | series | {8 series tags} |
| tutorials | volume_ham_12.2.5 | series | {8 series tags} |
| tutorials | volume_ham_12.3.1 | series | {8 series tags} |
| tutorials | volume_ham_12.3.2 | series | {8 series tags} |
| tutorials | volume_ham_12.3.3 | series | {8 series tags} |
| tutorials | volume_ham_12.3.4 | series | {8 series tags} |
| tutorials | volume_ham_13 | series | {8 series tags} |
| tutorials | volume_ham_14 | series | {8 series tags} |
| tutorials | volume_ham_15 | series | {8 series tags} |
| tutorials | volume_ham_2 | series | {8 series tags} |
| tutorials | volume_ham_3 | series | {8 series tags} |
| tutorials | volume_ham_4.1 | series | {8 series tags} |
| tutorials | volume_ham_4.2 | series | {8 series tags} |
| tutorials | volume_ham_5 | series | {8 series tags} |
| tutorials | volume_ham_6 | series | {8 series tags} |
| tutorials | volume_ham_7.1 | series | {8 series tags} |
| tutorials | volume_ham_7.2 | series | {8 series tags} |
| tutorials | volume_ham_7.3 | series | {8 series tags} |
| tutorials | volume_ham_7.4 | series | {8 series tags} |
| tutorials | volume_ham_7.5 | series | {8 series tags} |
| tutorials | volume_ham_7.6 | series | {8 series tags} |
| tutorials | volume_ham_8.1 | series | {8 series tags} |
| tutorials | volume_ham_8.2 | series | {8 series tags} |
| tutorials | volume_ham_8.3 | series | {8 series tags} |
| tutorials | volume_ham_8.4 | series | {8 series tags} |
| tutorials | volume_ham_8.5 | series | {8 series tags} |
| tutorials | volume_ham_8.6 | series | {8 series tags} |
| tutorials | volume_ham_8.7 | series | {8 series tags} |
| tutorials | volume_ham_8.8 | series | {8 series tags} |
| tutorials | volume_ham_8.9 | series | {8 series tags} |
| tutorials | volume_ham_9 | series | {8 series tags} |
| tutorials | volume_juice_1.1.1 | series | {8 series tags} |
| tutorials | volume_juice_1.1.2 | series | {8 series tags} |
| tutorials | volume_juice_1.1.3 | series | {8 series tags} |
| tutorials | volume_juice_1.2 | series | {8 series tags} |
| tutorials | volume_juice_11.1 | series | {8 series tags} |
| tutorials | volume_juice_11.2 | series | {8 series tags} |
| tutorials | volume_juice_12.1.1 | series | {8 series tags} |
| tutorials | volume_juice_12.1.10 | series | {8 series tags} |
| tutorials | volume_juice_12.1.11 | series | {8 series tags} |
| tutorials | volume_juice_12.1.12 | series | {8 series tags} |
| tutorials | volume_juice_12.1.13 | series | {8 series tags} |
| tutorials | volume_juice_12.1.2 | series | {8 series tags} |
| tutorials | volume_juice_12.1.3 | series | {8 series tags} |
| tutorials | volume_juice_12.1.4 | series | {8 series tags} |
| tutorials | volume_juice_12.1.5 | series | {8 series tags} |
| tutorials | volume_juice_12.1.6 | series | {8 series tags} |
| tutorials | volume_juice_12.1.7 | series | {8 series tags} |
| tutorials | volume_juice_12.1.8 | series | {8 series tags} |
| tutorials | volume_juice_12.1.9 | series | {8 series tags} |
| tutorials | volume_juice_12.2.1 | series | {8 series tags} |
| tutorials | volume_juice_12.2.2 | series | {8 series tags} |
| tutorials | volume_juice_12.2.3 | series | {8 series tags} |
| tutorials | volume_juice_12.2.4 | series | {8 series tags} |
| tutorials | volume_juice_12.2.5 | series | {8 series tags} |
| tutorials | volume_juice_12.3.1 | series | {8 series tags} |
| tutorials | volume_juice_12.3.2 | series | {8 series tags} |
| tutorials | volume_juice_12.3.3 | series | {8 series tags} |
| tutorials | volume_juice_12.3.4 | series | {8 series tags} |
| tutorials | volume_juice_13 | series | {8 series tags} |
| tutorials | volume_juice_14 | series | {8 series tags} |
| tutorials | volume_juice_15 | series | {8 series tags} |
| tutorials | volume_juice_2 | series | {8 series tags} |
| tutorials | volume_juice_3 | series | {8 series tags} |
| tutorials | volume_juice_4.1 | series | {8 series tags} |
| tutorials | volume_juice_4.2 | series | {8 series tags} |
| tutorials | volume_juice_5 | series | {8 series tags} |
| tutorials | volume_juice_6 | series | {8 series tags} |
| tutorials | volume_juice_7.1 | series | {8 series tags} |
| tutorials | volume_juice_7.2 | series | {8 series tags} |
| tutorials | volume_juice_7.3 | series | {8 series tags} |
| tutorials | volume_juice_7.4 | series | {8 series tags} |
| tutorials | volume_juice_7.5 | series | {8 series tags} |
| tutorials | volume_juice_7.6 | series | {8 series tags} |
| tutorials | volume_juice_8.1 | series | {8 series tags} |
| tutorials | volume_juice_8.2 | series | {8 series tags} |
| tutorials | volume_juice_8.3 | series | {8 series tags} |
| tutorials | volume_juice_8.4 | series | {8 series tags} |
| tutorials | volume_juice_8.5 | series | {8 series tags} |
| tutorials | volume_juice_8.6 | series | {8 series tags} |
| tutorials | volume_juice_8.7 | series | {8 series tags} |
| tutorials | volume_juice_8.8 | series | {8 series tags} |
| tutorials | volume_juice_8.9 | series | {8 series tags} |
| tutorials | volume_juice_9 | series | {8 series tags} |
| tutorials | volume_milk_1.1.1 | series | {8 series tags} |
| tutorials | volume_milk_1.1.2 | series | {8 series tags} |
| tutorials | volume_milk_1.1.3 | series | {8 series tags} |
| tutorials | volume_milk_1.2 | series | {8 series tags} |
| tutorials | volume_milk_11.1 | series | {8 series tags} |
| tutorials | volume_milk_11.2 | series | {8 series tags} |
| tutorials | volume_milk_12.1.1 | series | {8 series tags} |
| tutorials | volume_milk_12.1.10 | series | {8 series tags} |
| tutorials | volume_milk_12.1.11 | series | {8 series tags} |
| tutorials | volume_milk_12.1.12 | series | {8 series tags} |
| tutorials | volume_milk_12.1.13 | series | {8 series tags} |
| tutorials | volume_milk_12.1.2 | series | {8 series tags} |
| tutorials | volume_milk_12.1.3 | series | {8 series tags} |
| tutorials | volume_milk_12.1.4 | series | {8 series tags} |
| tutorials | volume_milk_12.1.5 | series | {8 series tags} |
| tutorials | volume_milk_12.1.6 | series | {8 series tags} |
| tutorials | volume_milk_12.1.7 | series | {8 series tags} |
| tutorials | volume_milk_12.1.8 | series | {8 series tags} |
| tutorials | volume_milk_12.1.9 | series | {8 series tags} |
| tutorials | volume_milk_12.2.1 | series | {8 series tags} |
| tutorials | volume_milk_12.2.2 | series | {8 series tags} |
| tutorials | volume_milk_12.2.3 | series | {8 series tags} |
| tutorials | volume_milk_12.2.4 | series | {8 series tags} |
| tutorials | volume_milk_12.2.5 | series | {8 series tags} |
| tutorials | volume_milk_12.3.1 | series | {8 series tags} |
| tutorials | volume_milk_12.3.2 | series | {8 series tags} |
| tutorials | volume_milk_12.3.3 | series | {8 series tags} |
| tutorials | volume_milk_12.3.4 | series | {8 series tags} |
| tutorials | volume_milk_13 | series | {8 series tags} |
| tutorials | volume_milk_14 | series | {8 series tags} |
| tutorials | volume_milk_15 | series | {8 series tags} |
| tutorials | volume_milk_2 | series | {8 series tags} |
| tutorials | volume_milk_3 | series | {8 series tags} |
| tutorials | volume_milk_4.1 | series | {8 series tags} |
| tutorials | volume_milk_4.2 | series | {8 series tags} |
| tutorials | volume_milk_5 | series | {8 series tags} |
| tutorials | volume_milk_6 | series | {8 series tags} |
| tutorials | volume_milk_7.1 | series | {8 series tags} |
| tutorials | volume_milk_7.2 | series | {8 series tags} |
| tutorials | volume_milk_7.3 | series | {8 series tags} |
| tutorials | volume_milk_7.4 | series | {8 series tags} |
| tutorials | volume_milk_7.5 | series | {8 series tags} |
| tutorials | volume_milk_7.6 | series | {8 series tags} |
| tutorials | volume_milk_8.1 | series | {8 series tags} |
| tutorials | volume_milk_8.2 | series | {8 series tags} |
| tutorials | volume_milk_8.3 | series | {8 series tags} |
| tutorials | volume_milk_8.4 | series | {8 series tags} |
| tutorials | volume_milk_8.5 | series | {8 series tags} |
| tutorials | volume_milk_8.6 | series | {8 series tags} |
| tutorials | volume_milk_8.7 | series | {8 series tags} |
| tutorials | volume_milk_8.8 | series | {8 series tags} |
| tutorials | volume_milk_8.9 | series | {8 series tags} |
| tutorials | volume_milk_9 | series | {8 series tags} |
| tutorials | POPU06 | dataset | {4 set tags + 5 series} |
| tutorials | Denmark | series | {2 series tags} |
| tutorials | Finland | series | {2 series tags} |
| tutorials | Iceland | series | {2 series tags} |
| tutorials | Norway | series | {2 series tags} |
| tutorials | Sweden | series | {2 series tags} |
| tutorials | PQR | dataset | {8 set tags + 3 series} |
| tutorials | p | series | {11 series tags} |
| tutorials | q | series | {11 series tags} |
| tutorials | r | series | {11 series tags} |
| tutorials | Prices and Volumes | dataset | {4 set tags + 12 series} |
| tutorials | price_bread | series | {7 series tags} |
| tutorials | price_cheese | series | {7 series tags} |
| tutorials | price_eggs | series | {7 series tags} |
| tutorials | price_ham | series | {7 series tags} |
| tutorials | price_juice | series | {7 series tags} |
| tutorials | price_milk | series | {7 series tags} |
| tutorials | volume_bread | series | {7 series tags} |
| tutorials | volume_cheese | series | {7 series tags} |
| tutorials | volume_eggs | series | {7 series tags} |
| tutorials | volume_ham | series | {7 series tags} |
| tutorials | volume_juice | series | {7 series tags} |
| tutorials | volume_milk | series | {7 series tags} |
| tutorials | XYZ | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {2 series tags} |
| tutorials | y | series | {2 series tags} |
| tutorials | z | series | {2 series tags} |

Filtering: column selection
----------------------------

```python {.marimo}
sample_set = Dataset('PQR')
```

When initialising a variable for an existing `Dataset`, we automatically retrieve the previously stored metadata.

```python {.marimo}
sample_set.tags
```

<!-- @output:TRpd -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">{&#x27;name&#x27;: &#x27;PQR&#x27;,
 &#x27;product group&#x27;: &#x27;essential&#x27;,
 &#x27;repository&#x27;: &#x27;tutorials&#x27;,
 &#x27;series&#x27;: {&#x27;p&#x27;: {&#x27;dataset&#x27;: &#x27;PQR&#x27;,
                  &#x27;name&#x27;: &#x27;p&#x27;,
                  &#x27;product&#x27;: &#x27;coffee&#x27;,
                  &#x27;product group&#x27;: &#x27;essential&#x27;,
                  &#x27;repository&#x27;: &#x27;tutorials&#x27;,
                  &#x27;temporality&#x27;: &#x27;AT&#x27;,
                  &#x27;vare&#x27;: &#x27;kaffe&#x27;,
                  &#x27;varegruppe&#x27;: &#x27;nødvendigheter&#x27;,
                  &#x27;variabel&#x27;: &#x27;pris&#x27;,
                  &#x27;variable&#x27;: &#x27;price&#x27;,
                  &#x27;versioning&#x27;: &#x27;NONE&#x27;},
            &#x27;q&#x27;: {&#x27;dataset&#x27;: &#x27;PQR&#x27;,
                  &#x27;name&#x27;: &#x27;q&#x27;,
                  &#x27;product&#x27;: &#x27;crispbread&#x27;,
                  &#x27;product group&#x27;: &#x27;essential&#x27;,
                  &#x27;repository&#x27;: &#x27;tutorials&#x27;,
                  &#x27;temporality&#x27;: &#x27;AT&#x27;,
                  &#x27;vare&#x27;: &#x27;knekkebrød&#x27;,
                  &#x27;varegruppe&#x27;: &#x27;nødvendigheter&#x27;,
                  &#x27;variabel&#x27;: &#x27;pris&#x27;,
                  &#x27;variable&#x27;: &#x27;price&#x27;,
                  &#x27;versioning&#x27;: &#x27;NONE&#x27;},
            &#x27;r&#x27;: {&#x27;dataset&#x27;: &#x27;PQR&#x27;,
                  &#x27;name&#x27;: &#x27;r&#x27;,
                  &#x27;product&#x27;: &#x27;brown cheese&#x27;,
                  &#x27;product group&#x27;: &#x27;essential&#x27;,
                  &#x27;repository&#x27;: &#x27;tutorials&#x27;,
                  &#x27;temporality&#x27;: &#x27;AT&#x27;,
                  &#x27;vare&#x27;: &#x27;brunost&#x27;,
                  &#x27;varegruppe&#x27;: &#x27;nødvendigheter&#x27;,
                  &#x27;variabel&#x27;: &#x27;pris&#x27;,
                  &#x27;variable&#x27;: &#x27;price&#x27;,
                  &#x27;versioning&#x27;: &#x27;NONE&#x27;}},
 &#x27;temporality&#x27;: &#x27;AT&#x27;,
 &#x27;varegruppe&#x27;: &#x27;nødvendigheter&#x27;,
 &#x27;variabel&#x27;: &#x27;pris&#x27;,
 &#x27;variable&#x27;: &#x27;price&#x27;,
 &#x27;versioning&#x27;: &#x27;NONE&#x27;}</pre>

`Dataset.select()` picks columns.
Column selection may be done by names, simple patterns or regexes.

```python {.marimo}
sample_set['p','q'].plot()
```

<!-- @output:dNNg -->

![png](meta-search-and-filtering_assets/figure-1.png)

Or by metadata tags:

```python {.marimo}
sample_set[{'product':'coffee'}].plot()
```

<!-- @output:wlCL -->

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

<!-- @output:lgWD -->

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

<!-- @output:LJZf -->

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

<!-- @output:jxvo -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">&#91;32m.&#91;0m&#91;32m                                                                        &#91;100%&#93;&#91;0m
=================================== Overview ===================================
Passed Tests:
✓ notebooks/meta-search-and-filtering.py::test_true

Summary:
Total: 1, Passed: 1, Failed: 0, Errors: 0, Skipped: 0
</pre>
