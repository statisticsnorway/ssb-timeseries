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
    name = 'Sample Data',
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
    data = _sample_data.create_df(['x','y','z'], start_date='2020-01-01', end_date='2025-06-01', freq='D'),
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
| tutorials | ABC | dataset | {4 set tags + 6 series} |
| tutorials | PQR | dataset | {4 set tags + 3 series} |
| tutorials | Sample Data | dataset | {6 set tags + 3 series} |
| tutorials | XYZ | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_01467351 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_06690c35 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_07f5ed8c | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_08c7fb95 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_0fb6f4d4 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_10ef582f | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_1296b25d | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_133efd09 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_138928c9 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_14e25934 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_15613b7d | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_1d6fc3f3 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_220102a7 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_233799d2 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_23b8cac9 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_25920447 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_2aee681a | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_2d7ed423 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_31438726 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_314640b0 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_3e58b0cf | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_40ab339d | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_42fc4770 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_43610d5b | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_4505f14b | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_49aeace7 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_4a31e811 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_4bfeeaee | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_4d66e72a | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_5080ae0f | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_56010127 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_5933f406 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_5bfe8212 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_61bebb01 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_63b8ac7f | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_67e6d540 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_684b4573 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_68a63d13 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_6a7c5747 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_71ea833f | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_7839a617 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_7fb2ee25 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_855ac680 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_8732df60 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_89e54dd5 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_8a00e19f | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_8c3febc7 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_8d9c1f58 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_8dda4e6e | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_8f0ba191 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_8fab22ec | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_911f0d75 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_924cd754 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_9873e53b | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_9aaaa640 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_a99daba7 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_ad11302a | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_b75f01e2 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_b79b51e6 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_c390d6b5 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_c6a9e589 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_c7bd5e27 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_cefe8091 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_d2ed8629 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_d769e1f0 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_d84a3af2 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_e3db372b | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_e6d506f7 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_e6da0e82 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_e7444b5d | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_e7b08061 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_eba74d7d | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_ec00eea1 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_ec1374ea | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_eec645de | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_f0f8229a | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_f451f31b | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_f4cbeff3 | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_f8a209fc | dataset | {4 set tags + 3 series} |
| tutorials | XYZ_fac41f35 | dataset | {4 set tags + 3 series} |
|  | probe-pollution | dataset | {4 set tags + 1 series} |
|  | x | dataset | {5 series tags} |

Or the unique repositories:

```python {.marimo}
repositories = {s.repository_name for s in all_sets}
repositories
```

<!-- @output:iLit -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">{&#x27;&lt;team_name&gt;&#x27;, &#x27;tutorials&#x27;}</pre>

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

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">346</pre>

Or, to search for all `Datasets` that contain `Series` tagged with `{'product': 'coffee'}`, just return the `parent` for the `.series` search:

```python {.marimo}
sets_that_have_series_tagged_with = {result.parent for result in timeseries_catalog.series(tags={'product': 'coffee'})}
```

```python {.marimo}
[s.object_name for s in all_sets]
```

<!-- @output:ulZA -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">&#91;&#x27;A Sample Dataset&#x27;,
 &#x27;ABC&#x27;,
 &#x27;PQR&#x27;,
 &#x27;Sample Data&#x27;,
 &#x27;XYZ&#x27;,
 &#x27;XYZ_01467351&#x27;,
 &#x27;XYZ_06690c35&#x27;,
 &#x27;XYZ_07f5ed8c&#x27;,
 &#x27;XYZ_08c7fb95&#x27;,
 &#x27;XYZ_0fb6f4d4&#x27;,
 &#x27;XYZ_10ef582f&#x27;,
 &#x27;XYZ_1296b25d&#x27;,
 &#x27;XYZ_133efd09&#x27;,
 &#x27;XYZ_138928c9&#x27;,
 &#x27;XYZ_14e25934&#x27;,
 &#x27;XYZ_15613b7d&#x27;,
 &#x27;XYZ_1d6fc3f3&#x27;,
 &#x27;XYZ_220102a7&#x27;,
 &#x27;XYZ_233799d2&#x27;,
 &#x27;XYZ_23b8cac9&#x27;,
 &#x27;XYZ_25920447&#x27;,
 &#x27;XYZ_2aee681a&#x27;,
 &#x27;XYZ_2d7ed423&#x27;,
 &#x27;XYZ_31438726&#x27;,
 &#x27;XYZ_314640b0&#x27;,
 &#x27;XYZ_3e58b0cf&#x27;,
 &#x27;XYZ_40ab339d&#x27;,
 &#x27;XYZ_42fc4770&#x27;,
 &#x27;XYZ_43610d5b&#x27;,
 &#x27;XYZ_4505f14b&#x27;,
 &#x27;XYZ_49aeace7&#x27;,
 &#x27;XYZ_4a31e811&#x27;,
 &#x27;XYZ_4bfeeaee&#x27;,
 &#x27;XYZ_4d66e72a&#x27;,
 &#x27;XYZ_5080ae0f&#x27;,
 &#x27;XYZ_56010127&#x27;,
 &#x27;XYZ_5933f406&#x27;,
 &#x27;XYZ_5bfe8212&#x27;,
 &#x27;XYZ_61bebb01&#x27;,
 &#x27;XYZ_63b8ac7f&#x27;,
 &#x27;XYZ_67e6d540&#x27;,
 &#x27;XYZ_684b4573&#x27;,
 &#x27;XYZ_68a63d13&#x27;,
 &#x27;XYZ_6a7c5747&#x27;,
 &#x27;XYZ_71ea833f&#x27;,
 &#x27;XYZ_7839a617&#x27;,
 &#x27;XYZ_7fb2ee25&#x27;,
 &#x27;XYZ_855ac680&#x27;,
 &#x27;XYZ_8732df60&#x27;,
 &#x27;XYZ_89e54dd5&#x27;,
 &#x27;XYZ_8a00e19f&#x27;,
 &#x27;XYZ_8c3febc7&#x27;,
 &#x27;XYZ_8d9c1f58&#x27;,
 &#x27;XYZ_8dda4e6e&#x27;,
 &#x27;XYZ_8f0ba191&#x27;,
 &#x27;XYZ_8fab22ec&#x27;,
 &#x27;XYZ_911f0d75&#x27;,
 &#x27;XYZ_924cd754&#x27;,
 &#x27;XYZ_9873e53b&#x27;,
 &#x27;XYZ_9aaaa640&#x27;,
 &#x27;XYZ_a99daba7&#x27;,
 &#x27;XYZ_ad11302a&#x27;,
 &#x27;XYZ_b75f01e2&#x27;,
 &#x27;XYZ_b79b51e6&#x27;,
 &#x27;XYZ_c390d6b5&#x27;,
 &#x27;XYZ_c6a9e589&#x27;,
 &#x27;XYZ_c7bd5e27&#x27;,
 &#x27;XYZ_cefe8091&#x27;,
 &#x27;XYZ_d2ed8629&#x27;,
 &#x27;XYZ_d769e1f0&#x27;,
 &#x27;XYZ_d84a3af2&#x27;,
 &#x27;XYZ_e3db372b&#x27;,
 &#x27;XYZ_e6d506f7&#x27;,
 &#x27;XYZ_e6da0e82&#x27;,
 &#x27;XYZ_e7444b5d&#x27;,
 &#x27;XYZ_e7b08061&#x27;,
 &#x27;XYZ_eba74d7d&#x27;,
 &#x27;XYZ_ec00eea1&#x27;,
 &#x27;XYZ_ec1374ea&#x27;,
 &#x27;XYZ_eec645de&#x27;,
 &#x27;XYZ_f0f8229a&#x27;,
 &#x27;XYZ_f451f31b&#x27;,
 &#x27;XYZ_f4cbeff3&#x27;,
 &#x27;XYZ_f8a209fc&#x27;,
 &#x27;XYZ_fac41f35&#x27;,
 &#x27;probe-pollution&#x27;,
 &#x27;x&#x27;&#93;</pre>

To get a global tag dictionary:

```python {.marimo}
all_sets_tag_dict = {s.object_name: s.object_tags for s in all_sets}
all_sets_tag_dict['Sample Data']
```

<!-- @output:Pvdt -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">{&#x27;name&#x27;: &#x27;Sample Data&#x27;,
 &#x27;product group&#x27;: &#x27;essential&#x27;,
 &#x27;repository&#x27;: &#x27;tutorials&#x27;,
 &#x27;series&#x27;: {&#x27;p&#x27;: {&#x27;dataset&#x27;: &#x27;Sample Data&#x27;,
                  &#x27;name&#x27;: &#x27;p&#x27;,
                  &#x27;product&#x27;: &#x27;coffee&#x27;,
                  &#x27;product group&#x27;: &#x27;essential&#x27;,
                  &#x27;repository&#x27;: &#x27;tutorials&#x27;,
                  &#x27;temporality&#x27;: &#x27;AT&#x27;,
                  &#x27;variable&#x27;: &#x27;price&#x27;,
                  &#x27;versioning&#x27;: &#x27;NONE&#x27;},
            &#x27;q&#x27;: {&#x27;dataset&#x27;: &#x27;Sample Data&#x27;,
                  &#x27;name&#x27;: &#x27;q&#x27;,
                  &#x27;product&#x27;: &#x27;crispbread&#x27;,
                  &#x27;product group&#x27;: &#x27;essential&#x27;,
                  &#x27;repository&#x27;: &#x27;tutorials&#x27;,
                  &#x27;temporality&#x27;: &#x27;AT&#x27;,
                  &#x27;variable&#x27;: &#x27;price&#x27;,
                  &#x27;versioning&#x27;: &#x27;NONE&#x27;},
            &#x27;r&#x27;: {&#x27;dataset&#x27;: &#x27;Sample Data&#x27;,
                  &#x27;name&#x27;: &#x27;r&#x27;,
                  &#x27;product&#x27;: &#x27;brown cheese&#x27;,
                  &#x27;product group&#x27;: &#x27;essential&#x27;,
                  &#x27;repository&#x27;: &#x27;tutorials&#x27;,
                  &#x27;temporality&#x27;: &#x27;AT&#x27;,
                  &#x27;variable&#x27;: &#x27;price&#x27;,
                  &#x27;versioning&#x27;: &#x27;NONE&#x27;}},
 &#x27;temporality&#x27;: &#x27;AT&#x27;,
 &#x27;variable&#x27;: &#x27;price&#x27;,
 &#x27;versioning&#x27;: &#x27;NONE&#x27;}</pre>

For each item, the `object_tags` in the global dictionary is the same as `Dataset.tags`:

```python {.marimo}
sample_data = Dataset('Sample Data')
sample_data.tags == all_sets_tag_dict['Sample Data']
```

<!-- @output:aLJB -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">True</pre>

Searches that rely only on names and tags can be performed without accessing data:

```python {.marimo}
series = timeseries_catalog.series(tags={'dataset': ['Sample Data', 'XYZ']})
[f"{s.repository_name}/{s.parent}/{s.object_name}" for s in series]
```

<!-- @output:xXTn -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">&#91;&#x27;tutorials/Sample Data/p&#x27;,
 &#x27;tutorials/Sample Data/q&#x27;,
 &#x27;tutorials/Sample Data/r&#x27;,
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
| tutorials | ABC | dataset | {4 set tags + 6 series} |
| tutorials | x_coffee_price | series | {8 series tags} |
| tutorials | x_tea_price | series | {8 series tags} |
| tutorials | y_coffee_price | series | {8 series tags} |
| tutorials | y_tea_price | series | {8 series tags} |
| tutorials | z_coffee_price | series | {8 series tags} |
| tutorials | z_tea_price | series | {8 series tags} |
| tutorials | PQR | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {2 series tags} |
| tutorials | y | series | {2 series tags} |
| tutorials | z | series | {2 series tags} |
| tutorials | Sample Data | dataset | {6 set tags + 3 series} |
| tutorials | p | series | {8 series tags} |
| tutorials | q | series | {8 series tags} |
| tutorials | r | series | {8 series tags} |
| tutorials | XYZ | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {2 series tags} |
| tutorials | y | series | {2 series tags} |
| tutorials | z | series | {2 series tags} |
| tutorials | XYZ_01467351 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_06690c35 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_07f5ed8c | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_08c7fb95 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_0fb6f4d4 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_10ef582f | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_1296b25d | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_133efd09 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_138928c9 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_14e25934 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_15613b7d | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_1d6fc3f3 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_220102a7 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_233799d2 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_23b8cac9 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_25920447 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_2aee681a | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_2d7ed423 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_31438726 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_314640b0 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_3e58b0cf | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_40ab339d | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_42fc4770 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_43610d5b | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_4505f14b | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_49aeace7 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_4a31e811 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_4bfeeaee | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_4d66e72a | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_5080ae0f | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_56010127 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_5933f406 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_5bfe8212 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_61bebb01 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_63b8ac7f | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_67e6d540 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_684b4573 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_68a63d13 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_6a7c5747 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_71ea833f | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_7839a617 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_7fb2ee25 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_855ac680 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_8732df60 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_89e54dd5 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_8a00e19f | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_8c3febc7 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_8d9c1f58 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_8dda4e6e | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_8f0ba191 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_8fab22ec | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_911f0d75 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_924cd754 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_9873e53b | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_9aaaa640 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_a99daba7 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_ad11302a | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_b75f01e2 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_b79b51e6 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_c390d6b5 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_c6a9e589 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_c7bd5e27 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_cefe8091 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_d2ed8629 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_d769e1f0 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_d84a3af2 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_e3db372b | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_e6d506f7 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_e6da0e82 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_e7444b5d | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_e7b08061 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_eba74d7d | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_ec00eea1 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_ec1374ea | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_eec645de | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_f0f8229a | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_f451f31b | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_f4cbeff3 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_f8a209fc | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
| tutorials | XYZ_fac41f35 | dataset | {4 set tags + 3 series} |
| tutorials | x | series | {6 series tags} |
| tutorials | y | series | {6 series tags} |
| tutorials | z | series | {6 series tags} |
|  | probe-pollution | dataset | {4 set tags + 1 series} |
|  | x | series | {2 series tags} |
|  | x | dataset | {5 series tags} |

Filtering: column selection
----------------------------

```python {.marimo}
sample_set = Dataset('Sample Data')
```

When initialising a variable for an existing `Dataset`, we automatically retrieve the previously stored metadata.

```python {.marimo}
sample_set.tags
```

<!-- @output:TRpd -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">{&#x27;name&#x27;: &#x27;Sample Data&#x27;,
 &#x27;product group&#x27;: &#x27;essential&#x27;,
 &#x27;repository&#x27;: &#x27;tutorials&#x27;,
 &#x27;series&#x27;: {&#x27;p&#x27;: {&#x27;dataset&#x27;: &#x27;Sample Data&#x27;,
                  &#x27;name&#x27;: &#x27;p&#x27;,
                  &#x27;product&#x27;: &#x27;coffee&#x27;,
                  &#x27;product group&#x27;: &#x27;essential&#x27;,
                  &#x27;repository&#x27;: &#x27;tutorials&#x27;,
                  &#x27;temporality&#x27;: &#x27;AT&#x27;,
                  &#x27;variable&#x27;: &#x27;price&#x27;,
                  &#x27;versioning&#x27;: &#x27;NONE&#x27;},
            &#x27;q&#x27;: {&#x27;dataset&#x27;: &#x27;Sample Data&#x27;,
                  &#x27;name&#x27;: &#x27;q&#x27;,
                  &#x27;product&#x27;: &#x27;crispbread&#x27;,
                  &#x27;product group&#x27;: &#x27;essential&#x27;,
                  &#x27;repository&#x27;: &#x27;tutorials&#x27;,
                  &#x27;temporality&#x27;: &#x27;AT&#x27;,
                  &#x27;variable&#x27;: &#x27;price&#x27;,
                  &#x27;versioning&#x27;: &#x27;NONE&#x27;},
            &#x27;r&#x27;: {&#x27;dataset&#x27;: &#x27;Sample Data&#x27;,
                  &#x27;name&#x27;: &#x27;r&#x27;,
                  &#x27;product&#x27;: &#x27;brown cheese&#x27;,
                  &#x27;product group&#x27;: &#x27;essential&#x27;,
                  &#x27;repository&#x27;: &#x27;tutorials&#x27;,
                  &#x27;temporality&#x27;: &#x27;AT&#x27;,
                  &#x27;variable&#x27;: &#x27;price&#x27;,
                  &#x27;versioning&#x27;: &#x27;NONE&#x27;}},
 &#x27;temporality&#x27;: &#x27;AT&#x27;,
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
