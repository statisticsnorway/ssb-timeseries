---
title: Meta Basics
marimo-version: 0.24.0
---

# Metadata fundamentals
<!---->
Scope
-----

This guide explains how metadata works in SSB Timeseries.
It covers key concepts like:

- [Repositories, Datasets and Series](#)
- the type system
- tag inheritance from `Dataset` to `Series` objects

It also touches ever so lightly some topics that deserve being covered in more depth:

- [search and filtering](meta-search-and-filtering) with tags
- [tag maintenance](meta-tag-maintenance)
- consuming taxonomies
- calculations with metadata
<!---->
Prerequisites
-------------

``` {note}
The guide assumes that the SSB Timeseries library is installed and that a working configuration is active.
See [the quickstart guide](quickstart) for instructions to that.
```

```python {.marimo}
from ssb_timeseries.config import Config

Config.active().is_valid
```

<!-- @output:lEQa -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">True</pre>

Repositories, Datasets and Series
---------------------------------

Repositories, Datasets and Series are the building blocks of a hierarchy.
`Repositories` are unique within the universe held within a [configuration](..configuring-io).
Repositories contain `Datasets`.
Datasets must be uniquely identified within their repository.
Similarly, `Series` must be uniquely identified within the Datasets they are part of.

Their *names* are unique identifiers within the scope of their parent.
That means that it is possible to have:

```
Repository A
    Dataset PQR
      Series P
      Series Q
      Series R
    Dataset XYZ-1
        Series X
        Series Y
        Series Z
    Dataset XYZ-2
        Series X
        Series Y
        Series Z
Repository B
    Dataset PQR
        Series P
        Series Q
        Series R
```

This scoping provides flexibility.
It allows the same logic for different datasets.
Creating a new dataset with *almost* identical content makes sense and allows easy transitions and comparisons in cases of changing methodologies or classifications.
It also creates a potential for confusion.

`Datasets` and `Series` are also associated with both technical and purely descriptive metadata via `tags`.
While the "long name" `Repository/Dataset/Series` carries the identity of an individual series, its `tags` defines its meaning.
If two complete sets of descriptions (tags) are identical, that implies identity.
If there is a "real" difference (as opposed to merely a copy existing) it should show up in the metadata.
<!---->
The type system
---------------

<!-- @output:SFPL -->

SeriesTypes are defined by combinations of attributes that have technical implications for time series datasets.

Notable examples are Versioning and Temporality, but a few more may be added later.

 - Versioning refers to how revisions of data are identified (named).
 - Temporality describes the time dimensionality of each data point; notably duration or lack thereof.

```python {.marimo}
from ssb_timeseries.dataset import Dataset
from ssb_timeseries.types import SeriesType, Versioning, Temporality
```

Creating a Dataset
------------------

```python {.marimo}
some_data = dataframe_like_data_from_file_or_query()
```

When creating a `Dataset` for the first time, a `name`, a `type` and some data are required.

Specifying a `repository` is optional.
If not specified, the configuration will determine which one is used, if there is more than one.

```python {.marimo}
sample_set = Dataset(
    name = 'Sample Data',
    data_type = SeriesType(Versioning.NONE, Temporality.AT),
    data = some_data,
)
sample_set.save()
```

```python {.marimo}
print(repr(sample_set))
```

<!-- @output:ZHCJ -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">Dataset(name=&quot;Sample Data&quot;, repository=&quot;tutorials&quot;, data_type=SeriesType(Versioning.NONE,Temporality.AT), as_of_tz=None)
</pre>

These attributes are technically significant.
If any of them are changed, it changes *where* or *how* the data is stored, and how it may be used.

The technical attributes are both object properties and reflected in `Dataset.tags`. This minimal amount of mandatory metadata is applied creation time and can not be changed without running the risk of breaking functionality.

```python {.marimo}
sample_set.tags
```

<!-- @output:qnkX -->

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

Note how `Dataset.name` becomes `Series.dataset` in the tags, while the technical properties are inherited directly.
The datatype dimensions are reflected in both in `.versioning` and the single `valid_at` column in `.data`:

```python {.marimo}
sample_set.data
```

<!-- @output:Vxnm -->

| valid_at | p | q | r |
| --- | --- | --- | --- |
| 2020-01-01 | 110.0 | 110.0 | 110.0 |
| 2020-01-02 | 90.0 | 100.0 | 90.0 |
| 2020-01-03 | 90.0 | 100.0 | 90.0 |
| 2020-01-04 | 90.0 | 100.0 | 110.0 |
| 2020-01-05 | 80.0 | 100.0 | 110.0 |
| ... | ... | ... | ... |
| 2025-05-28 | 100.0 | 90.0 | 100.0 |
| 2025-05-29 | 100.0 | 100.0 | 100.0 |
| 2025-05-30 | 90.0 | 110.0 | 100.0 |
| 2025-05-31 | 110.0 | 100.0 | 90.0 |
| 2025-06-01 | 100.0 | 90.0 | 110.0 |

To apply more than the minimal set of technical tags, we need to "tag" the dataset and series.

```python {.marimo}
sample_set.tag_dataset(tags={'variable': 'price','product group': 'essential'})

sample_set.tag_series('x',tags={'product': 'coffee'})
sample_set.tag_series('y',tags={'product': 'crispbread'})
sample_set.tag_series('z',tags={'product': 'brown cheese'})

sample_set.save()
sample_set.tags
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

Initialising a variable for an existing `Dataset`, we retrieve the previously stored metadata.

```python {.marimo}
xyz = Dataset('Sample Data')
```

```python {.marimo}
xyz.tags
```

<!-- @output:ZBYS -->

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

## Selecting series

Series can be selected from the dataset by name, regex patterns or tags.

```python {.marimo}
xyz['x','y'].plot()
```

<!-- @output:nHfw -->

![png](meta-basics_assets/figure-1.png)

And tags as well:

```python {.marimo}
xyz[{'area': 'z'}].plot()
```

<!-- @output:AjVT -->

![png](meta-basics_assets/figure-2.png)

With the simple "XYZ" dataset this is not so exciting.
However, selection by tags becomes very powerful for bigger datasets.

```python {.marimo}
bigger_data = mock_interval_data_from_file_or_query(start='2025-01-01', end='2025-06-01')
```

```python {.marimo}
az = Dataset(
    name = 'AZ_drinks',
    data_type = SeriesType('NONE', 'FROM_TO'),
    data = bigger_data,
    attributes=['store','variable','product', 'region'],
)
#az.save()
```

<!-- @output:TXez -->

The `az` set has 2080 series.

At this scale, it is not longer practical to deal with individual series:

```python {.marimo}
az.tags["series"]["a_price_coffee_NW"]
```

<!-- @output:wlCL -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">{&#x27;dataset&#x27;: &#x27;AZ_drinks&#x27;,
 &#x27;name&#x27;: &#x27;a_price_coffee_NW&#x27;,
 &#x27;product&#x27;: &#x27;coffee&#x27;,
 &#x27;region&#x27;: &#x27;NW&#x27;,
 &#x27;repository&#x27;: &#x27;tutorials&#x27;,
 &#x27;store&#x27;: &#x27;a&#x27;,
 &#x27;temporality&#x27;: &#x27;FROM_TO&#x27;,
 &#x27;variable&#x27;: &#x27;price&#x27;,
 &#x27;versioning&#x27;: &#x27;NONE&#x27;}</pre>

While one could do something like looping over name patterns, organising the data in subsets identified by tags is much more practical:

```python {.marimo}
prices = az[{'variable':'price'}]
volumes = az[{'variable':'volume'}]
```

Series in `prices`:

<!-- @output:dGlV -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">a_price_beer_N a_price_beer_NE ... z_price_wine_SE z_price_wine_SW z_price_wine_W

 len(prices.series)=1040
</pre>

New objects and tag maintenance
-------------------------------

The selection returns new dataset instnances for which both the data and the metadata have been filtered to match the criteria.
The retrieved data is sorted to allow calculations to be performed without complicated matching.

```python {.marimo}
revenue = prices * volumes
```

(Explicit matching may still be required in some corner cases.)

After a calculations, the original metadata will rarely be accurate anymore. Some functions update the metadata automatically, but in general tags need to be updated after calculations.

```python {.marimo}
revenue.rename('AZ_drinks', ('prices', 'volumes'))
revenue.replace_tags(({'variable':'price'},{'variable':'revenue'}))
revenue.plot()
```

<!-- @output:fwwy -->

![png](meta-basics_assets/figure-3.png)

See [tag maintenance](meta-tag-maintenance) or [calculations with metadata](calc-with-metadata) for more about either topic.
<!---->
Formal taxonomies
-----------------

As seen in the code above, the SSB Timeseries library implements tags as key value pairs and handles them through Python dictionaries.
This is a very lightweight approach that provides a lot of flexibility.
Just about anything that fits into the key value structure goes.

A more formal approach will put some governance and standardisation on which attributes to use, how to name them, and which values are allowed.

Integrating with such formal structures - and metadata systems - through the `meta` module is in the shaping.
The design philosophy is to keep the integration lightweight and configurable.
At the core is the idea that `attributes` take their `values` defined in a `Taxonomy`.

The code snippet below shows how a taxonomy may be consumed from Statistics Norway's taxonomy system KLASS.

```python {.marimo}
from ssb_timeseries.meta import Taxonomy

klass157 = Taxonomy(klass_id=157)
klass157.print_tree()
```

<!-- @output:jxvo -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">╙── 0
    ├─╼ 1
    │   ├─╼ 1.1
    │   │   ├─╼ 1.1.1
    │   │   ├─╼ 1.1.2
    │   │   └─╼ 1.1.3
    │   └─╼ 1.2
    ├─╼ 11
    │   ├─╼ 11.1
    │   └─╼ 11.2
    ├─╼ 12
    │   ├─╼ 12.1
    │   │   ├─╼ 12.1.1
    │   │   ├─╼ 12.1.10
    │   │   ├─╼ 12.1.11
    │   │   ├─╼ 12.1.12
    │   │   ├─╼ 12.1.13
    │   │   ├─╼ 12.1.2
    │   │   ├─╼ 12.1.3
    │   │   ├─╼ 12.1.4
    │   │   ├─╼ 12.1.5
    │   │   ├─╼ 12.1.6
    │   │   ├─╼ 12.1.7
    │   │   ├─╼ 12.1.8
    │   │   └─╼ 12.1.9
    │   ├─╼ 12.2
    │   │   ├─╼ 12.2.1
    │   │   ├─╼ 12.2.2
    │   │   ├─╼ 12.2.3
    │   │   ├─╼ 12.2.4
    │   │   └─╼ 12.2.5
    │   └─╼ 12.3
    │       ├─╼ 12.3.1
    │       ├─╼ 12.3.2
    │       ├─╼ 12.3.3
    │       └─╼ 12.3.4
    ├─╼ 13
    ├─╼ 14
    ├─╼ 15
    ├─╼ 2
    ├─╼ 3
    ├─╼ 4
    │   ├─╼ 4.1
    │   └─╼ 4.2
    ├─╼ 5
    ├─╼ 6
    ├─╼ 7
    │   ├─╼ 7.1
    │   ├─╼ 7.2
    │   ├─╼ 7.3
    │   ├─╼ 7.4
    │   ├─╼ 7.5
    │   └─╼ 7.6
    ├─╼ 8
    │   ├─╼ 8.1
    │   ├─╼ 8.2
    │   ├─╼ 8.3
    │   ├─╼ 8.4
    │   ├─╼ 8.5
    │   ├─╼ 8.6
    │   ├─╼ 8.7
    │   ├─╼ 8.8
    │   └─╼ 8.9
    └─╼ 9
</pre>

In this example the taxonomy has a hierarcical structure.
Hierarchical (or even graph) structures may be used for [calculations](calc-with-metadata), as long as the tag values match a taxonomy.

While features for [tag mainatenance](meta-tag-maintenance) allow fixing some mistakes after the fact,
attribute structures are important considerations that should not be taken lightly.
They are, after all, a subset of ["naming things"](https://martinfowler.com/bliki/TwoHardThings.html).
<!---->
Data catalog
------------

The SSB Timeseries library can be configured to deal with the metadata in more than one way.
The library configuration allows setting up metadata repositories independent of the data storage.
That allows multiple data repositories to share a single metadata repository.
At the most technical level, storage comes down to IO implementation, but the separate configurations allow the metadata to be stored more than once. It can be stored both near the actual data, say in header or footer fields of file based storage, and in a sentral repository accessed through an API.

Regardless of setup, multiple metadata repositories in a configuration can be treated as a single catalog.
Collecting structured metadata in one place makes it easier to search.

```python {.marimo}
from ssb_timeseries import get_catalog

our_timeseries_database = get_catalog()
all_the_datasets = our_timeseries_database.datasets()
```

```python {.marimo}
type(all_the_datasets)
```

<!-- @output:zlud -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">&lt;class &#x27;list&#x27;&gt;</pre>

```python {.marimo}
type(all_the_datasets[0])
```

<!-- @output:tZnO -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">&lt;class &#x27;ssb_timeseries.catalog.CatalogItem&#x27;&gt;</pre>

```python {.marimo}
[catalog_item.object_name for catalog_item in all_the_datasets]
```

<!-- @output:xvXZ -->

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

```python {.marimo disabled="true"}
import pandas as pd
pd.DataFrame(all_the_datasets )
```

The list above should correspond to what we find in our file based repository:

<!-- @output:cEAS -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">timeseries/
├── AS_OF_AT/
│   ├── Sample Data/
│   │   ├── Sample Data-as_of_2023-12-31T230000+0000-data.parquet
│   │   ├── Sample Data-as_of_2024-01-31T230000+0000-data.parquet
│   │   ├── Sample Data-as_of_2024-02-29T230000+0000-data.parquet
│   │   ├── Sample Data-as_of_2024-03-31T220000+0000-data.parquet
│   │   ├── Sample Data-as_of_2024-04-30T220000+0000-data.parquet
│   │   ├── Sample Data-as_of_2024-05-31T220000+0000-data.parquet
│   │   ├── Sample Data-as_of_2024-06-30T220000+0000-data.parquet
│   │   ├── Sample Data-as_of_2024-07-31T220000+0000-data.parquet
│   │   ├── Sample Data-as_of_2024-08-31T220000+0000-data.parquet
│   │   ├── Sample Data-as_of_2024-09-30T220000+0000-data.parquet
│   │   ├── Sample Data-as_of_2024-10-31T230000+0000-data.parquet
│   │   ├── Sample Data-as_of_2024-11-30T230000+0000-data.parquet
│   │   ├── Sample Data-as_of_2024-12-31T230000+0000-data.parquet
│   │   ├── Sample Data-as_of_2025-01-31T230000+0000-data.parquet
│   │   ├── Sample Data-as_of_2025-02-28T230000+0000-data.parquet
│   │   ├── Sample Data-as_of_2025-03-31T220000+0000-data.parquet
│   │   ├── Sample Data-as_of_2025-04-30T220000+0000-data.parquet
│   │   ├── Sample Data-as_of_2025-05-31T220000+0000-data.parquet
│   │   ├── Sample Data-as_of_2025-06-30T220000+0000-data.parquet
│   │   ├── Sample Data-as_of_2025-07-31T220000+0000-data.parquet
│   │   ├── Sample Data-as_of_2025-08-31T220000+0000-data.parquet
│   │   ├── Sample Data-as_of_2025-09-30T220000+0000-data.parquet
│   │   ├── Sample Data-as_of_2025-10-31T230000+0000-data.parquet
│   │   └── Sample Data-as_of_2025-11-30T230000+0000-data.parquet
│   └── XYZ/
│       ├── XYZ-as_of_2025-04-30T220000+0000-data.parquet
│       ├── XYZ-as_of_2025-05-31T220000+0000-data.parquet
│       ├── XYZ-as_of_2025-08-02T220000+0000-data.parquet
│       ├── XYZ-as_of_2025-08-03T220000+0000-data.parquet
│       ├── XYZ-as_of_2025-08-04T220000+0000-data.parquet
│       ├── XYZ-as_of_2025-08-05T220000+0000-data.parquet
│       └── XYZ-as_of_2025-08-06T220000+0000-data.parquet
├── AS_OF_FROM_TO/
│   └── Prices and Volumes/
│       ├── Prices and Volumes-as_of_2023-12-31T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2024-01-31T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2024-02-29T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2024-03-31T220000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2024-04-30T220000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2024-05-31T220000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2024-06-30T220000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2024-07-31T220000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2024-08-31T220000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2024-09-30T220000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2024-10-31T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2024-11-30T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2024-12-31T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2025-01-31T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2025-02-28T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2025-03-31T220000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2025-04-30T220000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2025-05-31T220000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2025-06-30T220000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2025-07-31T220000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2025-08-31T220000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2025-09-30T220000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2025-10-31T230000+0000-data.parquet
│       └── Prices and Volumes-as_of_2025-11-30T230000+0000-data.parquet
├── metadata/
│   ├── A Sample Dataset-metadata.json
│   ├── AZ Drinks-metadata.json
│   ├── AZ_drikkevarer-metadata.json
│   ├── AZ_drinks-metadata.json
│   ├── AZ_omsetning-metadata.json
│   ├── More Prices and Volumes-metadata.json
│   ├── PQR-metadata.json
│   ├── Prices and Volumes-metadata.json
│   ├── Sample Data-metadata.json
│   └── XYZ-metadata.json
├── NONE_AT/
│   ├── A Sample Dataset/
│   │   └── A Sample Dataset-latest-data.parquet
│   ├── PQR/
│   │   └── PQR-latest-data.parquet
│   └── XYZ/
│       └── XYZ-latest-data.parquet
└── NONE_FROM_TO/
    ├── AZ Drinks/
    │   └── AZ Drinks-latest-data.parquet
    ├── AZ_drikkevarer/
    │   └── AZ_drikkevarer-latest-data.parquet
    ├── AZ_drinks/
    │   └── AZ_drinks-latest-data.parquet
    ├── AZ_omsetning/
    │   └── AZ_omsetning-latest-data.parquet
    └── More Prices and Volumes/
        └── More Prices and Volumes-latest-data.parquet

</pre>

See the guide to [search and filtering](meta-search-and-filtering) for more details on the `Catalog`.

```python {.marimo name="test_success"}
# @supress

def test_success():
    assert True
```

<!-- @output:EJmg -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">&#91;32m.&#91;0m&#91;32m                                                                        &#91;100%&#93;&#91;0m
=================================== Overview ===================================
Passed Tests:
&#91;1m&#91;32m&#91;22m✓&#91;0m&#91;0m notebooks/meta-basics.py::test_success

Summary:
Total: 1, Passed: 1, Failed: 0, Errors: 0, Skipped: 0
</pre>

```python {.marimo}
testing.run_and_report([test_success])
```
