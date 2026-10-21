---
title: Data Archiving And Sharing
marimo-version: 0.24.2
---

Archiving and sharing
=====================

Motivation
----------

Statistics Norway collects data from a vast number of sources.
We have a broad mandate, but are also subject to strict regulations and reviews.
Our production platform is designed to limit access to data, and we commit to a formal process model so that we can prove that we operate within the mandate.
Persisting data in stable states is a key element in how this translates into practice.

The practical implication is that at defined points in the statistics production pipeline, the data has to be persisted.
Stricter requirements apply than just saving.
First, immutability: the persisted data must be stored "forever", without being subject to change.
Second, conventions apply to storage location, technology and format, naming and documentation.

Data shared between different statistics are subject to similar restrictions.
The requirements are not particularily hard to meet, per se.
In both cases, the conventions are designed for transparency and review, not for efficient data handling.

Our solution is to separate "archiving" and "sharing" from ordinary "reading" and "writing".
In trivial use cases it does not really matter.
Beyond those, we keep our options open, and can switch technologies freely while conventions dictate that archiving and sharing are always file based.
<!---->
Scope
-----

Here we focus on the practical side.
The I/O abstractions are described in more detail in the architecture section, and there is a separate guide to I/O configurations.
<!---->
Prerequisites
-------------

Since the sole purpose is to separate archiving and sharing from reading and writing, additional configuration is required.
repository entry:

```python {.marimo}
CONFIG.archives
```

<!-- @output:BYtC -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">{&#x27;default&#x27;: {&#x27;directory&#x27;: {&#x27;handler&#x27;: &#x27;archive&#x27;,
                           &#x27;options&#x27;: {&#x27;path&#x27;: &#x27;/home/bernhard/timeseries/archives&#x27;}},
             &#x27;options&#x27;: {&#x27;archive_format&#x27;: &#x27;ssb&#x27;}}}</pre>

With `handler` matching a listing in `io_handlers`, here "archive":

```python {.marimo}
CONFIG.io_handlers
```

<!-- @output:Kclp -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">{&#x27;archive&#x27;: {&#x27;handler&#x27;: &#x27;ssb_timeseries.io.archiving.Archive&#x27;, &#x27;options&#x27;: {}},
 &#x27;hive-partitioned-parquet&#x27;: {&#x27;handler&#x27;: &#x27;ssb_timeseries.io.pyarrow_hive.HiveFileSystem&#x27;,
                              &#x27;options&#x27;: {}},
 &#x27;json&#x27;: {&#x27;handler&#x27;: &#x27;ssb_timeseries.io.json_metadata.JsonMetaIO&#x27;,
          &#x27;options&#x27;: {}},
 &#x27;simple-parquet&#x27;: {&#x27;handler&#x27;: &#x27;ssb_timeseries.io.pyarrow_simple.FileSystem&#x27;,
                    &#x27;options&#x27;: {}}}</pre>

With global configurations in place, dataset level properties control how individual sets are archived or shared by pointing to the repository entry. Ours have the orginal name "default", and the option `archive_format` is "ssb", which is what tells the built in archive handler to apply the Statistics Norway convention.

```python {.marimo}
from ssb_timeseries.dataset import Dataset
from ssb_timeseries.types import NONE_AT
```

We start with an empty repository:

<!-- @output:DnEU -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">timeseries/
├── AS_OF_AT/
│   └── POPU06/
│       ├── POPU06-as_of_2024-12-31T230000+0000-data.parquet
│       ├── POPU06-as_of_2025-01-31T230000+0000-data.parquet
│       ├── POPU06-as_of_2025-10-31T230000+0000-data.parquet
│       ├── POPU06-as_of_2025-11-30T230000+0000-data.parquet
│       └── ...
├── AS_OF_FROM_TO/
│   └── Prices and Volumes/
│       ├── Prices and Volumes-as_of_2023-12-31T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2024-01-31T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2025-10-31T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2025-11-30T230000+0000-data.parquet
│       └── ...
├── metadata/
│   ├── More Prices and Volumes-metadata.json
│   ├── POPU06-metadata.json
│   ├── Sample Dataset to be Archived-metadata.json
│   ├── XYZ-metadata.json
│   └── ...
├── NONE_AT/
│   ├── Sample Dataset to be Archived/
│   │   └── Sample Dataset to be Archived-latest-data.parquet
│   └── XYZ/
│       └── XYZ-latest-data.parquet
└── NONE_FROM_TO/
    └── More Prices and Volumes/
        └── More Prices and Volumes-latest-data.parquet

</pre>

... and begin by creating some sample data.
<!---->
For data in between the stable "checkpoint" states, naming is unconstrained.
The naming conventions for archiving limits character usage and allow no spaces.

```python {.marimo}
set_name = 'Sample Dataset to be Archived'
```

```python {.marimo}
p = Dataset(
    name = set_name,
    data_type = NONE_AT,
    data = xyz_at(),
).save()
```

After the `.save()` we should see one dataset:

```python {.marimo}
treee()
```

<!-- @output:DnEU -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">timeseries/
├── AS_OF_AT/
│   └── POPU06/
│       ├── POPU06-as_of_2024-12-31T230000+0000-data.parquet
│       ├── POPU06-as_of_2025-01-31T230000+0000-data.parquet
│       ├── POPU06-as_of_2025-10-31T230000+0000-data.parquet
│       ├── POPU06-as_of_2025-11-30T230000+0000-data.parquet
│       └── ...
├── AS_OF_FROM_TO/
│   └── Prices and Volumes/
│       ├── Prices and Volumes-as_of_2023-12-31T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2024-01-31T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2025-10-31T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2025-11-30T230000+0000-data.parquet
│       └── ...
├── metadata/
│   ├── More Prices and Volumes-metadata.json
│   ├── POPU06-metadata.json
│   ├── Sample Dataset to be Archived-metadata.json
│   ├── XYZ-metadata.json
│   └── ...
├── NONE_AT/
│   ├── Sample Dataset to be Archived/
│   │   └── Sample Dataset to be Archived-latest-data.parquet
│   └── XYZ/
│       └── XYZ-latest-data.parquet
└── NONE_FROM_TO/
    └── More Prices and Volumes/
        └── More Prices and Volumes-latest-data.parquet

</pre>

Dataset properties
------------------
<!---->
We read the data back, just because we can, and because there is no guarantee that the archiving happens in the same process (or at the same frequency) that the data is gathered, calculated or saved.

The dataset must be saved before it is archived, though.
Otherwise a `FileNotFound` error will be raised.

```python {.marimo}
q = Dataset(set_name)
```

```python {.marimo}
statistics_product = 'Our Product'
q.process_stage = 'statistics'
q.product = statistics_product
```

A team can be responsible for one or more "statistical products" or "data products".
Outsiders to Statistics Norway can safely ignore the nuance.

The `process stage` is a named stable state, or "checkpoint" in the production process.
<!---->
## Archiving
<!---->
Then, the actual action happens via `Dataset.archive()`.
<!---->
The `.archive()` function takes care of both archiving and sharing.
This reflects the conventions that applies in Statistics Norway, but is technically a remnant from early PoC phase.

**Warning:** It may be subject to change later.
<!---->
Archiving writes to the archive and, for each sharing key, to the
destination that key names.
The archive path is built from the tags, so `product` and `process_stage` are set
before the dataset is archived.

```python {.marimo}
print_err_and_continue( q.archive )
```

<!-- @output:NCOB -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">-------------------------------- ERROR OCCURRED --------------------------------
The dataset name &#x27;Sample Dataset to be Archived&#x27; contains characters that the
&#x27;ssb&#x27; archive format does not allow: &#x27; &#x27;. Allowed characters are
&#x27;abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_&#x27;.
--------------------------------------------------------------------------------
</pre>

Surprise.
We ignore proper change management and rename.
Note that the rename creates a new dataset, and any data written earlier that was not read back now will be left behind in the old set.

```python {.marimo}
q.rename("SampleDatasetForArchiving")
q.save() # it will fail again if we do not save first.
q.archive()
```

```python {.marimo}
print(treee())
```

<!-- @output:mWxS -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">timeseries/
├── archives/
│   ├── Our Product/
│   │   └── statistics/
│   │       └── SampleDatasetForArchiving/
│   │           └── SampleDatasetForArchiving_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v1.parquet
│   ├── statistics/
│   │   └── PQR/
│   │       └── PQR_p2019-12-31T23-00-00.000+00-00_p2025-05-31T22-00-00.000+00-00_v1.parquet
│   └── XYZ/
│       ├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v1.parquet
│       ├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v2.parquet
│       ├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v3.parquet
│       └── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v4.parquet
├── AS_OF_AT/
│   └── POPU06/
│       ├── POPU06-as_of_2024-12-31T230000+0000-data.parquet
│       ├── POPU06-as_of_2025-01-31T230000+0000-data.parquet
│       ├── POPU06-as_of_2025-10-31T230000+0000-data.parquet
│       ├── POPU06-as_of_2025-11-30T230000+0000-data.parquet
│       └── ...
├── AS_OF_FROM_TO/
│   └── Prices and Volumes/
│       ├── Prices and Volumes-as_of_2023-12-31T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2024-01-31T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2025-10-31T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2025-11-30T230000+0000-data.parquet
│       └── ...
├── metadata/
│   ├── More Prices and Volumes-metadata.json
│   ├── POPU06-metadata.json
│   ├── SampleDatasetForArchiving-metadata.json
│   ├── XYZ-metadata.json
│   └── ...
├── NONE_AT/
│   ├── PQR/
│   │   └── PQR-latest-data.parquet
│   ├── Sample Dataset to be Archived/
│   │   └── Sample Dataset to be Archived-latest-data.parquet
│   ├── SampleDatasetForArchiving/
│   │   └── SampleDatasetForArchiving-latest-data.parquet
│   └── XYZ/
│       └── XYZ-latest-data.parquet
├── NONE_FROM_TO/
│   └── More Prices and Volumes/
│       └── More Prices and Volumes-latest-data.parquet
└── shared/
    └── default/
        ├── statistics/
        │   └── PQR/
        │       └── PQR_p2019-12-31T23-00-00.000+00-00_p2025-05-31T22-00-00.000+00-00_v1.parquet
        └── XYZ/
            ├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v1.parquet
            ├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v2.parquet
            ├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v3.parquet
            └── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v4.parquet

None
</pre>

<!-- @output:mWxS -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">timeseries/
├── archives/
│   ├── Our Product/
│   │   └── statistics/
│   │       └── SampleDatasetForArchiving/
│   │           └── SampleDatasetForArchiving_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v1.parquet
│   ├── statistics/
│   │   └── PQR/
│   │       └── PQR_p2019-12-31T23-00-00.000+00-00_p2025-05-31T22-00-00.000+00-00_v1.parquet
│   └── XYZ/
│       ├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v1.parquet
│       ├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v2.parquet
│       ├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v3.parquet
│       └── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v4.parquet
├── AS_OF_AT/
│   └── POPU06/
│       ├── POPU06-as_of_2024-12-31T230000+0000-data.parquet
│       ├── POPU06-as_of_2025-01-31T230000+0000-data.parquet
│       ├── POPU06-as_of_2025-10-31T230000+0000-data.parquet
│       ├── POPU06-as_of_2025-11-30T230000+0000-data.parquet
│       └── ...
├── AS_OF_FROM_TO/
│   └── Prices and Volumes/
│       ├── Prices and Volumes-as_of_2023-12-31T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2024-01-31T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2025-10-31T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2025-11-30T230000+0000-data.parquet
│       └── ...
├── metadata/
│   ├── More Prices and Volumes-metadata.json
│   ├── POPU06-metadata.json
│   ├── SampleDatasetForArchiving-metadata.json
│   ├── XYZ-metadata.json
│   └── ...
├── NONE_AT/
│   ├── PQR/
│   │   └── PQR-latest-data.parquet
│   ├── Sample Dataset to be Archived/
│   │   └── Sample Dataset to be Archived-latest-data.parquet
│   ├── SampleDatasetForArchiving/
│   │   └── SampleDatasetForArchiving-latest-data.parquet
│   └── XYZ/
│       └── XYZ-latest-data.parquet
├── NONE_FROM_TO/
│   └── More Prices and Volumes/
│       └── More Prices and Volumes-latest-data.parquet
└── shared/
    └── default/
        ├── statistics/
        │   └── PQR/
        │       └── PQR_p2019-12-31T23-00-00.000+00-00_p2025-05-31T22-00-00.000+00-00_v1.parquet
        └── XYZ/
            ├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v1.parquet
            ├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v2.parquet
            ├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v3.parquet
            └── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v4.parquet

None
</pre>

Note that we have both the original dataset and the copy, and the `/archives` part:

```python {.marimo}
print(tree(f'{data_path}/archives'))
```

<!-- @output:yOPj -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">archives/
├── Our Product/
│   └── statistics/
│       └── SampleDatasetForArchiving/
│           └── SampleDatasetForArchiving_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v1.parquet
├── statistics/
│   └── PQR/
│       └── PQR_p2019-12-31T23-00-00.000+00-00_p2025-05-31T22-00-00.000+00-00_v1.parquet
└── XYZ/
    └── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v1.parquet

</pre>

<!-- @output:yOPj -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">archives/
├── Our Product/
│   └── statistics/
│       └── SampleDatasetForArchiving/
│           └── SampleDatasetForArchiving_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v1.parquet
├── statistics/
│   └── PQR/
│       └── PQR_p2019-12-31T23-00-00.000+00-00_p2025-05-31T22-00-00.000+00-00_v1.parquet
└── XYZ/
    └── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v1.parquet

</pre>

(We also observe that the product contains spaces. That is a breach of convention, and we should expect it to stop working without notice.)
<!---->
Sharing
-------

We create some more data, and read it back before setting slightly different properties:

```python {.marimo}
# let us differentiate sharing
r = Dataset("XYZ")
r.sharing = ["s123", "s234"]
r.archive()
```

```python {.marimo}
s = Dataset("PQR")
s.process_stage = "statistics"
s.sharing = ["s234"]
s.archive()
```

```python {.marimo}
# each sharing key falls back to the destination configured as "default":
print(tree(f'{data_path}/shared'))
```

<!-- @output:SdmI -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">shared/
└── default/
    ├── statistics/
    │   └── PQR/
    │       └── PQR_p2019-12-31T23-00-00.000+00-00_p2025-05-31T22-00-00.000+00-00_v1.parquet
    └── XYZ/
        └── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v1.parquet

</pre>

Observing what happened in `archives/` we see what happens if the `product`, or both `product` and `process_stage` is empty - the tree is flattened.

```python {.marimo}
print(tree(f'{data_path}/archives'))
```

We `.archive()` again, a few times.

```python {.marimo}
r.archive()
r.archive()
r.archive()
```

... and see the version counter increase.

```python {.marimo}
print(tree(f'{data_path}/archives/XYZ'))
```

<!-- @output:jxvo -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">XYZ/
├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v1.parquet
├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v2.parquet
├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v3.parquet
└── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v4.parquet

</pre>

```python {.marimo}
print(treee())
```
