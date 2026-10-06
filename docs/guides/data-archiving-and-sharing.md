---
title: Data Archiving And Sharing
marimo-version: 0.24.2
---

Archiving and sharing
=====================

To comply with legal requirements, Statistics Norway commits itself to working according to a formal process model.
For the sake of transparency and process reviews, at certain points in the process data has to be persisted.
That does not simply mean the data must be saved.
Stricter requirements apply.
First, immutability: the persisted data must be stored "forever", without being subject to change.
Second, conventions apply to storage formats, naming and documentation.

Data shared between different statistics are subject to the same restrictions.

The conventions that apply are designed for archive and review purposes, not to for efficient data manipulation or retrievel.
That is contrary to the purpose of the SSB Timeseries library,
which is the reason "archiving" and "sharing" are treated differently from ordinary reads and writes.

Configurations at the set level control how a dataset is archived and shared, but the actual writing happens when data is persisted.
Since an archive is a persisted copy, the `.archive()` function takes care of both.
<!---->
## Setup

```python {.marimo}
data_path = CONFIG.repositories['tutorials']['directory']['options']['path']
def treee():
    print(tree(data_path))
treee()
```

<!-- @output:lEQa -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">timeseries/
├── archives/
│   ├── A Sample Dataset/
│   │   └── A Sample Dataset_v1.parquet
│   ├── PQR/
│   │   └── PQR_v1.parquet
│   ├── statistics/
│   │   └── PQR/
│   │       ├── PQR_p2019-12-31T23-00-00.000+00-00_p2025-05-31T22-00-00.000+00-00_v1.parquet
│   │       └── PQR_p2019-12-31T23-00-00.000+00-00_p2025-05-31T22-00-00.000+00-00_v2.parquet
│   ├── The Sample Statistic/
│   │   └── statistics/
│   │       └── SampleDataset/
│   │           └── SampleDataset_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v1.parquet
│   └── XYZ/
│       ├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v2.parquet
│       ├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v3.parquet
│       └── XYZ_v1.parquet
├── metadata/
│   ├── A Sample Dataset-metadata.json
│   ├── PQR-metadata.json
│   ├── SampleDataset-metadata.json
│   └── XYZ-metadata.json
├── NONE_AT/
│   ├── A Sample Dataset/
│   │   └── A Sample Dataset-latest-data.parquet
│   ├── PQR/
│   │   └── PQR-latest-data.parquet
│   ├── SampleDataset/
│   │   └── SampleDataset-latest-data.parquet
│   └── XYZ/
│       └── XYZ-latest-data.parquet
└── shared/
    └── default/
        ├── statistics/
        │   └── PQR/
        │       ├── PQR_p2019-12-31T23-00-00.000+00-00_p2025-05-31T22-00-00.000+00-00_v1.parquet
        │       └── PQR_p2019-12-31T23-00-00.000+00-00_p2025-05-31T22-00-00.000+00-00_v2.parquet
        └── XYZ/
            ├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v2.parquet
            └── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v3.parquet

</pre>

```python {.marimo}
# what is there before we start?
treee()
```

<!-- @output:PKri -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">timeseries/
├── archives/
│   ├── A Sample Dataset/
│   │   └── A Sample Dataset_v1.parquet
│   ├── PQR/
│   │   └── PQR_v1.parquet
│   ├── statistics/
│   │   └── PQR/
│   │       ├── PQR_p2019-12-31T23-00-00.000+00-00_p2025-05-31T22-00-00.000+00-00_v1.parquet
│   │       └── PQR_p2019-12-31T23-00-00.000+00-00_p2025-05-31T22-00-00.000+00-00_v2.parquet
│   ├── The Sample Statistic/
│   │   └── statistics/
│   │       └── SampleDataset/
│   │           └── SampleDataset_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v1.parquet
│   └── XYZ/
│       ├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v2.parquet
│       ├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v3.parquet
│       └── XYZ_v1.parquet
├── metadata/
│   ├── A Sample Dataset-metadata.json
│   ├── PQR-metadata.json
│   ├── SampleDataset-metadata.json
│   └── XYZ-metadata.json
├── NONE_AT/
│   ├── A Sample Dataset/
│   │   └── A Sample Dataset-latest-data.parquet
│   ├── PQR/
│   │   └── PQR-latest-data.parquet
│   ├── SampleDataset/
│   │   └── SampleDataset-latest-data.parquet
│   └── XYZ/
│       └── XYZ-latest-data.parquet
└── shared/
    └── default/
        ├── statistics/
        │   └── PQR/
        │       ├── PQR_p2019-12-31T23-00-00.000+00-00_p2025-05-31T22-00-00.000+00-00_v1.parquet
        │       └── PQR_p2019-12-31T23-00-00.000+00-00_p2025-05-31T22-00-00.000+00-00_v2.parquet
        └── XYZ/
            ├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v2.parquet
            └── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v3.parquet

</pre>

### Example: point-in-time data, *without* versioning

```python {.marimo}
from ssb_timeseries.sample_data import xyz_at
```

```python {.marimo}
import ssb_timeseries as ts
```

```python {.marimo}
# the ssb archive convention allows no spaces in a name:
set_name = 'SampleDataset'
```

```python {.marimo}
p = ts.dataset.Dataset(
    name = set_name,
    data_type = ts.types.SeriesType('NONE','AT'),
    data = xyz_at(),
).save()
```

```python {.marimo}
# what is there after the .save():
treee()
```

<!-- @output:emfo -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">timeseries/
├── archives/
│   ├── A Sample Dataset/
│   │   └── A Sample Dataset_v1.parquet
│   ├── PQR/
│   │   └── PQR_v1.parquet
│   ├── statistics/
│   │   └── PQR/
│   │       ├── PQR_p2019-12-31T23-00-00.000+00-00_p2025-05-31T22-00-00.000+00-00_v1.parquet
│   │       └── PQR_p2019-12-31T23-00-00.000+00-00_p2025-05-31T22-00-00.000+00-00_v2.parquet
│   ├── The Sample Statistic/
│   │   └── statistics/
│   │       └── SampleDataset/
│   │           └── SampleDataset_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v1.parquet
│   └── XYZ/
│       ├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v2.parquet
│       ├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v3.parquet
│       └── XYZ_v1.parquet
├── metadata/
│   ├── A Sample Dataset-metadata.json
│   ├── PQR-metadata.json
│   ├── SampleDataset-metadata.json
│   └── XYZ-metadata.json
├── NONE_AT/
│   ├── A Sample Dataset/
│   │   └── A Sample Dataset-latest-data.parquet
│   ├── PQR/
│   │   └── PQR-latest-data.parquet
│   ├── SampleDataset/
│   │   └── SampleDataset-latest-data.parquet
│   └── XYZ/
│       └── XYZ-latest-data.parquet
└── shared/
    └── default/
        ├── statistics/
        │   └── PQR/
        │       ├── PQR_p2019-12-31T23-00-00.000+00-00_p2025-05-31T22-00-00.000+00-00_v1.parquet
        │       └── PQR_p2019-12-31T23-00-00.000+00-00_p2025-05-31T22-00-00.000+00-00_v2.parquet
        └── XYZ/
            ├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v2.parquet
            └── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v3.parquet

</pre>

```python {.marimo}
#read the data back, just because we can
q = ts.dataset.Dataset(set_name)
```

## Archiving
<!---->
Archiving writes to the archive and, for each sharing key, to the
destination that key names.
The archive path is built from the tags, so `product` and `process_stage` are set
before the dataset is archived.

```python {.marimo}
statistics_product = 'The Sample Statistic'
q.process_stage = 'statistics'
q.product = statistics_product
```

```python {.marimo}
q.archive()
```

```python {.marimo}
print(tree(f'{data_path}/archives'))
```

<!-- @output:qnkX -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">archives/
├── A Sample Dataset/
│   └── A Sample Dataset_v1.parquet
├── PQR/
│   └── PQR_v1.parquet
├── statistics/
│   └── PQR/
│       ├── PQR_p2019-12-31T23-00-00.000+00-00_p2025-05-31T22-00-00.000+00-00_v1.parquet
│       └── PQR_p2019-12-31T23-00-00.000+00-00_p2025-05-31T22-00-00.000+00-00_v2.parquet
├── The Sample Statistic/
│   └── statistics/
│       └── SampleDataset/
│           ├── SampleDataset_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v1.parquet
│           └── SampleDataset_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v2.parquet
└── XYZ/
    ├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v2.parquet
    ├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v3.parquet
    └── XYZ_v1.parquet

</pre>

## Sharing

Archiving and sharing apply to any dataset, not only the one created above.
The two datasets used by the other guides are created here so that this guide runs on its own.

```python {.marimo}
from ssb_timeseries.sample_data import create_df

# Archiving and sharing apply to any dataset, not only the one created above.
# The datasets used by the other guides are created here, from the same
# generators, so that this guide runs on its own and writes identical data.
for name, data in (
    ("XYZ", xyz_at()),
    (
        "PQR",
        create_df(
            ["p", "q", "r"],
            start_date="2020-01-01",
            end_date="2025-06-01",
            freq="D",
            temporality="AT",
        ),
    ),
):
    ts.dataset.Dataset(
        name = name,
        data_type = ts.types.SeriesType('NONE','AT'),
        data = data,
    ).save()
```

```python {.marimo}
# let us differentiate sharing
r = ts.dataset.Dataset("XYZ")
r.sharing = ["s123", "s234"]
r.archive()
```

```python {.marimo}
s = ts.dataset.Dataset("PQR")
s.process_stage = "statistics"
s.sharing = ["s234"]
s.archive()
```

```python {.marimo}
# each sharing key falls back to the destination configured as "default":
print(tree(f'{data_path}/shared'))
```

<!-- @output:ecfG -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">shared/
└── default/
    ├── statistics/
    │   └── PQR/
    │       ├── PQR_p2019-12-31T23-00-00.000+00-00_p2025-05-31T22-00-00.000+00-00_v1.parquet
    │       ├── PQR_p2019-12-31T23-00-00.000+00-00_p2025-05-31T22-00-00.000+00-00_v2.parquet
    │       └── PQR_p2019-12-31T23-00-00.000+00-00_p2025-05-31T22-00-00.000+00-00_v3.parquet
    └── XYZ/
        ├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v2.parquet
        ├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v3.parquet
        └── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v4.parquet

</pre>
