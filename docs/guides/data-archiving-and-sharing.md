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

Configurations at the set level controls how a dataset is shared, but the actual sharing happens when data is persisted.
Since shared data must be persisted, the `.snapshot()` function takes care of both.
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
│   └── XYZ/
│       └── XYZ_v1.parquet
├── AS_OF_AT/
│   ├── POPU06/
│   │   ├── POPU06-as_of_2023-12-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-01-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-02-29T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-03-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-04-30T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-05-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-06-30T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-07-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-08-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-09-30T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-10-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-11-30T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-12-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-01-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-02-28T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-03-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-04-30T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-05-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-06-30T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-07-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-08-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-09-30T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-10-31T230000+0000-data.parquet
│   │   └── POPU06-as_of_2025-11-30T230000+0000-data.parquet
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
│   ├── AZ_drikkevarer-metadata.json
│   ├── AZ_drinks-metadata.json
│   ├── AZ_omsetning-metadata.json
│   ├── More Prices and Volumes-metadata.json
│   ├── POPU06-metadata.json
│   ├── PQR-metadata.json
│   ├── Prices and Volumes-metadata.json
│   └── XYZ-metadata.json
├── NONE_AT/
│   ├── A Sample Dataset/
│   │   └── A Sample Dataset-latest-data.parquet
│   ├── PQR/
│   │   └── PQR-latest-data.parquet
│   └── XYZ/
│       └── XYZ-latest-data.parquet
└── NONE_FROM_TO/
    ├── AZ_drikkevarer/
    │   └── AZ_drikkevarer-latest-data.parquet
    ├── AZ_drinks/
    │   └── AZ_drinks-latest-data.parquet
    ├── AZ_omsetning/
    │   └── AZ_omsetning-latest-data.parquet
    └── More Prices and Volumes/
        └── More Prices and Volumes-latest-data.parquet

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
│   └── XYZ/
│       └── XYZ_v1.parquet
├── AS_OF_AT/
│   ├── POPU06/
│   │   ├── POPU06-as_of_2023-12-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-01-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-02-29T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-03-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-04-30T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-05-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-06-30T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-07-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-08-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-09-30T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-10-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-11-30T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-12-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-01-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-02-28T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-03-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-04-30T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-05-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-06-30T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-07-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-08-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-09-30T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-10-31T230000+0000-data.parquet
│   │   └── POPU06-as_of_2025-11-30T230000+0000-data.parquet
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
│   ├── AZ_drikkevarer-metadata.json
│   ├── AZ_drinks-metadata.json
│   ├── AZ_omsetning-metadata.json
│   ├── More Prices and Volumes-metadata.json
│   ├── POPU06-metadata.json
│   ├── PQR-metadata.json
│   ├── Prices and Volumes-metadata.json
│   └── XYZ-metadata.json
├── NONE_AT/
│   ├── A Sample Dataset/
│   │   └── A Sample Dataset-latest-data.parquet
│   ├── PQR/
│   │   └── PQR-latest-data.parquet
│   └── XYZ/
│       └── XYZ-latest-data.parquet
└── NONE_FROM_TO/
    ├── AZ_drikkevarer/
    │   └── AZ_drikkevarer-latest-data.parquet
    ├── AZ_drinks/
    │   └── AZ_drinks-latest-data.parquet
    ├── AZ_omsetning/
    │   └── AZ_omsetning-latest-data.parquet
    └── More Prices and Volumes/
        └── More Prices and Volumes-latest-data.parquet

</pre>

### Eksempel: momentane data, *uten* versjonering

```python {.marimo}
from ssb_timeseries.sample_data import xyz_at
from ssb_timeseries.types import SeriesType
from ssb_timeseries.dataset import SeriesType
```

```python {.marimo}
import ssb_timeseries as ts
```

```python {.marimo}
set_name = 'A Sample Dataset'
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
│   └── XYZ/
│       └── XYZ_v1.parquet
├── AS_OF_AT/
│   ├── POPU06/
│   │   ├── POPU06-as_of_2023-12-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-01-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-02-29T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-03-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-04-30T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-05-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-06-30T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-07-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-08-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-09-30T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-10-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-11-30T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-12-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-01-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-02-28T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-03-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-04-30T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-05-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-06-30T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-07-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-08-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-09-30T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-10-31T230000+0000-data.parquet
│   │   └── POPU06-as_of_2025-11-30T230000+0000-data.parquet
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
│   ├── AZ_drikkevarer-metadata.json
│   ├── AZ_drinks-metadata.json
│   ├── AZ_omsetning-metadata.json
│   ├── More Prices and Volumes-metadata.json
│   ├── POPU06-metadata.json
│   ├── PQR-metadata.json
│   ├── Prices and Volumes-metadata.json
│   └── XYZ-metadata.json
├── NONE_AT/
│   ├── A Sample Dataset/
│   │   └── A Sample Dataset-latest-data.parquet
│   ├── PQR/
│   │   └── PQR-latest-data.parquet
│   └── XYZ/
│       └── XYZ-latest-data.parquet
└── NONE_FROM_TO/
    ├── AZ_drikkevarer/
    │   └── AZ_drikkevarer-latest-data.parquet
    ├── AZ_drinks/
    │   └── AZ_drinks-latest-data.parquet
    ├── AZ_omsetning/
    │   └── AZ_omsetning-latest-data.parquet
    └── More Prices and Volumes/
        └── More Prices and Volumes-latest-data.parquet

</pre>

```python {.marimo}
#read the data back, just because we can
q = ts.dataset.Dataset(set_name)
```

...

```python {.marimo}
statistics_product = 'The Sample Statistic'
q.process_stage = 'statistikk'
q.product = statistics_product
```

```python {.marimo}
#q.snapshot()
```

```python {.marimo}
CONFIG.configuration_file= '/home/bernhard/code/ssb-timeseries/notebooks/sharing_config.json'
CONFIG.activate()
CONFIG.refresh()
#CONFIG.__dict__
```

<!-- @output:ROlb -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">&lt;ssb_timeseries.config.Config object at 0x7f90b5f10190&gt;</pre>

```python {.marimo}
q.snapshot()
```

<!-- @output:qnkX -->

<pre class="stderr" style="white-space: pre-wrap; overflow-wrap: break-word;">/tmp/marimo_728805/__marimo__cell_qnkX_.py:1: DeprecationWarning: Dataset.snapshot is deprecated and will be removed in a future version. Use Dataset.archive instead.
  q.snapshot()
</pre>

```python {.marimo}
print(tree(data_path))
```

<!-- @output:ZBYS -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">timeseries/
├── archives/
│   ├── A Sample Dataset/
│   │   ├── A Sample Dataset_v1.parquet
│   │   └── A Sample Dataset_v2.parquet
│   ├── PQR/
│   │   ├── PQR_v1.parquet
│   │   └── PQR_v2.parquet
│   └── XYZ/
│       ├── XYZ_v1.parquet
│       └── XYZ_v2.parquet
├── AS_OF_AT/
│   ├── POPU06/
│   │   ├── POPU06-as_of_2023-12-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-01-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-02-29T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-03-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-04-30T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-05-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-06-30T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-07-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-08-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-09-30T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-10-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-11-30T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-12-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-01-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-02-28T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-03-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-04-30T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-05-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-06-30T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-07-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-08-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-09-30T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-10-31T230000+0000-data.parquet
│   │   └── POPU06-as_of_2025-11-30T230000+0000-data.parquet
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
│   ├── AZ_drikkevarer-metadata.json
│   ├── AZ_drinks-metadata.json
│   ├── AZ_omsetning-metadata.json
│   ├── More Prices and Volumes-metadata.json
│   ├── POPU06-metadata.json
│   ├── PQR-metadata.json
│   ├── Prices and Volumes-metadata.json
│   └── XYZ-metadata.json
├── NONE_AT/
│   ├── A Sample Dataset/
│   │   └── A Sample Dataset-latest-data.parquet
│   ├── PQR/
│   │   └── PQR-latest-data.parquet
│   └── XYZ/
│       └── XYZ-latest-data.parquet
└── NONE_FROM_TO/
    ├── AZ_drikkevarer/
    │   └── AZ_drikkevarer-latest-data.parquet
    ├── AZ_drinks/
    │   └── AZ_drinks-latest-data.parquet
    ├── AZ_omsetning/
    │   └── AZ_omsetning-latest-data.parquet
    └── More Prices and Volumes/
        └── More Prices and Volumes-latest-data.parquet

</pre>

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
r.snapshot()
```

<!-- @output:ulZA -->

<pre class="stderr" style="white-space: pre-wrap; overflow-wrap: break-word;">/tmp/marimo_728805/__marimo__cell_ulZA_.py:4: DeprecationWarning: Dataset.snapshot is deprecated and will be removed in a future version. Use Dataset.archive instead.
  r.snapshot()
</pre>

```python {.marimo}
s = ts.dataset.Dataset("PQR")
s.process_stage = "statistikk"
s.sharing = ["s234"]
s.snapshot()
```

<!-- @output:ecfG -->

<pre class="stderr" style="white-space: pre-wrap; overflow-wrap: break-word;">/tmp/marimo_728805/__marimo__cell_ecfG_.py:4: DeprecationWarning: Dataset.snapshot is deprecated and will be removed in a future version. Use Dataset.archive instead.
  s.snapshot()
</pre>

```python {.marimo}
treee()
```

<!-- @output:Pvdt -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">timeseries/
├── archives/
│   ├── A Sample Dataset/
│   │   ├── A Sample Dataset_v1.parquet
│   │   └── A Sample Dataset_v2.parquet
│   ├── PQR/
│   │   ├── PQR_v1.parquet
│   │   └── PQR_v2.parquet
│   └── XYZ/
│       ├── XYZ_v1.parquet
│       └── XYZ_v2.parquet
├── AS_OF_AT/
│   ├── POPU06/
│   │   ├── POPU06-as_of_2023-12-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-01-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-02-29T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-03-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-04-30T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-05-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-06-30T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-07-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-08-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-09-30T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-10-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-11-30T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-12-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-01-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-02-28T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-03-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-04-30T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-05-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-06-30T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-07-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-08-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-09-30T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-10-31T230000+0000-data.parquet
│   │   └── POPU06-as_of_2025-11-30T230000+0000-data.parquet
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
│   ├── AZ_drikkevarer-metadata.json
│   ├── AZ_drinks-metadata.json
│   ├── AZ_omsetning-metadata.json
│   ├── More Prices and Volumes-metadata.json
│   ├── POPU06-metadata.json
│   ├── PQR-metadata.json
│   ├── Prices and Volumes-metadata.json
│   └── XYZ-metadata.json
├── NONE_AT/
│   ├── A Sample Dataset/
│   │   └── A Sample Dataset-latest-data.parquet
│   ├── PQR/
│   │   └── PQR-latest-data.parquet
│   └── XYZ/
│       └── XYZ-latest-data.parquet
└── NONE_FROM_TO/
    ├── AZ_drikkevarer/
    │   └── AZ_drikkevarer-latest-data.parquet
    ├── AZ_drinks/
    │   └── AZ_drinks-latest-data.parquet
    ├── AZ_omsetning/
    │   └── AZ_omsetning-latest-data.parquet
    └── More Prices and Volumes/
        └── More Prices and Volumes-latest-data.parquet

</pre>

<!-- @output:ZBYS -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">timeseries/
├── archives/
│   ├── A Sample Dataset/
│   │   ├── A Sample Dataset_v1.parquet
│   │   └── A Sample Dataset_v2.parquet
│   ├── PQR/
│   │   ├── PQR_v1.parquet
│   │   └── PQR_v2.parquet
│   └── XYZ/
│       ├── XYZ_v1.parquet
│       └── XYZ_v2.parquet
├── AS_OF_AT/
│   ├── POPU06/
│   │   ├── POPU06-as_of_2023-12-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-01-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-02-29T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-03-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-04-30T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-05-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-06-30T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-07-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-08-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-09-30T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-10-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-11-30T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-12-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-01-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-02-28T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-03-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-04-30T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-05-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-06-30T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-07-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-08-31T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-09-30T220000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-10-31T230000+0000-data.parquet
│   │   └── POPU06-as_of_2025-11-30T230000+0000-data.parquet
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
│   ├── AZ_drikkevarer-metadata.json
│   ├── AZ_drinks-metadata.json
│   ├── AZ_omsetning-metadata.json
│   ├── More Prices and Volumes-metadata.json
│   ├── POPU06-metadata.json
│   ├── PQR-metadata.json
│   ├── Prices and Volumes-metadata.json
│   └── XYZ-metadata.json
├── NONE_AT/
│   ├── A Sample Dataset/
│   │   └── A Sample Dataset-latest-data.parquet
│   ├── PQR/
│   │   └── PQR-latest-data.parquet
│   └── XYZ/
│       └── XYZ-latest-data.parquet
└── NONE_FROM_TO/
    ├── AZ_drikkevarer/
    │   └── AZ_drikkevarer-latest-data.parquet
    ├── AZ_drinks/
    │   └── AZ_drinks-latest-data.parquet
    ├── AZ_omsetning/
    │   └── AZ_omsetning-latest-data.parquet
    └── More Prices and Volumes/
        └── More Prices and Volumes-latest-data.parquet

</pre>
