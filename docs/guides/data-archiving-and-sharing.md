---
title: Data Archiving And Sharing
marimo-version: 0.24.0
---

Archiving and sharing
---------------------

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
├── AS_OF_AT/
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
│   ├── PQR-metadata.json
│   ├── Prices and Volumes-metadata.json
│   ├── Sample Data-metadata.json
│   └── XYZ-metadata.json
├── NONE_AT/
│   ├── A Sample Dataset/
│   │   └── A Sample Dataset-latest-data.parquet
│   ├── PQR/
│   │   └── PQR-latest-data.parquet
│   ├── Sample Data/
│   │   └── Sample Data-latest-data.parquet
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
├── AS_OF_AT/
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
│   ├── PQR-metadata.json
│   ├── Prices and Volumes-metadata.json
│   ├── Sample Data-metadata.json
│   └── XYZ-metadata.json
├── NONE_AT/
│   ├── A Sample Dataset/
│   │   └── A Sample Dataset-latest-data.parquet
│   ├── PQR/
│   │   └── PQR-latest-data.parquet
│   ├── Sample Data/
│   │   └── Sample Data-latest-data.parquet
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
├── AS_OF_AT/
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
│   ├── PQR-metadata.json
│   ├── Prices and Volumes-metadata.json
│   ├── Sample Data-metadata.json
│   └── XYZ-metadata.json
├── NONE_AT/
│   ├── A Sample Dataset/
│   │   └── A Sample Dataset-latest-data.parquet
│   ├── PQR/
│   │   └── PQR-latest-data.parquet
│   ├── Sample Data/
│   │   └── Sample Data-latest-data.parquet
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

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">&lt;ssb_timeseries.config.Config object at 0x7f4050da4690&gt;</pre>

```python {.marimo}
q.snapshot()
```

<!-- @output:qnkX -->

<pre class="stderr" style="white-space: pre-wrap; overflow-wrap: break-word;">Traceback (most recent call last):
  File &quot;/home/bernhard/code/ssb-timeseries/.nox/docs/tmp/marimo_141584/__marimo__cell_qnkX_.py&quot;, line 1, in
    q.snapshot()
    ~~~~~~~~~~^^
  File &quot;/home/bernhard/code/ssb-timeseries/src/ssb_timeseries/dataset.py&quot;, line 467, in snapshot
    io.persist(self)  # is &#x27;archive&#x27; a better name than &#x27;persist&#x27; or &#x27;snapshot&#x27;?
    ~~~~~~~~~~^^^^^^
  File &quot;/home/bernhard/code/ssb-timeseries/src/ssb_timeseries/io/__init__.py&quot;, line 437, in persist
    date_from = ds.data&#91;ds.datetime_columns&#93;.min().min()
                ~~~~~~~^^^^^^^^^^^^^^^^^^^^^
  File &quot;pyarrow/table.pxi&quot;, line 1714, in pyarrow.lib._Tabular.__getitem__
  File &quot;pyarrow/table.pxi&quot;, line 1799, in pyarrow.lib._Tabular.column
    return self._column(self._ensure_integer_index(i))
  File &quot;pyarrow/table.pxi&quot;, line 1745, in pyarrow.lib._Tabular._ensure_integer_index
    raise TypeError(&quot;Index must either be string or integer&quot;)
TypeError: Index must either be string or integer

</pre>

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">exception: Index must either be string or integer</pre>

```python {.marimo}
print(tree(data_path))
```

<!-- @output:ecfG -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">timeseries/
├── AS_OF_AT/
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
│   ├── PQR-metadata.json
│   ├── Prices and Volumes-metadata.json
│   ├── Sample Data-metadata.json
│   └── XYZ-metadata.json
├── NONE_AT/
│   ├── A Sample Dataset/
│   │   └── A Sample Dataset-latest-data.parquet
│   ├── PQR/
│   │   └── PQR-latest-data.parquet
│   ├── Sample Data/
│   │   └── Sample Data-latest-data.parquet
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
# let us differentiate sharing
r = ts.dataset.Dataset("XYZ")
r.sharing = [{'team': 's123', 'path': f'/home/bernhard/timeseries/{statistics_product}/shared/s123/'}, {'team': 's234', 'path': f'/home/bernhard/timeseries/{statistics_product}/shared/s234/'}]
r.snapshot()
treee()
```

<!-- @output:Vxnm -->

<pre class="stderr" style="white-space: pre-wrap; overflow-wrap: break-word;">Traceback (most recent call last):
  File &quot;/home/bernhard/code/ssb-timeseries/.nox/docs/tmp/marimo_141584/__marimo__cell_Vxnm_.py&quot;, line 4, in
    r.snapshot()
    ~~~~~~~~~~^^
  File &quot;/home/bernhard/code/ssb-timeseries/src/ssb_timeseries/dataset.py&quot;, line 467, in snapshot
    io.persist(self)  # is &#x27;archive&#x27; a better name than &#x27;persist&#x27; or &#x27;snapshot&#x27;?
    ~~~~~~~~~~^^^^^^
  File &quot;/home/bernhard/code/ssb-timeseries/src/ssb_timeseries/io/__init__.py&quot;, line 437, in persist
    date_from = ds.data&#91;ds.datetime_columns&#93;.min().min()
                ~~~~~~~^^^^^^^^^^^^^^^^^^^^^
  File &quot;pyarrow/table.pxi&quot;, line 1714, in pyarrow.lib._Tabular.__getitem__
    return self.column(key)
  File &quot;pyarrow/table.pxi&quot;, line 1799, in pyarrow.lib._Tabular.column
    return self._column(self._ensure_integer_index(i))
  File &quot;pyarrow/table.pxi&quot;, line 1745, in pyarrow.lib._Tabular._ensure_integer_index
    raise TypeError(&quot;Index must either be string or integer&quot;)
TypeError: Index must either be string or integer

</pre>

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">exception: Index must either be string or integer</pre>

```python {.marimo}
s = ts.dataset.Dataset("PQR")
s.process_stage = 'statistikk'
s.sharing = [
    {"team": "s234", "path": f'/home/bernhard/timeseries/{statistics_product}/shared/s234/'},
]
s.snapshot()
```

<!-- @output:DnEU -->

<pre class="stderr" style="white-space: pre-wrap; overflow-wrap: break-word;">Traceback (most recent call last):
  File &quot;/home/bernhard/code/ssb-timeseries/.nox/docs/tmp/marimo_141584/__marimo__cell_DnEU_.py&quot;, line 6, in
    s.snapshot()
    ~~~~~~~~~~^^
  File &quot;/home/bernhard/code/ssb-timeseries/src/ssb_timeseries/dataset.py&quot;, line 467, in snapshot
    io.persist(self)  # is &#x27;archive&#x27; a better name than &#x27;persist&#x27; or &#x27;snapshot&#x27;?
    ~~~~~~~~~~^^^^^^
  File &quot;/home/bernhard/code/ssb-timeseries/src/ssb_timeseries/io/__init__.py&quot;, line 437, in persist
    date_from = ds.data&#91;ds.datetime_columns&#93;.min().min()
                ~~~~~~~^^^^^^^^^^^^^^^^^^^^^
  File &quot;pyarrow/table.pxi&quot;, line 1714, in pyarrow.lib._Tabular.__getitem__
    return self.column(key)
  File &quot;pyarrow/table.pxi&quot;, line 1799, in pyarrow.lib._Tabular.column
    return self._column(self._ensure_integer_index(i))
  File &quot;pyarrow/table.pxi&quot;, line 1745, in pyarrow.lib._Tabular._ensure_integer_index
    raise TypeError(&quot;Index must either be string or integer&quot;)
TypeError: Index must either be string or integer

</pre>

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">exception: Index must either be string or integer</pre>

```python {.marimo}
treee()
```

<!-- @output:ulZA -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">timeseries/
├── AS_OF_AT/
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
│   ├── PQR-metadata.json
│   ├── Prices and Volumes-metadata.json
│   ├── Sample Data-metadata.json
│   └── XYZ-metadata.json
├── NONE_AT/
│   ├── A Sample Dataset/
│   │   └── A Sample Dataset-latest-data.parquet
│   ├── PQR/
│   │   └── PQR-latest-data.parquet
│   ├── Sample Data/
│   │   └── Sample Data-latest-data.parquet
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

<!-- @output:ecfG -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">timeseries/
├── AS_OF_AT/
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
│   ├── PQR-metadata.json
│   ├── Prices and Volumes-metadata.json
│   ├── Sample Data-metadata.json
│   └── XYZ-metadata.json
├── NONE_AT/
│   ├── A Sample Dataset/
│   │   └── A Sample Dataset-latest-data.parquet
│   ├── PQR/
│   │   └── PQR-latest-data.parquet
│   ├── Sample Data/
│   │   └── Sample Data-latest-data.parquet
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
