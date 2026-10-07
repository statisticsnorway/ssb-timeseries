---
title: Data Types And Storage
marimo-version: 0.24.2
---

```python {.marimo}
import marimo as mo
```

```python {.marimo}
from filetree import tree
from ssb_timeseries import get_configuration
CONFIG = get_configuration()
```

# Data types and storage

## Setup

```python {.marimo}
# both data and metadata will be stored here
data_path = CONFIG.repositories['tutorials']['directory']['options']['path']
print(data_path)
```

<!-- @output:bkHC -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">/home/bernhard/timeseries
</pre>

```python {.marimo}
# what is there before we start?
print(tree(data_path))
```

<!-- @output:lEQa -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">timeseries/
├── archives/
│   ├── statistics/
│   │   └── PQR/
│   │       ├── PQR_p2019-12-31T23-00-00.000+00-00_p2025-08-14T22-00-00.000+00-00_v1.parquet
│   │       └── PQR_p2019-12-31T23-00-00.000+00-00_p2025-08-14T22-00-00.000+00-00_v2.parquet
│   ├── The Sample Statistic/
│   │   └── statistics/
│   │       └── SampleDataset/
│   │           ├── SampleDataset_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v1.parquet
│   │           └── SampleDataset_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v2.parquet
│   └── XYZ/
│       ├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v2.parquet
│       └── XYZ_p2025-07-30T22-00-00.000+00-00_p2025-08-05T22-00-00.000+00-00_v2025-08-06T22-00-00.000+00-00_v_v1.parquet
├── AS_OF_AT/
│   └── POPU06/
│       ├── POPU06-as_of_2023-12-31T230000+0000-data.parquet
│       ├── POPU06-as_of_2024-01-31T230000+0000-data.parquet
│       ├── POPU06-as_of_2025-10-31T230000+0000-data.parquet
│       ├── POPU06-as_of_2025-11-30T230000+0000-data.parquet
│       └── ...
├── AS_OF_FROM_TO/
│   ├── BNO/
│   │   ├── BNO-as_of_2025-05-31T220000+0000-data.parquet
│   │   └── BNO-as_of_2025-08-06T220000+0000-data.parquet
│   └── Prices and Volumes/
│       ├── Prices and Volumes-as_of_2023-12-31T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2024-01-31T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2025-10-31T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2025-11-30T230000+0000-data.parquet
│       └── ...
├── metadata/
│   ├── AZ_beverages-metadata.json
│   ├── AZ_drinks-metadata.json
│   ├── SampleDataset-metadata.json
│   ├── XYZ-metadata.json
│   └── ...
├── NONE_AT/
│   ├── PQR/
│   │   └── PQR-latest-data.parquet
│   ├── SampleDataset/
│   │   └── SampleDataset-latest-data.parquet
│   └── XYZ/
│       └── XYZ-latest-data.parquet
├── NONE_FROM_TO/
│   ├── AZ_beverages/
│   │   └── AZ_beverages-latest-data.parquet
│   ├── AZ_drinks/
│   │   └── AZ_drinks-latest-data.parquet
│   └── More Prices and Volumes/
│       └── More Prices and Volumes-latest-data.parquet
└── shared/
    └── default/
        ├── statistics/
        │   └── PQR/
        │       ├── PQR_p2019-12-31T23-00-00.000+00-00_p2025-08-14T22-00-00.000+00-00_v1.parquet
        │       └── PQR_p2019-12-31T23-00-00.000+00-00_p2025-08-14T22-00-00.000+00-00_v2.parquet
        └── XYZ/
            ├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v2.parquet
            └── XYZ_p2025-07-30T22-00-00.000+00-00_p2025-08-05T22-00-00.000+00-00_v2025-08-06T22-00-00.000+00-00_v_v1.parquet

</pre>

```python {.marimo}
from ssb_timeseries.dataset import Dataset
from ssb_timeseries.types import SeriesType, Versioning, Temporality
```

```python {.marimo}
from datetime import timedelta

from ssb_timeseries.sample_data import create_df
from ssb_timeseries.dates import ensure_datetime, date_utc
```

```python {.marimo}
import polars as pl
from datetime import datetime
```

## Saving data
<!---->
### Example: point-in-time data, *without* versioning

```python {.marimo}
point_in_time_data = SeriesType('NONE', 'AT')
```

```python {.marimo}
def some_simple_data_from_file_or_query(start='2020-01-01', end='2025-06-01'):
    return create_df(['p','q','r'], start_date=start,end_date=end, freq='D')
```

```python {.marimo}
pqr_df = some_simple_data_from_file_or_query()
print(type(pqr_df))
pqr_df
```

<!-- @output:Hstk -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">&lt;class &#x27;pandas.core.frame.DataFrame&#x27;&gt;
</pre>

| valid_at | p | q | r |
| --- | --- | --- | --- |
| 2020-01-01 00:00:00+01:00 | 110.0 | 100.0 | 110.0 |
| 2020-01-02 00:00:00+01:00 | 90.0 | 90.0 | 100.0 |
| 2020-01-03 00:00:00+01:00 | 90.0 | 90.0 | 80.0 |
| 2020-01-04 00:00:00+01:00 | 80.0 | 100.0 | 120.0 |
| 2020-01-05 00:00:00+01:00 | 110.0 | 100.0 | 100.0 |
| ... | ... | ... | ... |
| 2025-05-28 00:00:00+02:00 | 90.0 | 120.0 | 90.0 |
| 2025-05-29 00:00:00+02:00 | 110.0 | 100.0 | 80.0 |
| 2025-05-30 00:00:00+02:00 | 100.0 | 120.0 | 110.0 |
| 2025-05-31 00:00:00+02:00 | 100.0 | 100.0 | 100.0 |
| 2025-06-01 00:00:00+02:00 | 100.0 | 100.0 | 90.0 |

```python {.marimo}
pqr = Dataset(
    name = 'PQR',
    data_type = point_in_time_data,
    data = pqr_df,
)
```

```python {.marimo}
type(pqr)
```

<!-- @output:iLit -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">&lt;class &#x27;ssb_timeseries.dataset.Dataset&#x27;&gt;</pre>

```python {.marimo}
pqr.data
```

<!-- @output:ZHCJ -->

| valid_at | p | q | r |
| --- | --- | --- | --- |
| 2019-12-31 23:00:00+00:00 | 110.0 | 100.0 | 110.0 |
| 2020-01-01 23:00:00+00:00 | 90.0 | 90.0 | 100.0 |
| 2020-01-02 23:00:00+00:00 | 90.0 | 90.0 | 80.0 |
| 2020-01-03 23:00:00+00:00 | 80.0 | 100.0 | 120.0 |
| 2020-01-04 23:00:00+00:00 | 110.0 | 100.0 | 100.0 |
| ... | ... | ... | ... |
| 2025-05-27 22:00:00+00:00 | 90.0 | 120.0 | 90.0 |
| 2025-05-28 22:00:00+00:00 | 110.0 | 100.0 | 80.0 |
| 2025-05-29 22:00:00+00:00 | 100.0 | 120.0 | 110.0 |
| 2025-05-30 22:00:00+00:00 | 100.0 | 100.0 | 100.0 |
| 2025-05-31 22:00:00+00:00 | 100.0 | 100.0 | 90.0 |

```python {.marimo}
pqr.tags
```

<!-- @output:ROlb -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">{&#x27;name&#x27;: &#x27;PQR&#x27;,
 &#x27;product group&#x27;: &#91;&#x27;essential&#x27;, &#x27;essentials&#x27;&#93;,
 &#x27;repository&#x27;: &#x27;tutorials&#x27;,
 &#x27;series&#x27;: {&#x27;p&#x27;: {&#x27;dataset&#x27;: &#x27;PQR&#x27;,
                  &#x27;name&#x27;: &#x27;p&#x27;,
                  &#x27;product&#x27;: &#x27;coffee&#x27;,
                  &#x27;product group&#x27;: &#91;&#x27;essential&#x27;, &#x27;essentials&#x27;&#93;,
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
                  &#x27;product group&#x27;: &#91;&#x27;essential&#x27;, &#x27;essentials&#x27;&#93;,
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
                  &#x27;product group&#x27;: &#91;&#x27;essential&#x27;, &#x27;essentials&#x27;&#93;,
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

```python {.marimo}
pqr.tag_dataset(tags={'variable': 'price','product group': 'essentials'})

pqr.tag_series('p',tags={'product': 'coffee'})
pqr.tag_series('q',tags={'product': 'crispbread'})
pqr.tag_series('r',tags={'product': 'brown cheese'})

pqr.tags
```

<!-- @output:qnkX -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">{&#x27;name&#x27;: &#x27;PQR&#x27;,
 &#x27;product group&#x27;: &#91;&#x27;essential&#x27;, &#x27;essentials&#x27;&#93;,
 &#x27;repository&#x27;: &#x27;tutorials&#x27;,
 &#x27;series&#x27;: {&#x27;p&#x27;: {&#x27;dataset&#x27;: &#x27;PQR&#x27;,
                  &#x27;name&#x27;: &#x27;p&#x27;,
                  &#x27;product&#x27;: &#x27;coffee&#x27;,
                  &#x27;product group&#x27;: &#91;&#x27;essential&#x27;, &#x27;essentials&#x27;&#93;,
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
                  &#x27;product group&#x27;: &#91;&#x27;essential&#x27;, &#x27;essentials&#x27;&#93;,
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
                  &#x27;product group&#x27;: &#91;&#x27;essential&#x27;, &#x27;essentials&#x27;&#93;,
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

```python {.marimo}
pqr.save()
```

```python {.marimo}
print(tree(data_path))
```

<!-- @output:YWSi -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">timeseries/
├── archives/
│   ├── statistics/
│   │   └── PQR/
│   │       ├── PQR_p2019-12-31T23-00-00.000+00-00_p2025-08-14T22-00-00.000+00-00_v1.parquet
│   │       └── PQR_p2019-12-31T23-00-00.000+00-00_p2025-08-14T22-00-00.000+00-00_v2.parquet
│   ├── The Sample Statistic/
│   │   └── statistics/
│   │       └── SampleDataset/
│   │           ├── SampleDataset_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v1.parquet
│   │           └── SampleDataset_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v2.parquet
│   └── XYZ/
│       ├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v2.parquet
│       └── XYZ_p2025-07-30T22-00-00.000+00-00_p2025-08-05T22-00-00.000+00-00_v2025-08-06T22-00-00.000+00-00_v_v1.parquet
├── AS_OF_AT/
│   ├── POPU06/
│   │   ├── POPU06-as_of_2023-12-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-01-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-10-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-11-30T230000+0000-data.parquet
│   │   └── ...
│   └── XYZ/
│       ├── XYZ-as_of_2025-04-30T220000+0000-data.parquet
│       ├── XYZ-as_of_2025-05-31T220000+0000-data.parquet
│       ├── XYZ-as_of_2025-08-05T220000+0000-data.parquet
│       ├── XYZ-as_of_2025-08-06T220000+0000-data.parquet
│       └── ...
├── AS_OF_FROM_TO/
│   ├── BNO/
│   │   ├── BNO-as_of_2025-05-31T220000+0000-data.parquet
│   │   └── BNO-as_of_2025-08-06T220000+0000-data.parquet
│   └── Prices and Volumes/
│       ├── Prices and Volumes-as_of_2023-12-31T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2024-01-31T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2025-10-31T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2025-11-30T230000+0000-data.parquet
│       └── ...
├── metadata/
│   ├── AZ_beverages-metadata.json
│   ├── AZ_drinks-metadata.json
│   ├── SampleDataset-metadata.json
│   ├── XYZ-metadata.json
│   └── ...
├── NONE_AT/
│   ├── PQR/
│   │   └── PQR-latest-data.parquet
│   ├── SampleDataset/
│   │   └── SampleDataset-latest-data.parquet
│   └── XYZ/
│       └── XYZ-latest-data.parquet
├── NONE_FROM_TO/
│   ├── AZ_beverages/
│   │   └── AZ_beverages-latest-data.parquet
│   ├── AZ_drinks/
│   │   └── AZ_drinks-latest-data.parquet
│   └── More Prices and Volumes/
│       └── More Prices and Volumes-latest-data.parquet
└── shared/
    └── default/
        ├── statistics/
        │   └── PQR/
        │       ├── PQR_p2019-12-31T23-00-00.000+00-00_p2025-08-14T22-00-00.000+00-00_v1.parquet
        │       └── PQR_p2019-12-31T23-00-00.000+00-00_p2025-08-14T22-00-00.000+00-00_v2.parquet
        └── XYZ/
            ├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v2.parquet
            └── XYZ_p2025-07-30T22-00-00.000+00-00_p2025-08-05T22-00-00.000+00-00_v2025-08-06T22-00-00.000+00-00_v_v1.parquet

</pre>

<!-- @output:YWSi -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">timeseries/
├── archives/
│   ├── statistics/
│   │   └── PQR/
│   │       ├── PQR_p2019-12-31T23-00-00.000+00-00_p2025-08-14T22-00-00.000+00-00_v1.parquet
│   │       └── PQR_p2019-12-31T23-00-00.000+00-00_p2025-08-14T22-00-00.000+00-00_v2.parquet
│   ├── The Sample Statistic/
│   │   └── statistics/
│   │       └── SampleDataset/
│   │           ├── SampleDataset_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v1.parquet
│   │           └── SampleDataset_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v2.parquet
│   └── XYZ/
│       ├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v2.parquet
│       └── XYZ_p2025-07-30T22-00-00.000+00-00_p2025-08-05T22-00-00.000+00-00_v2025-08-06T22-00-00.000+00-00_v_v1.parquet
├── AS_OF_AT/
│   ├── POPU06/
│   │   ├── POPU06-as_of_2023-12-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-01-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-10-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-11-30T230000+0000-data.parquet
│   │   └── ...
│   └── XYZ/
│       ├── XYZ-as_of_2025-04-30T220000+0000-data.parquet
│       ├── XYZ-as_of_2025-05-31T220000+0000-data.parquet
│       ├── XYZ-as_of_2025-08-05T220000+0000-data.parquet
│       ├── XYZ-as_of_2025-08-06T220000+0000-data.parquet
│       └── ...
├── AS_OF_FROM_TO/
│   ├── BNO/
│   │   ├── BNO-as_of_2025-05-31T220000+0000-data.parquet
│   │   └── BNO-as_of_2025-08-06T220000+0000-data.parquet
│   └── Prices and Volumes/
│       ├── Prices and Volumes-as_of_2023-12-31T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2024-01-31T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2025-10-31T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2025-11-30T230000+0000-data.parquet
│       └── ...
├── metadata/
│   ├── AZ_beverages-metadata.json
│   ├── AZ_drinks-metadata.json
│   ├── SampleDataset-metadata.json
│   ├── XYZ-metadata.json
│   └── ...
├── NONE_AT/
│   ├── PQR/
│   │   └── PQR-latest-data.parquet
│   ├── SampleDataset/
│   │   └── SampleDataset-latest-data.parquet
│   └── XYZ/
│       └── XYZ-latest-data.parquet
├── NONE_FROM_TO/
│   ├── AZ_beverages/
│   │   └── AZ_beverages-latest-data.parquet
│   ├── AZ_drinks/
│   │   └── AZ_drinks-latest-data.parquet
│   └── More Prices and Volumes/
│       └── More Prices and Volumes-latest-data.parquet
└── shared/
    └── default/
        ├── statistics/
        │   └── PQR/
        │       ├── PQR_p2019-12-31T23-00-00.000+00-00_p2025-08-14T22-00-00.000+00-00_v1.parquet
        │       └── PQR_p2019-12-31T23-00-00.000+00-00_p2025-08-14T22-00-00.000+00-00_v2.parquet
        └── XYZ/
            ├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v2.parquet
            └── XYZ_p2025-07-30T22-00-00.000+00-00_p2025-08-05T22-00-00.000+00-00_v2025-08-06T22-00-00.000+00-00_v_v1.parquet

</pre>

<!-- @output:YWSi -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">timeseries/
├── archives/
│   ├── statistics/
│   │   └── PQR/
│   │       ├── PQR_p2019-12-31T23-00-00.000+00-00_p2025-08-14T22-00-00.000+00-00_v1.parquet
│   │       └── PQR_p2019-12-31T23-00-00.000+00-00_p2025-08-14T22-00-00.000+00-00_v2.parquet
│   ├── The Sample Statistic/
│   │   └── statistics/
│   │       └── SampleDataset/
│   │           ├── SampleDataset_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v1.parquet
│   │           └── SampleDataset_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v2.parquet
│   └── XYZ/
│       ├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v2.parquet
│       └── XYZ_p2025-07-30T22-00-00.000+00-00_p2025-08-05T22-00-00.000+00-00_v2025-08-06T22-00-00.000+00-00_v_v1.parquet
├── AS_OF_AT/
│   ├── POPU06/
│   │   ├── POPU06-as_of_2023-12-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-01-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-10-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-11-30T230000+0000-data.parquet
│   │   └── ...
│   └── XYZ/
│       ├── XYZ-as_of_2025-04-30T220000+0000-data.parquet
│       ├── XYZ-as_of_2025-05-31T220000+0000-data.parquet
│       ├── XYZ-as_of_2025-08-05T220000+0000-data.parquet
│       ├── XYZ-as_of_2025-08-06T220000+0000-data.parquet
│       └── ...
├── AS_OF_FROM_TO/
│   ├── BNO/
│   │   ├── BNO-as_of_2025-05-31T220000+0000-data.parquet
│   │   └── BNO-as_of_2025-08-06T220000+0000-data.parquet
│   └── Prices and Volumes/
│       ├── Prices and Volumes-as_of_2023-12-31T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2024-01-31T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2025-10-31T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2025-11-30T230000+0000-data.parquet
│       └── ...
├── metadata/
│   ├── AZ_beverages-metadata.json
│   ├── AZ_drinks-metadata.json
│   ├── SampleDataset-metadata.json
│   ├── XYZ-metadata.json
│   └── ...
├── NONE_AT/
│   ├── PQR/
│   │   └── PQR-latest-data.parquet
│   ├── SampleDataset/
│   │   └── SampleDataset-latest-data.parquet
│   └── XYZ/
│       └── XYZ-latest-data.parquet
├── NONE_FROM_TO/
│   ├── AZ_beverages/
│   │   └── AZ_beverages-latest-data.parquet
│   ├── AZ_drinks/
│   │   └── AZ_drinks-latest-data.parquet
│   └── More Prices and Volumes/
│       └── More Prices and Volumes-latest-data.parquet
└── shared/
    └── default/
        ├── statistics/
        │   └── PQR/
        │       ├── PQR_p2019-12-31T23-00-00.000+00-00_p2025-08-14T22-00-00.000+00-00_v1.parquet
        │       └── PQR_p2019-12-31T23-00-00.000+00-00_p2025-08-14T22-00-00.000+00-00_v2.parquet
        └── XYZ/
            ├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v2.parquet
            └── XYZ_p2025-07-30T22-00-00.000+00-00_p2025-08-05T22-00-00.000+00-00_v2025-08-06T22-00-00.000+00-00_v_v1.parquet

</pre>

<!-- @output:YWSi -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">timeseries/
├── archives/
│   ├── statistics/
│   │   └── PQR/
│   │       ├── PQR_p2019-12-31T23-00-00.000+00-00_p2025-08-14T22-00-00.000+00-00_v1.parquet
│   │       └── PQR_p2019-12-31T23-00-00.000+00-00_p2025-08-14T22-00-00.000+00-00_v2.parquet
│   ├── The Sample Statistic/
│   │   └── statistics/
│   │       └── SampleDataset/
│   │           ├── SampleDataset_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v1.parquet
│   │           └── SampleDataset_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v2.parquet
│   └── XYZ/
│       ├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v2.parquet
│       └── XYZ_p2025-07-30T22-00-00.000+00-00_p2025-08-05T22-00-00.000+00-00_v2025-08-06T22-00-00.000+00-00_v_v1.parquet
├── AS_OF_AT/
│   ├── POPU06/
│   │   ├── POPU06-as_of_2023-12-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-01-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-10-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-11-30T230000+0000-data.parquet
│   │   └── ...
│   └── XYZ/
│       ├── XYZ-as_of_2025-04-30T220000+0000-data.parquet
│       ├── XYZ-as_of_2025-05-31T220000+0000-data.parquet
│       ├── XYZ-as_of_2025-08-05T220000+0000-data.parquet
│       ├── XYZ-as_of_2025-08-06T220000+0000-data.parquet
│       └── ...
├── AS_OF_FROM_TO/
│   ├── BNO/
│   │   ├── BNO-as_of_2025-05-31T220000+0000-data.parquet
│   │   └── BNO-as_of_2025-08-06T220000+0000-data.parquet
│   └── Prices and Volumes/
│       ├── Prices and Volumes-as_of_2023-12-31T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2024-01-31T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2025-10-31T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2025-11-30T230000+0000-data.parquet
│       └── ...
├── metadata/
│   ├── AZ_beverages-metadata.json
│   ├── AZ_drinks-metadata.json
│   ├── SampleDataset-metadata.json
│   ├── XYZ-metadata.json
│   └── ...
├── NONE_AT/
│   ├── PQR/
│   │   └── PQR-latest-data.parquet
│   ├── SampleDataset/
│   │   └── SampleDataset-latest-data.parquet
│   └── XYZ/
│       └── XYZ-latest-data.parquet
├── NONE_FROM_TO/
│   ├── AZ_beverages/
│   │   └── AZ_beverages-latest-data.parquet
│   ├── AZ_drinks/
│   │   └── AZ_drinks-latest-data.parquet
│   └── More Prices and Volumes/
│       └── More Prices and Volumes-latest-data.parquet
└── shared/
    └── default/
        ├── statistics/
        │   └── PQR/
        │       ├── PQR_p2019-12-31T23-00-00.000+00-00_p2025-08-14T22-00-00.000+00-00_v1.parquet
        │       └── PQR_p2019-12-31T23-00-00.000+00-00_p2025-08-14T22-00-00.000+00-00_v2.parquet
        └── XYZ/
            ├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v2.parquet
            └── XYZ_p2025-07-30T22-00-00.000+00-00_p2025-08-05T22-00-00.000+00-00_v2025-08-06T22-00-00.000+00-00_v_v1.parquet

</pre>

<!-- @output:YWSi -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">timeseries/
├── archives/
│   ├── statistics/
│   │   └── PQR/
│   │       ├── PQR_p2019-12-31T23-00-00.000+00-00_p2025-08-14T22-00-00.000+00-00_v1.parquet
│   │       └── PQR_p2019-12-31T23-00-00.000+00-00_p2025-08-14T22-00-00.000+00-00_v2.parquet
│   ├── The Sample Statistic/
│   │   └── statistics/
│   │       └── SampleDataset/
│   │           ├── SampleDataset_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v1.parquet
│   │           └── SampleDataset_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v2.parquet
│   └── XYZ/
│       ├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v2.parquet
│       └── XYZ_p2025-07-30T22-00-00.000+00-00_p2025-08-05T22-00-00.000+00-00_v2025-08-06T22-00-00.000+00-00_v_v1.parquet
├── AS_OF_AT/
│   ├── POPU06/
│   │   ├── POPU06-as_of_2023-12-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2024-01-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-10-31T230000+0000-data.parquet
│   │   ├── POPU06-as_of_2025-11-30T230000+0000-data.parquet
│   │   └── ...
│   └── XYZ/
│       ├── XYZ-as_of_2025-04-30T220000+0000-data.parquet
│       ├── XYZ-as_of_2025-05-31T220000+0000-data.parquet
│       ├── XYZ-as_of_2025-08-05T220000+0000-data.parquet
│       ├── XYZ-as_of_2025-08-06T220000+0000-data.parquet
│       └── ...
├── AS_OF_FROM_TO/
│   ├── BNO/
│   │   ├── BNO-as_of_2025-05-31T220000+0000-data.parquet
│   │   └── BNO-as_of_2025-08-06T220000+0000-data.parquet
│   └── Prices and Volumes/
│       ├── Prices and Volumes-as_of_2023-12-31T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2024-01-31T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2025-10-31T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2025-11-30T230000+0000-data.parquet
│       └── ...
├── metadata/
│   ├── AZ_beverages-metadata.json
│   ├── AZ_drinks-metadata.json
│   ├── SampleDataset-metadata.json
│   ├── XYZ-metadata.json
│   └── ...
├── NONE_AT/
│   ├── PQR/
│   │   └── PQR-latest-data.parquet
│   ├── SampleDataset/
│   │   └── SampleDataset-latest-data.parquet
│   └── XYZ/
│       └── XYZ-latest-data.parquet
├── NONE_FROM_TO/
│   ├── AZ_beverages/
│   │   └── AZ_beverages-latest-data.parquet
│   ├── AZ_drinks/
│   │   └── AZ_drinks-latest-data.parquet
│   └── More Prices and Volumes/
│       └── More Prices and Volumes-latest-data.parquet
└── shared/
    └── default/
        ├── statistics/
        │   └── PQR/
        │       ├── PQR_p2019-12-31T23-00-00.000+00-00_p2025-08-14T22-00-00.000+00-00_v1.parquet
        │       └── PQR_p2019-12-31T23-00-00.000+00-00_p2025-08-14T22-00-00.000+00-00_v2.parquet
        └── XYZ/
            ├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v2.parquet
            └── XYZ_p2025-07-30T22-00-00.000+00-00_p2025-08-05T22-00-00.000+00-00_v2025-08-06T22-00-00.000+00-00_v_v1.parquet

</pre>

```python {.marimo}
# reading the data back:
x = Dataset('PQR')
x.data    # ... now an Arrow table
```

<!-- @output:DnEU -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">pyarrow.Table
valid_at: timestamp&#91;ns, tz=UTC&#93;
p: double
q: double
r: double
----
valid_at: &#91;&#91;2019-12-31 23:00:00.000000000Z,2020-01-01 23:00:00.000000000Z,2020-01-02 23:00:00.000000000Z,2020-01-03 23:00:00.000000000Z,2020-01-04 23:00:00.000000000Z,...,2025-08-10 22:00:00.000000000Z,2025-08-11 22:00:00.000000000Z,2025-08-12 22:00:00.000000000Z,2025-08-13 22:00:00.000000000Z,2025-08-14 22:00:00.000000000Z&#93;&#93;
p: &#91;&#91;110,90,90,80,110,...,100,90,100,90,100&#93;&#93;
q: &#91;&#91;100,90,90,100,100,...,60,100,80,100,100&#93;&#93;
r: &#91;&#91;110,100,80,120,100,...,90,90,120,100,100&#93;&#93;</pre>

```python {.marimo}
x.nw.to_pandas()
```

<!-- @output:ulZA -->

| valid_at | p | q | r |
| --- | --- | --- | --- |
| 2019-12-31 23:00:00+00:00 | 110.0 | 100.0 | 110.0 |
| 2020-01-01 23:00:00+00:00 | 90.0 | 90.0 | 100.0 |
| 2020-01-02 23:00:00+00:00 | 90.0 | 90.0 | 80.0 |
| 2020-01-03 23:00:00+00:00 | 80.0 | 100.0 | 120.0 |
| 2020-01-04 23:00:00+00:00 | 110.0 | 100.0 | 100.0 |
| ... | ... | ... | ... |
| 2025-08-10 22:00:00+00:00 | 100.0 | 60.0 | 90.0 |
| 2025-08-11 22:00:00+00:00 | 90.0 | 100.0 | 90.0 |
| 2025-08-12 22:00:00+00:00 | 100.0 | 80.0 | 120.0 |
| 2025-08-13 22:00:00+00:00 | 90.0 | 100.0 | 100.0 |
| 2025-08-14 22:00:00+00:00 | 100.0 | 100.0 | 100.0 |

```python {.marimo}
x.plot()
```

<!-- @output:ecfG -->

![png](data-types-and-storage_assets/figure-1.png)

```python {.marimo}
more_pqr_data = some_simple_data_from_file_or_query('2025-05-29','2025-08-15')
more_pqr_data
```

<!-- @output:Pvdt -->

| valid_at | p | q | r |
| --- | --- | --- | --- |
| 2025-05-29 00:00:00+02:00 | 110.0 | 90.0 | 100.0 |
| 2025-05-30 00:00:00+02:00 | 100.0 | 100.0 | 100.0 |
| 2025-05-31 00:00:00+02:00 | 110.0 | 110.0 | 100.0 |
| 2025-06-01 00:00:00+02:00 | 100.0 | 90.0 | 100.0 |
| 2025-06-02 00:00:00+02:00 | 90.0 | 90.0 | 100.0 |
| ... | ... | ... | ... |
| 2025-08-11 00:00:00+02:00 | 110.0 | 90.0 | 90.0 |
| 2025-08-12 00:00:00+02:00 | 110.0 | 90.0 | 100.0 |
| 2025-08-13 00:00:00+02:00 | 100.0 | 90.0 | 100.0 |
| 2025-08-14 00:00:00+02:00 | 100.0 | 100.0 | 100.0 |
| 2025-08-15 00:00:00+02:00 | 100.0 | 100.0 | 90.0 |

```python {.marimo}
pqr_second_write = Dataset(
    name = 'PQR',
    data = more_pqr_data,
)
# obj init will retrieve previously saved metadata for an existing set and series:
print(pqr_second_write.tags)
```

<!-- @output:ZBYS -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">{&#x27;name&#x27;: &#x27;PQR&#x27;, &#x27;versioning&#x27;: &#x27;NONE&#x27;, &#x27;temporality&#x27;: &#x27;AT&#x27;, &#x27;series&#x27;: {&#x27;p&#x27;: {&#x27;dataset&#x27;: &#x27;PQR&#x27;, &#x27;name&#x27;: &#x27;p&#x27;, &#x27;variabel&#x27;: &#x27;pris&#x27;, &#x27;varegruppe&#x27;: &#x27;nødvendigheter&#x27;, &#x27;versioning&#x27;: &#x27;NONE&#x27;, &#x27;temporality&#x27;: &#x27;AT&#x27;, &#x27;repository&#x27;: &#x27;tutorials&#x27;, &#x27;vare&#x27;: &#x27;kaffe&#x27;, &#x27;variable&#x27;: &#x27;price&#x27;, &#x27;product group&#x27;: &#91;&#x27;essential&#x27;, &#x27;essentials&#x27;&#93;, &#x27;product&#x27;: &#x27;coffee&#x27;}, &#x27;q&#x27;: {&#x27;dataset&#x27;: &#x27;PQR&#x27;, &#x27;name&#x27;: &#x27;q&#x27;, &#x27;variabel&#x27;: &#x27;pris&#x27;, &#x27;varegruppe&#x27;: &#x27;nødvendigheter&#x27;, &#x27;versioning&#x27;: &#x27;NONE&#x27;, &#x27;temporality&#x27;: &#x27;AT&#x27;, &#x27;repository&#x27;: &#x27;tutorials&#x27;, &#x27;vare&#x27;: &#x27;knekkebrød&#x27;, &#x27;variable&#x27;: &#x27;price&#x27;, &#x27;product group&#x27;: &#91;&#x27;essential&#x27;, &#x27;essentials&#x27;&#93;, &#x27;product&#x27;: &#x27;crispbread&#x27;}, &#x27;r&#x27;: {&#x27;dataset&#x27;: &#x27;PQR&#x27;, &#x27;name&#x27;: &#x27;r&#x27;, &#x27;variabel&#x27;: &#x27;pris&#x27;, &#x27;varegruppe&#x27;: &#x27;nødvendigheter&#x27;, &#x27;versioning&#x27;: &#x27;NONE&#x27;, &#x27;temporality&#x27;: &#x27;AT&#x27;, &#x27;repository&#x27;: &#x27;tutorials&#x27;, &#x27;vare&#x27;: &#x27;brunost&#x27;, &#x27;variable&#x27;: &#x27;price&#x27;, &#x27;product group&#x27;: &#91;&#x27;essential&#x27;, &#x27;essentials&#x27;&#93;, &#x27;product&#x27;: &#x27;brown cheese&#x27;}}, &#x27;repository&#x27;: &#x27;tutorials&#x27;, &#x27;variabel&#x27;: &#x27;pris&#x27;, &#x27;varegruppe&#x27;: &#x27;nødvendigheter&#x27;, &#x27;variable&#x27;: &#x27;price&#x27;, &#x27;product group&#x27;: &#91;&#x27;essential&#x27;, &#x27;essentials&#x27;&#93;}
</pre>

```python {.marimo}
pqr_second_write.save()
```

```python {.marimo}
# in memory object instances do not change
print(pqr.data)
print(pqr_second_write.data)
```

<!-- @output:nHfw -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">                      valid_at      p      q      r
0    2019-12-31 23:00:00+00:00  110.0  100.0  110.0
1    2020-01-01 23:00:00+00:00   90.0   90.0  100.0
2    2020-01-02 23:00:00+00:00   90.0   90.0   80.0
3    2020-01-03 23:00:00+00:00   80.0  100.0  120.0
4    2020-01-04 23:00:00+00:00  110.0  100.0  100.0
...                        ...    ...    ...    ...
1974 2025-05-27 22:00:00+00:00   90.0  120.0   90.0
1975 2025-05-28 22:00:00+00:00  110.0  100.0   80.0
1976 2025-05-29 22:00:00+00:00  100.0  120.0  110.0
1977 2025-05-30 22:00:00+00:00  100.0  100.0  100.0
1978 2025-05-31 22:00:00+00:00  100.0  100.0   90.0

&#91;1979 rows x 4 columns&#93;
                    valid_at      p      q      r
0  2025-05-28 22:00:00+00:00  110.0   90.0  100.0
1  2025-05-29 22:00:00+00:00  100.0  100.0  100.0
2  2025-05-30 22:00:00+00:00  110.0  110.0  100.0
3  2025-05-31 22:00:00+00:00  100.0   90.0  100.0
4  2025-06-01 22:00:00+00:00   90.0   90.0  100.0
..                       ...    ...    ...    ...
74 2025-08-10 22:00:00+00:00  110.0   90.0   90.0
75 2025-08-11 22:00:00+00:00  110.0   90.0  100.0
76 2025-08-12 22:00:00+00:00  100.0   90.0  100.0
77 2025-08-13 22:00:00+00:00  100.0  100.0  100.0
78 2025-08-14 22:00:00+00:00  100.0  100.0   90.0

&#91;79 rows x 4 columns&#93;
</pre>

```python {.marimo}
y = Dataset('PQR')
y.nw.to_polars()
```

<!-- @output:xXTn -->

| valid_at | p | q | r |
| --- | --- | --- | --- |
| datetime[ns, UTC] | f64 | f64 | f64 |
| 2019-12-31 23:00:00 UTC | 110.0 | 100.0 | 110.0 |
| 2020-01-01 23:00:00 UTC | 90.0 | 90.0 | 100.0 |
| 2020-01-02 23:00:00 UTC | 90.0 | 90.0 | 80.0 |
| 2020-01-03 23:00:00 UTC | 80.0 | 100.0 | 120.0 |
| 2020-01-04 23:00:00 UTC | 110.0 | 100.0 | 100.0 |
| … | … | … | … |
| 2025-08-10 22:00:00 UTC | 110.0 | 90.0 | 90.0 |
| 2025-08-11 22:00:00 UTC | 110.0 | 90.0 | 100.0 |
| 2025-08-12 22:00:00 UTC | 100.0 | 90.0 | 100.0 |
| 2025-08-13 22:00:00 UTC | 100.0 | 100.0 | 100.0 |
| 2025-08-14 22:00:00 UTC | 100.0 | 100.0 | 90.0 |

```python {.marimo}
# ... but the data file has been overwritten:
y.nw.to_polars().filter(
    pl.col("valid_at").is_between(pl.date(2025, 5, 29), pl.date(2025, 6, 2))
)
```

<!-- @output:AjVT -->

| valid_at | p | q | r |
| --- | --- | --- | --- |
| datetime[ns, UTC] | f64 | f64 | f64 |
| 2025-05-29 22:00:00 UTC | 100.0 | 100.0 | 100.0 |
| 2025-05-30 22:00:00 UTC | 110.0 | 110.0 | 100.0 |
| 2025-05-31 22:00:00 UTC | 100.0 | 90.0 | 100.0 |
| 2025-06-01 22:00:00 UTC | 90.0 | 90.0 | 100.0 |

```python {.marimo}
# note that for unversioned type: we operate on the same files all the way
print(tree(data_path))
```

<!-- @output:pHFh -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">timeseries/
├── archives/
│   ├── statistics/
│   │   └── PQR/
│   │       ├── PQR_p2019-12-31T23-00-00.000+00-00_p2025-08-14T22-00-00.000+00-00_v1.parquet
│   │       └── PQR_p2019-12-31T23-00-00.000+00-00_p2025-08-14T22-00-00.000+00-00_v2.parquet
│   ├── The Sample Statistic/
│   │   └── statistics/
│   │       └── SampleDataset/
│   │           ├── SampleDataset_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v1.parquet
│   │           └── SampleDataset_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v2.parquet
│   └── XYZ/
│       ├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v2.parquet
│       └── XYZ_p2025-07-30T22-00-00.000+00-00_p2025-08-05T22-00-00.000+00-00_v2025-08-06T22-00-00.000+00-00_v_v1.parquet
├── AS_OF_AT/
│   └── POPU06/
│       ├── POPU06-as_of_2023-12-31T230000+0000-data.parquet
│       ├── POPU06-as_of_2024-01-31T230000+0000-data.parquet
│       ├── POPU06-as_of_2025-10-31T230000+0000-data.parquet
│       ├── POPU06-as_of_2025-11-30T230000+0000-data.parquet
│       └── ...
├── AS_OF_FROM_TO/
│   ├── BNO/
│   │   ├── BNO-as_of_2025-05-31T220000+0000-data.parquet
│   │   └── BNO-as_of_2025-08-06T220000+0000-data.parquet
│   └── Prices and Volumes/
│       ├── Prices and Volumes-as_of_2023-12-31T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2024-01-31T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2025-10-31T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2025-11-30T230000+0000-data.parquet
│       └── ...
├── metadata/
│   ├── AZ_beverages-metadata.json
│   ├── AZ_drinks-metadata.json
│   ├── SampleDataset-metadata.json
│   ├── XYZ-metadata.json
│   └── ...
├── NONE_AT/
│   ├── PQR/
│   │   └── PQR-latest-data.parquet
│   ├── SampleDataset/
│   │   └── SampleDataset-latest-data.parquet
│   └── XYZ/
│       └── XYZ-latest-data.parquet
├── NONE_FROM_TO/
│   ├── AZ_beverages/
│   │   └── AZ_beverages-latest-data.parquet
│   ├── AZ_drinks/
│   │   └── AZ_drinks-latest-data.parquet
│   └── More Prices and Volumes/
│       └── More Prices and Volumes-latest-data.parquet
└── shared/
    └── default/
        ├── statistics/
        │   └── PQR/
        │       ├── PQR_p2019-12-31T23-00-00.000+00-00_p2025-08-14T22-00-00.000+00-00_v1.parquet
        │       └── PQR_p2019-12-31T23-00-00.000+00-00_p2025-08-14T22-00-00.000+00-00_v2.parquet
        └── XYZ/
            ├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v2.parquet
            └── XYZ_p2025-07-30T22-00-00.000+00-00_p2025-08-05T22-00-00.000+00-00_v2025-08-06T22-00-00.000+00-00_v_v1.parquet

</pre>

```python {.marimo}
print(tree(data_path))
```

```python {.marimo}
x.data = x.nw.to_pandas() # <-- workaround for bug in groupby
xx = x.groupby('Q','sum')

# xx is a new dataset, hence gets a new name on creation:
xx
```

<!-- @output:aqbW -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">Dataset(name=&quot;(PQR.groupby(Q,sum))&quot;, repository=&quot;tutorials&quot;, data_type=SeriesType(Versioning.NONE,Temporality.AT), as_of_tz=None)</pre>

```python {.marimo}
xx.data
```

<!-- @output:TRpd -->

| p | q | r |
| --- | --- | --- |
|  |  |  |
| 110.0 | 100.0 | 110.0 |
| 8960.0 | 9030.0 | 8940.0 |
| 9160.0 | 9250.0 | 9020.0 |
| 9260.0 | 9070.0 | 9340.0 |
| 9270.0 | 9280.0 | 9330.0 |
| ... | ... | ... |
| 9330.0 | 9270.0 | 9050.0 |
| 9200.0 | 9160.0 | 9130.0 |
| 8880.0 | 9050.0 | 9100.0 |
| 8970.0 | 9070.0 | 8920.0 |
| 4520.0 | 4450.0 | 4490.0 |

### Example: data for periods, *without* versioning

```python {.marimo}
interval_data = SeriesType(Versioning.NONE, Temporality.FROM_TO)
```

```python {.marimo}
def mock_interval_data_from_file_or_query(start, end):
    a_to_z = [chr(i) for i in range(ord('a'), ord('z') + 1)]
    variables = ['quantity', 'price']
    goods = ['coffee', 'tea', 'soda', 'beer', 'wine']
    return create_df(
        a_to_z, variables, goods,
        start_date=start,
        end_date=end,
        freq='M',
        temporality='FROM_TO',
        implementation='polars'
    )
```

```python {.marimo}
bigger_data = mock_interval_data_from_file_or_query(start='2025-01-01', end='2025-06-01')
bigger_data.shape
```

<!-- @output:wlCL -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">&#91;6, 262&#93;</pre>

```python {.marimo}
bigger_data
```

<!-- @output:kqZH -->

| valid_from | valid_to | a_quantity_coffee | a_quantity_tea | a_quantity_soda | a_quantity_beer | a_quantity_wine | a_price_coffee | a_price_tea | a_price_soda | a_price_beer | a_price_wine | b_quantity_coffee | b_quantity_tea | b_quantity_soda | b_quantity_beer | b_quantity_wine | b_price_coffee | b_price_tea | b_price_soda | b_price_beer | b_price_wine | c_quantity_coffee | c_quantity_tea | c_quantity_soda | c_quantity_beer | c_quantity_wine | c_price_coffee | c_price_tea | c_price_soda | c_price_beer | c_price_wine | d_quantity_coffee | d_quantity_tea | d_quantity_soda | d_quantity_beer | d_quantity_wine | … | w_quantity_beer | w_quantity_wine | w_price_coffee | w_price_tea | w_price_soda | w_price_beer | w_price_wine | x_quantity_coffee | x_quantity_tea | x_quantity_soda | x_quantity_beer | x_quantity_wine | x_price_coffee | x_price_tea | x_price_soda | x_price_beer | x_price_wine | y_quantity_coffee | y_quantity_tea | y_quantity_soda | y_quantity_beer | y_quantity_wine | y_price_coffee | y_price_tea | y_price_soda | y_price_beer | y_price_wine | z_quantity_coffee | z_quantity_tea | z_quantity_soda | z_quantity_beer | z_quantity_wine | z_price_coffee | z_price_tea | z_price_soda | z_price_beer | z_price_wine |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| datetime[μs, Europe/Oslo] | datetime[μs, Europe/Oslo] | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | … | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 |
| 2025-01-01 00:00:00 CET | 2025-02-01 00:00:00 CET | 100.0 | 100.0 | 110.0 | 100.0 | 110.0 | 80.0 | 90.0 | 110.0 | 110.0 | 100.0 | 90.0 | 110.0 | 110.0 | 110.0 | 110.0 | 110.0 | 90.0 | 100.0 | 90.0 | 100.0 | 110.0 | 120.0 | 100.0 | 100.0 | 100.0 | 110.0 | 110.0 | 90.0 | 90.0 | 90.0 | 100.0 | 110.0 | 100.0 | 130.0 | 100.0 | … | 90.0 | 90.0 | 100.0 | 100.0 | 90.0 | 100.0 | 110.0 | 90.0 | 100.0 | 100.0 | 110.0 | 90.0 | 90.0 | 100.0 | 80.0 | 100.0 | 110.0 | 100.0 | 110.0 | 80.0 | 100.0 | 110.0 | 90.0 | 110.0 | 100.0 | 80.0 | 100.0 | 110.0 | 90.0 | 120.0 | 80.0 | 80.0 | 90.0 | 100.0 | 110.0 | 110.0 | 100.0 |
| 2025-02-01 00:00:00 CET | 2025-03-01 00:00:00 CET | 110.0 | 110.0 | 120.0 | 100.0 | 100.0 | 100.0 | 100.0 | 120.0 | 100.0 | 100.0 | 100.0 | 100.0 | 100.0 | 100.0 | 100.0 | 80.0 | 120.0 | 110.0 | 110.0 | 100.0 | 120.0 | 100.0 | 90.0 | 90.0 | 100.0 | 80.0 | 100.0 | 90.0 | 100.0 | 90.0 | 100.0 | 110.0 | 80.0 | 110.0 | 90.0 | … | 80.0 | 100.0 | 100.0 | 90.0 | 110.0 | 100.0 | 80.0 | 110.0 | 110.0 | 110.0 | 100.0 | 90.0 | 90.0 | 110.0 | 90.0 | 100.0 | 90.0 | 110.0 | 100.0 | 90.0 | 100.0 | 100.0 | 90.0 | 90.0 | 100.0 | 110.0 | 110.0 | 120.0 | 120.0 | 110.0 | 100.0 | 100.0 | 100.0 | 90.0 | 70.0 | 100.0 | 90.0 |
| 2025-03-01 00:00:00 CET | 2025-04-01 00:00:00 CEST | 80.0 | 100.0 | 100.0 | 100.0 | 100.0 | 120.0 | 100.0 | 90.0 | 110.0 | 110.0 | 100.0 | 110.0 | 100.0 | 100.0 | 120.0 | 100.0 | 100.0 | 110.0 | 110.0 | 110.0 | 100.0 | 110.0 | 90.0 | 100.0 | 90.0 | 100.0 | 100.0 | 100.0 | 110.0 | 100.0 | 100.0 | 90.0 | 100.0 | 100.0 | 100.0 | … | 120.0 | 130.0 | 100.0 | 80.0 | 90.0 | 90.0 | 100.0 | 90.0 | 90.0 | 80.0 | 100.0 | 100.0 | 100.0 | 90.0 | 110.0 | 100.0 | 110.0 | 90.0 | 130.0 | 100.0 | 80.0 | 120.0 | 130.0 | 100.0 | 110.0 | 110.0 | 110.0 | 90.0 | 80.0 | 90.0 | 110.0 | 100.0 | 110.0 | 100.0 | 90.0 | 90.0 | 80.0 |
| 2025-04-01 00:00:00 CEST | 2025-05-01 00:00:00 CEST | 90.0 | 90.0 | 110.0 | 100.0 | 70.0 | 60.0 | 110.0 | 100.0 | 100.0 | 110.0 | 100.0 | 90.0 | 120.0 | 110.0 | 90.0 | 110.0 | 110.0 | 100.0 | 110.0 | 100.0 | 110.0 | 100.0 | 100.0 | 80.0 | 100.0 | 100.0 | 100.0 | 100.0 | 80.0 | 90.0 | 90.0 | 100.0 | 100.0 | 100.0 | 90.0 | … | 100.0 | 90.0 | 100.0 | 110.0 | 110.0 | 100.0 | 110.0 | 100.0 | 120.0 | 90.0 | 110.0 | 100.0 | 110.0 | 120.0 | 90.0 | 100.0 | 90.0 | 100.0 | 110.0 | 80.0 | 80.0 | 100.0 | 90.0 | 90.0 | 100.0 | 80.0 | 100.0 | 120.0 | 90.0 | 80.0 | 110.0 | 100.0 | 70.0 | 100.0 | 100.0 | 110.0 | 80.0 |
| 2025-05-01 00:00:00 CEST | 2025-06-01 00:00:00 CEST | 120.0 | 90.0 | 120.0 | 100.0 | 90.0 | 110.0 | 100.0 | 110.0 | 110.0 | 100.0 | 110.0 | 100.0 | 100.0 | 70.0 | 120.0 | 90.0 | 100.0 | 100.0 | 100.0 | 100.0 | 100.0 | 130.0 | 110.0 | 100.0 | 90.0 | 110.0 | 100.0 | 100.0 | 100.0 | 100.0 | 80.0 | 100.0 | 90.0 | 100.0 | 90.0 | … | 100.0 | 80.0 | 100.0 | 110.0 | 110.0 | 110.0 | 100.0 | 110.0 | 110.0 | 90.0 | 100.0 | 110.0 | 110.0 | 100.0 | 110.0 | 90.0 | 100.0 | 90.0 | 90.0 | 110.0 | 100.0 | 110.0 | 90.0 | 100.0 | 80.0 | 100.0 | 130.0 | 100.0 | 90.0 | 90.0 | 100.0 | 110.0 | 90.0 | 100.0 | 90.0 | 110.0 | 100.0 |
| 2025-06-01 00:00:00 CEST | 2025-07-01 00:00:00 CEST | 110.0 | 110.0 | 100.0 | 110.0 | 100.0 | 90.0 | 100.0 | 100.0 | 120.0 | 90.0 | 110.0 | 90.0 | 100.0 | 110.0 | 100.0 | 100.0 | 100.0 | 90.0 | 100.0 | 110.0 | 110.0 | 90.0 | 110.0 | 100.0 | 120.0 | 100.0 | 80.0 | 110.0 | 90.0 | 90.0 | 110.0 | 100.0 | 100.0 | 80.0 | 110.0 | … | 100.0 | 100.0 | 100.0 | 90.0 | 100.0 | 100.0 | 110.0 | 80.0 | 90.0 | 110.0 | 110.0 | 110.0 | 90.0 | 110.0 | 100.0 | 100.0 | 90.0 | 110.0 | 100.0 | 90.0 | 100.0 | 90.0 | 110.0 | 120.0 | 110.0 | 100.0 | 110.0 | 110.0 | 90.0 | 110.0 | 90.0 | 110.0 | 100.0 | 100.0 | 110.0 | 100.0 | 110.0 |

```python {.marimo}
az = Dataset(
    name = 'AZ_beverages',
    data_type = interval_data,
    data = bigger_data,
    attributes=['store','variable','product'],
)
```

```python {.marimo}
# the per-series entries make the full tag dictionary too large to show:
print({k: v for k, v in az.tags.items() if k != 'series'})
az.tags['series']['a_price_beer']
```

<!-- @output:rEll -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">{&#x27;name&#x27;: &#x27;AZ_beverages&#x27;, &#x27;versioning&#x27;: &#x27;NONE&#x27;, &#x27;temporality&#x27;: &#x27;FROM_TO&#x27;, &#x27;repository&#x27;: &#x27;tutorials&#x27;}
</pre>

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">{&#x27;dataset&#x27;: &#x27;AZ_beverages&#x27;,
 &#x27;name&#x27;: &#x27;a_price_beer&#x27;,
 &#x27;product&#x27;: &#x27;beer&#x27;,
 &#x27;repository&#x27;: &#x27;tutorials&#x27;,
 &#x27;store&#x27;: &#x27;a&#x27;,
 &#x27;temporality&#x27;: &#x27;FROM_TO&#x27;,
 &#x27;variable&#x27;: &#x27;price&#x27;,
 &#x27;versioning&#x27;: &#x27;NONE&#x27;}</pre>

```python {.marimo}
# the periods need two date columns, since they have a duration:
az.data
```

<!-- @output:dGlV -->

| valid_from | valid_to | a_quantity_coffee | a_quantity_tea | a_quantity_soda | a_quantity_beer | a_quantity_wine | a_price_coffee | a_price_tea | a_price_soda | a_price_beer | a_price_wine | b_quantity_coffee | b_quantity_tea | b_quantity_soda | b_quantity_beer | b_quantity_wine | b_price_coffee | b_price_tea | b_price_soda | b_price_beer | b_price_wine | c_quantity_coffee | c_quantity_tea | c_quantity_soda | c_quantity_beer | c_quantity_wine | c_price_coffee | c_price_tea | c_price_soda | c_price_beer | c_price_wine | d_quantity_coffee | d_quantity_tea | d_quantity_soda | d_quantity_beer | d_quantity_wine | … | w_quantity_beer | w_quantity_wine | w_price_coffee | w_price_tea | w_price_soda | w_price_beer | w_price_wine | x_quantity_coffee | x_quantity_tea | x_quantity_soda | x_quantity_beer | x_quantity_wine | x_price_coffee | x_price_tea | x_price_soda | x_price_beer | x_price_wine | y_quantity_coffee | y_quantity_tea | y_quantity_soda | y_quantity_beer | y_quantity_wine | y_price_coffee | y_price_tea | y_price_soda | y_price_beer | y_price_wine | z_quantity_coffee | z_quantity_tea | z_quantity_soda | z_quantity_beer | z_quantity_wine | z_price_coffee | z_price_tea | z_price_soda | z_price_beer | z_price_wine |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| datetime[ns, UTC] | datetime[ns, UTC] | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | … | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 |
| 2024-12-31 23:00:00 UTC | 2025-01-31 23:00:00 UTC | 100.0 | 100.0 | 110.0 | 100.0 | 110.0 | 80.0 | 90.0 | 110.0 | 110.0 | 100.0 | 90.0 | 110.0 | 110.0 | 110.0 | 110.0 | 110.0 | 90.0 | 100.0 | 90.0 | 100.0 | 110.0 | 120.0 | 100.0 | 100.0 | 100.0 | 110.0 | 110.0 | 90.0 | 90.0 | 90.0 | 100.0 | 110.0 | 100.0 | 130.0 | 100.0 | … | 90.0 | 90.0 | 100.0 | 100.0 | 90.0 | 100.0 | 110.0 | 90.0 | 100.0 | 100.0 | 110.0 | 90.0 | 90.0 | 100.0 | 80.0 | 100.0 | 110.0 | 100.0 | 110.0 | 80.0 | 100.0 | 110.0 | 90.0 | 110.0 | 100.0 | 80.0 | 100.0 | 110.0 | 90.0 | 120.0 | 80.0 | 80.0 | 90.0 | 100.0 | 110.0 | 110.0 | 100.0 |
| 2025-01-31 23:00:00 UTC | 2025-02-28 23:00:00 UTC | 110.0 | 110.0 | 120.0 | 100.0 | 100.0 | 100.0 | 100.0 | 120.0 | 100.0 | 100.0 | 100.0 | 100.0 | 100.0 | 100.0 | 100.0 | 80.0 | 120.0 | 110.0 | 110.0 | 100.0 | 120.0 | 100.0 | 90.0 | 90.0 | 100.0 | 80.0 | 100.0 | 90.0 | 100.0 | 90.0 | 100.0 | 110.0 | 80.0 | 110.0 | 90.0 | … | 80.0 | 100.0 | 100.0 | 90.0 | 110.0 | 100.0 | 80.0 | 110.0 | 110.0 | 110.0 | 100.0 | 90.0 | 90.0 | 110.0 | 90.0 | 100.0 | 90.0 | 110.0 | 100.0 | 90.0 | 100.0 | 100.0 | 90.0 | 90.0 | 100.0 | 110.0 | 110.0 | 120.0 | 120.0 | 110.0 | 100.0 | 100.0 | 100.0 | 90.0 | 70.0 | 100.0 | 90.0 |
| 2025-02-28 23:00:00 UTC | 2025-03-31 22:00:00 UTC | 80.0 | 100.0 | 100.0 | 100.0 | 100.0 | 120.0 | 100.0 | 90.0 | 110.0 | 110.0 | 100.0 | 110.0 | 100.0 | 100.0 | 120.0 | 100.0 | 100.0 | 110.0 | 110.0 | 110.0 | 100.0 | 110.0 | 90.0 | 100.0 | 90.0 | 100.0 | 100.0 | 100.0 | 110.0 | 100.0 | 100.0 | 90.0 | 100.0 | 100.0 | 100.0 | … | 120.0 | 130.0 | 100.0 | 80.0 | 90.0 | 90.0 | 100.0 | 90.0 | 90.0 | 80.0 | 100.0 | 100.0 | 100.0 | 90.0 | 110.0 | 100.0 | 110.0 | 90.0 | 130.0 | 100.0 | 80.0 | 120.0 | 130.0 | 100.0 | 110.0 | 110.0 | 110.0 | 90.0 | 80.0 | 90.0 | 110.0 | 100.0 | 110.0 | 100.0 | 90.0 | 90.0 | 80.0 |
| 2025-03-31 22:00:00 UTC | 2025-04-30 22:00:00 UTC | 90.0 | 90.0 | 110.0 | 100.0 | 70.0 | 60.0 | 110.0 | 100.0 | 100.0 | 110.0 | 100.0 | 90.0 | 120.0 | 110.0 | 90.0 | 110.0 | 110.0 | 100.0 | 110.0 | 100.0 | 110.0 | 100.0 | 100.0 | 80.0 | 100.0 | 100.0 | 100.0 | 100.0 | 80.0 | 90.0 | 90.0 | 100.0 | 100.0 | 100.0 | 90.0 | … | 100.0 | 90.0 | 100.0 | 110.0 | 110.0 | 100.0 | 110.0 | 100.0 | 120.0 | 90.0 | 110.0 | 100.0 | 110.0 | 120.0 | 90.0 | 100.0 | 90.0 | 100.0 | 110.0 | 80.0 | 80.0 | 100.0 | 90.0 | 90.0 | 100.0 | 80.0 | 100.0 | 120.0 | 90.0 | 80.0 | 110.0 | 100.0 | 70.0 | 100.0 | 100.0 | 110.0 | 80.0 |
| 2025-04-30 22:00:00 UTC | 2025-05-31 22:00:00 UTC | 120.0 | 90.0 | 120.0 | 100.0 | 90.0 | 110.0 | 100.0 | 110.0 | 110.0 | 100.0 | 110.0 | 100.0 | 100.0 | 70.0 | 120.0 | 90.0 | 100.0 | 100.0 | 100.0 | 100.0 | 100.0 | 130.0 | 110.0 | 100.0 | 90.0 | 110.0 | 100.0 | 100.0 | 100.0 | 100.0 | 80.0 | 100.0 | 90.0 | 100.0 | 90.0 | … | 100.0 | 80.0 | 100.0 | 110.0 | 110.0 | 110.0 | 100.0 | 110.0 | 110.0 | 90.0 | 100.0 | 110.0 | 110.0 | 100.0 | 110.0 | 90.0 | 100.0 | 90.0 | 90.0 | 110.0 | 100.0 | 110.0 | 90.0 | 100.0 | 80.0 | 100.0 | 130.0 | 100.0 | 90.0 | 90.0 | 100.0 | 110.0 | 90.0 | 100.0 | 90.0 | 110.0 | 100.0 |
| 2025-05-31 22:00:00 UTC | 2025-06-30 22:00:00 UTC | 110.0 | 110.0 | 100.0 | 110.0 | 100.0 | 90.0 | 100.0 | 100.0 | 120.0 | 90.0 | 110.0 | 90.0 | 100.0 | 110.0 | 100.0 | 100.0 | 100.0 | 90.0 | 100.0 | 110.0 | 110.0 | 90.0 | 110.0 | 100.0 | 120.0 | 100.0 | 80.0 | 110.0 | 90.0 | 90.0 | 110.0 | 100.0 | 100.0 | 80.0 | 110.0 | … | 100.0 | 100.0 | 100.0 | 90.0 | 100.0 | 100.0 | 110.0 | 80.0 | 90.0 | 110.0 | 110.0 | 110.0 | 90.0 | 110.0 | 100.0 | 100.0 | 90.0 | 110.0 | 100.0 | 90.0 | 100.0 | 90.0 | 110.0 | 120.0 | 110.0 | 100.0 | 110.0 | 110.0 | 90.0 | 110.0 | 90.0 | 110.0 | 100.0 | 100.0 | 110.0 | 100.0 | 110.0 |

```python {.marimo}
print(tree(data_path))
```

```python {.marimo}
az.save()
print(tree(data_path))
```

<!-- @output:lgWD -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">timeseries/
├── archives/
│   ├── statistics/
│   │   └── PQR/
│   │       ├── PQR_p2019-12-31T23-00-00.000+00-00_p2025-08-14T22-00-00.000+00-00_v1.parquet
│   │       └── PQR_p2019-12-31T23-00-00.000+00-00_p2025-08-14T22-00-00.000+00-00_v2.parquet
│   ├── The Sample Statistic/
│   │   └── statistics/
│   │       └── SampleDataset/
│   │           ├── SampleDataset_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v1.parquet
│   │           └── SampleDataset_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v2.parquet
│   └── XYZ/
│       ├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v2.parquet
│       └── XYZ_p2025-07-30T22-00-00.000+00-00_p2025-08-05T22-00-00.000+00-00_v2025-08-06T22-00-00.000+00-00_v_v1.parquet
├── AS_OF_AT/
│   └── POPU06/
│       ├── POPU06-as_of_2023-12-31T230000+0000-data.parquet
│       ├── POPU06-as_of_2024-01-31T230000+0000-data.parquet
│       ├── POPU06-as_of_2025-10-31T230000+0000-data.parquet
│       ├── POPU06-as_of_2025-11-30T230000+0000-data.parquet
│       └── ...
├── AS_OF_FROM_TO/
│   ├── BNO/
│   │   ├── BNO-as_of_2025-05-31T220000+0000-data.parquet
│   │   └── BNO-as_of_2025-08-06T220000+0000-data.parquet
│   └── Prices and Volumes/
│       ├── Prices and Volumes-as_of_2023-12-31T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2024-01-31T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2025-10-31T230000+0000-data.parquet
│       ├── Prices and Volumes-as_of_2025-11-30T230000+0000-data.parquet
│       └── ...
├── metadata/
│   ├── AZ_beverages-metadata.json
│   ├── AZ_drinks-metadata.json
│   ├── SampleDataset-metadata.json
│   ├── XYZ-metadata.json
│   └── ...
├── NONE_AT/
│   ├── PQR/
│   │   └── PQR-latest-data.parquet
│   ├── SampleDataset/
│   │   └── SampleDataset-latest-data.parquet
│   └── XYZ/
│       └── XYZ-latest-data.parquet
├── NONE_FROM_TO/
│   ├── AZ_beverages/
│   │   └── AZ_beverages-latest-data.parquet
│   ├── AZ_drinks/
│   │   └── AZ_drinks-latest-data.parquet
│   └── More Prices and Volumes/
│       └── More Prices and Volumes-latest-data.parquet
└── shared/
    └── default/
        ├── statistics/
        │   └── PQR/
        │       ├── PQR_p2019-12-31T23-00-00.000+00-00_p2025-08-14T22-00-00.000+00-00_v1.parquet
        │       └── PQR_p2019-12-31T23-00-00.000+00-00_p2025-08-14T22-00-00.000+00-00_v2.parquet
        └── XYZ/
            ├── XYZ_p2021-12-31T23-00-00.000+00-00_p2022-11-30T23-00-00.000+00-00_v2.parquet
            └── XYZ_p2025-07-30T22-00-00.000+00-00_p2025-08-05T22-00-00.000+00-00_v2025-08-06T22-00-00.000+00-00_v_v1.parquet

</pre>

### Example: point-in-time data, *with* versioning

```python {.marimo}
estimated_point_in_time = SeriesType(Versioning.AS_OF, Temporality.AT)
```

```python {.marimo}
print(tree(data_path))
```

```python {.marimo}
def data_for_n_days_prior(as_of, n):
    start = ensure_datetime(as_of) - timedelta(days=n)
    end = ensure_datetime(as_of) - timedelta(days=1)
    return create_df(['x','y','z'], start_date=start,end_date=end, freq='D')
```

```python {.marimo}
n = 7
data_for_n_days_prior('2024-03-15', n)
```

<!-- @output:jxvo -->

| valid_at | x | y | z |
| --- | --- | --- | --- |
| 2024-03-08 00:00:00+01:00 | 90.0 | 90.0 | 90.0 |
| 2024-03-09 00:00:00+01:00 | 110.0 | 110.0 | 100.0 |
| 2024-03-10 00:00:00+01:00 | 100.0 | 100.0 | 90.0 |
| 2024-03-11 00:00:00+01:00 | 100.0 | 100.0 | 90.0 |
| 2024-03-12 00:00:00+01:00 | 110.0 | 110.0 | 100.0 |
| 2024-03-13 00:00:00+01:00 | 90.0 | 110.0 | 100.0 |
| 2024-03-14 00:00:00+01:00 | 80.0 | 90.0 | 110.0 |

```python {.marimo}
as_of_dates = ['2025-05-01','2025-06-01','2025-08-03','2025-08-04','2025-08-05','2025-08-06','2025-08-07']
```

```python {.marimo}
# update the data for several as of dates
# --> simulates running the production process for several periods
for as_of in as_of_dates:
    xyz_df = data_for_n_days_prior(as_of,n)
    Dataset(
        name = 'XYZ',
        data_type = estimated_point_in_time,
        as_of_tz=date_utc(as_of),
        data = xyz_df,
    ).save()
```

```python {.marimo}
print(tree(data_path))
```

```python {.marimo}
first = Dataset('XYZ', as_of_tz=as_of_dates[0])
last = Dataset('XYZ', as_of_tz=as_of_dates[-1])
```

```python {.marimo}
specific = Dataset('XYZ', as_of_tz='2025-08-04')
specific
```

<!-- @output:tZnO -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">Dataset(name=&quot;XYZ&quot;, repository=&quot;tutorials&quot;, data_type=SeriesType(Versioning.NONE,Temporality.AT), as_of_tz=&quot;2025-08-03T22:00:00+00:00&quot;)</pre>

```python {.marimo}
last
```

<!-- @output:xvXZ -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">Dataset(name=&quot;XYZ&quot;, repository=&quot;tutorials&quot;, data_type=SeriesType(Versioning.NONE,Temporality.AT), as_of_tz=&quot;2025-08-06T22:00:00+00:00&quot;)</pre>

```python {.marimo}
first.nw.to_pandas()
```

<!-- @output:CLip -->

| valid_at | x | y | z |
| --- | --- | --- | --- |
| 2021-12-31 23:00:00+00:00 | 100.0 | 100.0 | 110.0 |
| 2022-01-31 23:00:00+00:00 | 100.0 | 110.0 | 100.0 |
| 2022-02-28 23:00:00+00:00 | 90.0 | 100.0 | 100.0 |
| 2022-03-31 22:00:00+00:00 | 110.0 | 110.0 | 110.0 |
| 2022-04-30 22:00:00+00:00 | 100.0 | 110.0 | 110.0 |
| ... | ... | ... | ... |
| 2022-07-31 22:00:00+00:00 | 100.0 | 100.0 | 90.0 |
| 2022-08-31 22:00:00+00:00 | 80.0 | 110.0 | 110.0 |
| 2022-09-30 22:00:00+00:00 | 100.0 | 100.0 | 100.0 |
| 2022-10-31 23:00:00+00:00 | 100.0 | 100.0 | 100.0 |
| 2022-11-30 23:00:00+00:00 | 100.0 | 110.0 | 100.0 |

```python {.marimo}
last.nw.to_pandas()
```

<!-- @output:YECM -->

| valid_at | x | y | z |
| --- | --- | --- | --- |
| 2021-12-31 23:00:00+00:00 | 100.0 | 100.0 | 110.0 |
| 2022-01-31 23:00:00+00:00 | 100.0 | 110.0 | 100.0 |
| 2022-02-28 23:00:00+00:00 | 90.0 | 100.0 | 100.0 |
| 2022-03-31 22:00:00+00:00 | 110.0 | 110.0 | 110.0 |
| 2022-04-30 22:00:00+00:00 | 100.0 | 110.0 | 110.0 |
| ... | ... | ... | ... |
| 2022-07-31 22:00:00+00:00 | 100.0 | 100.0 | 90.0 |
| 2022-08-31 22:00:00+00:00 | 80.0 | 110.0 | 110.0 |
| 2022-09-30 22:00:00+00:00 | 100.0 | 100.0 | 100.0 |
| 2022-10-31 23:00:00+00:00 | 100.0 | 100.0 | 100.0 |
| 2022-11-30 23:00:00+00:00 | 100.0 | 110.0 | 100.0 |

```python {.marimo}
diff = last - first
diff
```

<!-- @output:cEAS -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">Dataset(name=&quot;(XYZ.subtract.XYZ)&quot;, repository=&quot;tutorials&quot;, data_type=SeriesType(Versioning.NONE,Temporality.AT), as_of_tz=&quot;2025-08-06T22:00:00+00:00&quot;)</pre>

```python {.marimo}
diff.nw.to_pandas()
```

<!-- @output:iXej -->

| valid_at | x | y | z |
| --- | --- | --- | --- |
| 2021-12-31 23:00:00+00:00 | 0.0 | 0.0 | 0.0 |
| 2022-01-31 23:00:00+00:00 | 0.0 | 0.0 | 0.0 |
| 2022-02-28 23:00:00+00:00 | 0.0 | 0.0 | 0.0 |
| 2022-03-31 22:00:00+00:00 | 0.0 | 0.0 | 0.0 |
| 2022-04-30 22:00:00+00:00 | 0.0 | 0.0 | 0.0 |
| ... | ... | ... | ... |
| 2022-07-31 22:00:00+00:00 | 0.0 | 0.0 | 0.0 |
| 2022-08-31 22:00:00+00:00 | 0.0 | 0.0 | 0.0 |
| 2022-09-30 22:00:00+00:00 | 0.0 | 0.0 | 0.0 |
| 2022-10-31 23:00:00+00:00 | 0.0 | 0.0 | 0.0 |
| 2022-11-30 23:00:00+00:00 | 0.0 | 0.0 | 0.0 |

### Example: data for periods, *with* versioning

```python {.marimo}
estimated_interval_data = SeriesType(Versioning.AS_OF, Temporality.FROM_TO)
```

```python {.marimo}
def monthly_periods(start, end):
    return create_df(
        ['tea', 'coffee'],
        ['quantity'],
        start_date=start,
        end_date=end,
        freq='M',
        temporality='FROM_TO',
        implementation='polars'
    )
```

```python {.marimo}
beverage_periods = monthly_periods('2025-01-01', '2025-06-01')
beverage_periods
```

<!-- @output:kLmu -->

| valid_from | valid_to | tea_quantity | coffee_quantity |
| --- | --- | --- | --- |
| datetime[μs, Europe/Oslo] | datetime[μs, Europe/Oslo] | f64 | f64 |
| 2025-01-01 00:00:00 CET | 2025-02-01 00:00:00 CET | 100.0 | 90.0 |
| 2025-02-01 00:00:00 CET | 2025-03-01 00:00:00 CET | 90.0 | 120.0 |
| 2025-03-01 00:00:00 CET | 2025-04-01 00:00:00 CEST | 100.0 | 110.0 |
| 2025-04-01 00:00:00 CEST | 2025-05-01 00:00:00 CEST | 100.0 | 100.0 |
| 2025-05-01 00:00:00 CEST | 2025-06-01 00:00:00 CEST | 110.0 | 100.0 |
| 2025-06-01 00:00:00 CEST | 2025-07-01 00:00:00 CEST | 110.0 | 100.0 |

```python {.marimo}
bno = Dataset(
    name = 'BNO',
    data_type = estimated_interval_data,
    data = beverage_periods,
    attributes = ['product', 'variable'],
)
```

```python {.marimo}
bno.tags
```

<!-- @output:dxZZ -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">{&#x27;name&#x27;: &#x27;BNO&#x27;,
 &#x27;repository&#x27;: &#x27;tutorials&#x27;,
 &#x27;series&#x27;: {&#x27;coffee_quantity&#x27;: {&#x27;dataset&#x27;: &#x27;BNO&#x27;,
                                &#x27;name&#x27;: &#x27;coffee_quantity&#x27;,
                                &#x27;product&#x27;: &#x27;coffee&#x27;,
                                &#x27;repository&#x27;: &#x27;tutorials&#x27;,
                                &#x27;temporality&#x27;: &#x27;FROM_TO&#x27;,
                                &#x27;variable&#x27;: &#x27;quantity&#x27;,
                                &#x27;versioning&#x27;: &#x27;AS_OF&#x27;},
            &#x27;tea_quantity&#x27;: {&#x27;dataset&#x27;: &#x27;BNO&#x27;,
                             &#x27;name&#x27;: &#x27;tea_quantity&#x27;,
                             &#x27;product&#x27;: &#x27;tea&#x27;,
                             &#x27;repository&#x27;: &#x27;tutorials&#x27;,
                             &#x27;temporality&#x27;: &#x27;FROM_TO&#x27;,
                             &#x27;variable&#x27;: &#x27;quantity&#x27;,
                             &#x27;versioning&#x27;: &#x27;AS_OF&#x27;}},
 &#x27;temporality&#x27;: &#x27;FROM_TO&#x27;,
 &#x27;versioning&#x27;: &#x27;AS_OF&#x27;}</pre>

```python {.marimo}
# the periods need two date columns, since they have a duration:
bno.data
```

<!-- @output:dlnW -->

| valid_from | valid_to | tea_quantity | coffee_quantity |
| --- | --- | --- | --- |
| datetime[ns, UTC] | datetime[ns, UTC] | f64 | f64 |
| 2024-12-31 23:00:00 UTC | 2025-01-31 23:00:00 UTC | 100.0 | 90.0 |
| 2025-01-31 23:00:00 UTC | 2025-02-28 23:00:00 UTC | 90.0 | 120.0 |
| 2025-02-28 23:00:00 UTC | 2025-03-31 22:00:00 UTC | 100.0 | 110.0 |
| 2025-03-31 22:00:00 UTC | 2025-04-30 22:00:00 UTC | 100.0 | 100.0 |
| 2025-04-30 22:00:00 UTC | 2025-05-31 22:00:00 UTC | 110.0 | 100.0 |
| 2025-05-31 22:00:00 UTC | 2025-06-30 22:00:00 UTC | 110.0 | 100.0 |

```python {.marimo}
# every production run writes a new file, named after the as of date
for _as_of in ['2025-06-01', '2025-08-07']:
    Dataset(
        name = 'BNO',
        data_type = estimated_interval_data,
        as_of_tz=date_utc(_as_of),
        data = beverage_periods,
    ).save()
```

```python {.marimo}
# versioning and temporality together decide the folder layout:
print(tree(f'{data_path}/AS_OF_FROM_TO'))
```

<!-- @output:RKFZ -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">AS_OF_FROM_TO/
├── BNO/
│   ├── BNO-as_of_2025-05-31T220000+0000-data.parquet
│   └── BNO-as_of_2025-08-06T220000+0000-data.parquet
└── Prices and Volumes/
    ├── Prices and Volumes-as_of_2023-12-31T230000+0000-data.parquet
    ├── Prices and Volumes-as_of_2024-01-31T230000+0000-data.parquet
    ├── Prices and Volumes-as_of_2025-10-31T230000+0000-data.parquet
    ├── Prices and Volumes-as_of_2025-11-30T230000+0000-data.parquet
    └── ...

</pre>

```python {.marimo}
bno_june = Dataset('BNO', as_of_tz='2025-06-01')
```

```python {.marimo}
bno_june.nw.to_pandas()
```

<!-- @output:IWgg -->

| valid_from | valid_to | coffee_quantity | tea_quantity |
| --- | --- | --- | --- |
| 2024-12-31 23:00:00+00:00 | 2025-01-31 23:00:00+00:00 | 90.0 | 100.0 |
| 2025-01-31 23:00:00+00:00 | 2025-02-28 23:00:00+00:00 | 120.0 | 90.0 |
| 2025-02-28 23:00:00+00:00 | 2025-03-31 22:00:00+00:00 | 110.0 | 100.0 |
| 2025-03-31 22:00:00+00:00 | 2025-04-30 22:00:00+00:00 | 100.0 | 100.0 |
| 2025-04-30 22:00:00+00:00 | 2025-05-31 22:00:00+00:00 | 100.0 | 110.0 |
| 2025-05-31 22:00:00+00:00 | 2025-06-30 22:00:00+00:00 | 100.0 | 110.0 |

```python {.marimo}
# the as of date is not part of the data, it identifies the version:
'as_of' in bno_june.nw.columns
```

<!-- @output:fCoF -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">False</pre>

```python {.marimo}
from ssb_timeseries.io import versions

# ... it is the version marker of the file the data is read from:
versions(bno_june)
```

<!-- @output:LkGn -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">&#91;&#x27;2025-05-31 22:00:00+00:00&#x27;, &#x27;2025-08-06 22:00:00+00:00&#x27;&#93;</pre>

### What the series type decides

| series type | date columns in `.data` | stored as |
|---|---|---|
| `SeriesType(NONE, AT)` | `valid_at` | one file, `NONE_AT/<name>/<name>-latest-data.parquet` |
| `SeriesType(NONE, FROM_TO)` | `valid_from`, `valid_to` | one file, `NONE_FROM_TO/<name>/<name>-latest-data.parquet` |
| `SeriesType(AS_OF, AT)` | `valid_at` | one file per version, `AS_OF_AT/<name>/<name>-as_of_<timestamp>-data.parquet` |
| `SeriesType(AS_OF, FROM_TO)` | `valid_from`, `valid_to` | one file per version, `AS_OF_FROM_TO/<name>/<name>-as_of_<timestamp>-data.parquet` |

Without versioning, a save merges the new data into the single existing file, as seen above with PQR.
With `AS_OF`, a save never overwrites: it adds a file, and the `as_of` column it writes there is a storage detail that `.data` does not expose.
