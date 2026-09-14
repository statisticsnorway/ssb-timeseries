---
title: Meta Tag Maintenance
marimo-version: 0.24.0
---

```python {.marimo}

```

```python {.marimo}

```

# Meta data
<!---->
## Setup

```python {.marimo}
from datetime import timedelta

from ssb_timeseries.sample_data import create_df,date_ranges
from ssb_timeseries.dates import ensure_datetime, date_utc
```

```python {.marimo}
import polars as pl
from datetime import datetime
```

```python {.marimo}
from ssb_timeseries.types import SeriesType, Versioning, Temporality
```

```python {.marimo}
from ssb_timeseries.dataset import Dataset
```

Manually tagging set and series
-------------------------------

```python {.marimo}

```

```python {.marimo}
def some_simple_data_from_file_or_query(
    start='2020-01-01',
    end='2025-06-01',
):
    """create some sample data"""
    return create_df(
        ['p','q','r'],
        start_date=start,
        end_date=end,
        freq='D'
    )

pqr_df = some_simple_data_from_file_or_query()
```

```python {.marimo}
pqr = Dataset(
    name = 'PQR',
    data_type = SeriesType('NONE', 'AT'),
    data = pqr_df,
)
```

```python {.marimo}
pqr.tags
```

<!-- @output:nWHF -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">{&#x27;name&#x27;: &#x27;PQR&#x27;,
 &#x27;repository&#x27;: &#x27;tutorials&#x27;,
 &#x27;series&#x27;: {&#x27;p&#x27;: {&#x27;dataset&#x27;: &#x27;PQR&#x27;,
                  &#x27;name&#x27;: &#x27;p&#x27;,
                  &#x27;repository&#x27;: &#x27;tutorials&#x27;,
                  &#x27;temporality&#x27;: &#x27;AT&#x27;,
                  &#x27;vare&#x27;: &#x27;kaffe&#x27;,
                  &#x27;varegruppe&#x27;: &#x27;nødvendigheter&#x27;,
                  &#x27;variabel&#x27;: &#x27;pris&#x27;,
                  &#x27;versioning&#x27;: &#x27;NONE&#x27;},
            &#x27;q&#x27;: {&#x27;dataset&#x27;: &#x27;PQR&#x27;,
                  &#x27;name&#x27;: &#x27;q&#x27;,
                  &#x27;repository&#x27;: &#x27;tutorials&#x27;,
                  &#x27;temporality&#x27;: &#x27;AT&#x27;,
                  &#x27;vare&#x27;: &#x27;knekkebrød&#x27;,
                  &#x27;varegruppe&#x27;: &#x27;nødvendigheter&#x27;,
                  &#x27;variabel&#x27;: &#x27;pris&#x27;,
                  &#x27;versioning&#x27;: &#x27;NONE&#x27;},
            &#x27;r&#x27;: {&#x27;dataset&#x27;: &#x27;PQR&#x27;,
                  &#x27;name&#x27;: &#x27;r&#x27;,
                  &#x27;repository&#x27;: &#x27;tutorials&#x27;,
                  &#x27;temporality&#x27;: &#x27;AT&#x27;,
                  &#x27;vare&#x27;: &#x27;brunost&#x27;,
                  &#x27;varegruppe&#x27;: &#x27;nødvendigheter&#x27;,
                  &#x27;variabel&#x27;: &#x27;pris&#x27;,
                  &#x27;versioning&#x27;: &#x27;NONE&#x27;}},
 &#x27;temporality&#x27;: &#x27;AT&#x27;,
 &#x27;varegruppe&#x27;: &#x27;nødvendigheter&#x27;,
 &#x27;variabel&#x27;: &#x27;pris&#x27;,
 &#x27;versioning&#x27;: &#x27;NONE&#x27;}</pre>

```python {.marimo}
pqr.tag_dataset(tags={'variabel': 'pris','varegruppe': 'nødvendigheter'})

pqr.tag_series('p',tags={'vare': 'kaffe'})
pqr.tag_series('q',tags={'vare': 'knekkebrød'})
pqr.tag_series('r',tags={'vare': 'brunost'})

pqr.tags
```

<!-- @output:iLit -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">{&#x27;name&#x27;: &#x27;PQR&#x27;,
 &#x27;repository&#x27;: &#x27;tutorials&#x27;,
 &#x27;series&#x27;: {&#x27;p&#x27;: {&#x27;dataset&#x27;: &#x27;PQR&#x27;,
                  &#x27;name&#x27;: &#x27;p&#x27;,
                  &#x27;repository&#x27;: &#x27;tutorials&#x27;,
                  &#x27;temporality&#x27;: &#x27;AT&#x27;,
                  &#x27;vare&#x27;: &#x27;kaffe&#x27;,
                  &#x27;varegruppe&#x27;: &#x27;nødvendigheter&#x27;,
                  &#x27;variabel&#x27;: &#x27;pris&#x27;,
                  &#x27;versioning&#x27;: &#x27;NONE&#x27;},
            &#x27;q&#x27;: {&#x27;dataset&#x27;: &#x27;PQR&#x27;,
                  &#x27;name&#x27;: &#x27;q&#x27;,
                  &#x27;repository&#x27;: &#x27;tutorials&#x27;,
                  &#x27;temporality&#x27;: &#x27;AT&#x27;,
                  &#x27;vare&#x27;: &#x27;knekkebrød&#x27;,
                  &#x27;varegruppe&#x27;: &#x27;nødvendigheter&#x27;,
                  &#x27;variabel&#x27;: &#x27;pris&#x27;,
                  &#x27;versioning&#x27;: &#x27;NONE&#x27;},
            &#x27;r&#x27;: {&#x27;dataset&#x27;: &#x27;PQR&#x27;,
                  &#x27;name&#x27;: &#x27;r&#x27;,
                  &#x27;repository&#x27;: &#x27;tutorials&#x27;,
                  &#x27;temporality&#x27;: &#x27;AT&#x27;,
                  &#x27;vare&#x27;: &#x27;brunost&#x27;,
                  &#x27;varegruppe&#x27;: &#x27;nødvendigheter&#x27;,
                  &#x27;variabel&#x27;: &#x27;pris&#x27;,
                  &#x27;versioning&#x27;: &#x27;NONE&#x27;}},
 &#x27;temporality&#x27;: &#x27;AT&#x27;,
 &#x27;varegruppe&#x27;: &#x27;nødvendigheter&#x27;,
 &#x27;variabel&#x27;: &#x27;pris&#x27;,
 &#x27;versioning&#x27;: &#x27;NONE&#x27;}</pre>

```python {.marimo}
pqr.save()
```

```python {.marimo}
# reading the data back:
x = Dataset('PQR')
```

Autotagging
-----------

```python {.marimo}
interval_data = SeriesType('NONE', 'FROM_TO')
```

```python {.marimo}
def mock_interval_data_from_file_or_query(start, end):
    a_to_z = [chr(i) for i in range(ord('a'), ord('z') + 1)]
    variables = ['antall', 'pris']
    goods = ['kaffe', 'te', 'brus', 'øl', 'vin']
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
```

```python {.marimo}
bigger_data
```

<!-- @output:ulZA -->

| valid_from | valid_to | a_antall_kaffe | a_antall_te | a_antall_brus | a_antall_øl | a_antall_vin | a_pris_kaffe | a_pris_te | a_pris_brus | a_pris_øl | a_pris_vin | b_antall_kaffe | b_antall_te | b_antall_brus | b_antall_øl | b_antall_vin | b_pris_kaffe | b_pris_te | b_pris_brus | b_pris_øl | b_pris_vin | c_antall_kaffe | c_antall_te | c_antall_brus | c_antall_øl | c_antall_vin | c_pris_kaffe | c_pris_te | c_pris_brus | c_pris_øl | c_pris_vin | d_antall_kaffe | d_antall_te | d_antall_brus | d_antall_øl | d_antall_vin | … | w_antall_øl | w_antall_vin | w_pris_kaffe | w_pris_te | w_pris_brus | w_pris_øl | w_pris_vin | x_antall_kaffe | x_antall_te | x_antall_brus | x_antall_øl | x_antall_vin | x_pris_kaffe | x_pris_te | x_pris_brus | x_pris_øl | x_pris_vin | y_antall_kaffe | y_antall_te | y_antall_brus | y_antall_øl | y_antall_vin | y_pris_kaffe | y_pris_te | y_pris_brus | y_pris_øl | y_pris_vin | z_antall_kaffe | z_antall_te | z_antall_brus | z_antall_øl | z_antall_vin | z_pris_kaffe | z_pris_te | z_pris_brus | z_pris_øl | z_pris_vin |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| datetime[μs] | datetime[μs] | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | … | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 |
| 2025-01-01 00:00:00 | 2025-02-01 00:00:00 | 100.0 | 90.0 | 120.0 | 90.0 | 90.0 | 90.0 | 110.0 | 100.0 | 110.0 | 110.0 | 110.0 | 90.0 | 100.0 | 100.0 | 120.0 | 80.0 | 110.0 | 90.0 | 90.0 | 100.0 | 100.0 | 100.0 | 100.0 | 100.0 | 120.0 | 80.0 | 100.0 | 110.0 | 100.0 | 90.0 | 90.0 | 100.0 | 80.0 | 90.0 | 100.0 | … | 110.0 | 110.0 | 90.0 | 120.0 | 100.0 | 120.0 | 90.0 | 100.0 | 100.0 | 110.0 | 110.0 | 90.0 | 90.0 | 110.0 | 80.0 | 90.0 | 80.0 | 100.0 | 100.0 | 110.0 | 90.0 | 100.0 | 110.0 | 90.0 | 110.0 | 100.0 | 90.0 | 100.0 | 90.0 | 110.0 | 100.0 | 100.0 | 120.0 | 90.0 | 90.0 | 90.0 | 110.0 |
| 2025-02-01 00:00:00 | 2025-03-01 00:00:00 | 110.0 | 110.0 | 100.0 | 110.0 | 80.0 | 100.0 | 120.0 | 90.0 | 100.0 | 100.0 | 100.0 | 90.0 | 100.0 | 90.0 | 90.0 | 100.0 | 90.0 | 100.0 | 80.0 | 90.0 | 90.0 | 80.0 | 100.0 | 100.0 | 100.0 | 100.0 | 100.0 | 100.0 | 90.0 | 110.0 | 80.0 | 90.0 | 110.0 | 90.0 | 120.0 | … | 90.0 | 100.0 | 110.0 | 90.0 | 90.0 | 110.0 | 100.0 | 90.0 | 100.0 | 100.0 | 100.0 | 80.0 | 90.0 | 100.0 | 100.0 | 100.0 | 90.0 | 100.0 | 110.0 | 100.0 | 90.0 | 100.0 | 100.0 | 80.0 | 110.0 | 100.0 | 120.0 | 110.0 | 100.0 | 100.0 | 90.0 | 110.0 | 90.0 | 100.0 | 100.0 | 100.0 | 100.0 |
| 2025-03-01 00:00:00 | 2025-04-01 00:00:00 | 120.0 | 110.0 | 100.0 | 100.0 | 100.0 | 100.0 | 110.0 | 110.0 | 80.0 | 100.0 | 100.0 | 110.0 | 120.0 | 90.0 | 100.0 | 110.0 | 100.0 | 110.0 | 110.0 | 100.0 | 100.0 | 100.0 | 110.0 | 100.0 | 100.0 | 100.0 | 90.0 | 110.0 | 110.0 | 90.0 | 90.0 | 100.0 | 90.0 | 120.0 | 110.0 | … | 100.0 | 110.0 | 100.0 | 110.0 | 70.0 | 90.0 | 110.0 | 80.0 | 90.0 | 90.0 | 100.0 | 90.0 | 100.0 | 100.0 | 100.0 | 100.0 | 100.0 | 100.0 | 90.0 | 100.0 | 90.0 | 100.0 | 120.0 | 90.0 | 90.0 | 110.0 | 80.0 | 100.0 | 110.0 | 90.0 | 90.0 | 100.0 | 110.0 | 120.0 | 80.0 | 100.0 | 90.0 |
| 2025-04-01 00:00:00 | 2025-05-01 00:00:00 | 100.0 | 100.0 | 100.0 | 100.0 | 110.0 | 90.0 | 90.0 | 100.0 | 100.0 | 100.0 | 80.0 | 110.0 | 100.0 | 90.0 | 110.0 | 90.0 | 100.0 | 100.0 | 100.0 | 100.0 | 80.0 | 100.0 | 100.0 | 100.0 | 90.0 | 100.0 | 100.0 | 90.0 | 110.0 | 100.0 | 100.0 | 110.0 | 100.0 | 90.0 | 90.0 | … | 110.0 | 100.0 | 120.0 | 100.0 | 90.0 | 100.0 | 100.0 | 110.0 | 100.0 | 100.0 | 110.0 | 70.0 | 100.0 | 100.0 | 100.0 | 110.0 | 90.0 | 110.0 | 120.0 | 90.0 | 80.0 | 80.0 | 100.0 | 110.0 | 100.0 | 90.0 | 90.0 | 100.0 | 90.0 | 90.0 | 110.0 | 100.0 | 110.0 | 100.0 | 100.0 | 110.0 | 110.0 |
| 2025-05-01 00:00:00 | 2025-06-01 00:00:00 | 90.0 | 90.0 | 90.0 | 100.0 | 110.0 | 100.0 | 80.0 | 100.0 | 90.0 | 100.0 | 80.0 | 110.0 | 110.0 | 90.0 | 100.0 | 90.0 | 100.0 | 100.0 | 100.0 | 100.0 | 100.0 | 100.0 | 120.0 | 110.0 | 100.0 | 110.0 | 120.0 | 110.0 | 90.0 | 110.0 | 90.0 | 100.0 | 80.0 | 110.0 | 90.0 | … | 100.0 | 100.0 | 100.0 | 100.0 | 110.0 | 90.0 | 80.0 | 100.0 | 110.0 | 90.0 | 100.0 | 90.0 | 90.0 | 90.0 | 100.0 | 80.0 | 100.0 | 100.0 | 100.0 | 90.0 | 90.0 | 90.0 | 80.0 | 80.0 | 90.0 | 120.0 | 100.0 | 120.0 | 120.0 | 90.0 | 100.0 | 120.0 | 110.0 | 90.0 | 100.0 | 110.0 | 90.0 |
| 2025-06-01 00:00:00 | 2025-07-01 00:00:00 | 90.0 | 100.0 | 100.0 | 110.0 | 100.0 | 100.0 | 80.0 | 130.0 | 100.0 | 90.0 | 100.0 | 110.0 | 120.0 | 110.0 | 120.0 | 100.0 | 100.0 | 80.0 | 80.0 | 100.0 | 130.0 | 80.0 | 100.0 | 110.0 | 110.0 | 90.0 | 100.0 | 100.0 | 110.0 | 100.0 | 100.0 | 100.0 | 100.0 | 80.0 | 110.0 | … | 90.0 | 110.0 | 100.0 | 100.0 | 100.0 | 100.0 | 90.0 | 100.0 | 100.0 | 100.0 | 100.0 | 110.0 | 90.0 | 100.0 | 110.0 | 110.0 | 100.0 | 80.0 | 110.0 | 80.0 | 110.0 | 90.0 | 110.0 | 100.0 | 110.0 | 100.0 | 100.0 | 100.0 | 110.0 | 100.0 | 90.0 | 110.0 | 110.0 | 100.0 | 110.0 | 100.0 | 100.0 |

```python {.marimo}
az = Dataset(
    name = 'AZ_drikkevarer',
    data_type = interval_data,
    data = bigger_data,
    attributes=['butikk','variabel','vare'], # <-- this is the clever part
)
```

```python {.marimo}
az.save()
```

Updating tags after calculations
--------------------------------

```python {.marimo}
priser = Dataset('AZ_drikkevarer')[{'variabel':'pris'}]
antall = Dataset('AZ_drikkevarer')[{'variabel':'antall'}]
omsetning = (priser * antall)
print(omsetning.name)
type(omsetning)
```

<!-- @output:aLJB -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">(COPY of(AZ_drikkevarer SELECTED by names (), pattern: , regex:  tags: &#91;{&#x27;variabel&#x27;: &#x27;pris&#x27;}&#93;).multiply.COPY of(AZ_drikkevarer SELECTED by names (), pattern: , regex:  tags: &#91;{&#x27;variabel&#x27;: &#x27;antall&#x27;}&#93;))
</pre>

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">&lt;class &#x27;ssb_timeseries.dataset.Dataset&#x27;&gt;</pre>

```python {.marimo}
omsetning.nw.to_pandas()
```

<!-- @output:AjVT -->

| valid_from | valid_to | a_omsetning_brus | a_omsetning_kaffe | a_omsetning_te | a_omsetning_vin | a_omsetning_øl | b_omsetning_brus | b_omsetning_kaffe | b_omsetning_te | ... | y_omsetning_brus | y_omsetning_kaffe | y_omsetning_te | y_omsetning_vin | y_omsetning_øl | z_omsetning_brus | z_omsetning_kaffe | z_omsetning_te | z_omsetning_vin | z_omsetning_øl |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2024-12-31 23:00:00+00:00 | 2025-01-31 23:00:00+00:00 | 12000.0 | 9000.0 | 9900.0 | 9900.0 | 9900.0 | 9000.0 | 8800.0 | 9900.0 | ... | 12100.0 | 11000.0 | 9000.0 | 9000.0 | 9000.0 | 9900.0 | 12000.0 | 8100.0 | 11000.0 | 9000.0 |
| 2025-01-31 23:00:00+00:00 | 2025-02-28 23:00:00+00:00 | 9000.0 | 11000.0 | 13200.0 | 8000.0 | 11000.0 | 10000.0 | 10000.0 | 8100.0 | ... | 11000.0 | 10000.0 | 8800.0 | 12000.0 | 9000.0 | 10000.0 | 9900.0 | 10000.0 | 11000.0 | 9000.0 |
| 2025-02-28 23:00:00+00:00 | 2025-03-31 22:00:00+00:00 | 11000.0 | 12000.0 | 12100.0 | 10000.0 | 8000.0 | 13200.0 | 11000.0 | 11000.0 | ... | 9000.0 | 12000.0 | 8100.0 | 8000.0 | 9900.0 | 7200.0 | 11000.0 | 13200.0 | 9000.0 | 9000.0 |
| 2025-03-31 22:00:00+00:00 | 2025-04-30 22:00:00+00:00 | 10000.0 | 9000.0 | 9000.0 | 11000.0 | 10000.0 | 10000.0 | 7200.0 | 11000.0 | ... | 9000.0 | 11000.0 | 13200.0 | 7200.0 | 7200.0 | 9000.0 | 11000.0 | 9000.0 | 11000.0 | 12100.0 |
| 2025-04-30 22:00:00+00:00 | 2025-05-31 22:00:00+00:00 | 9000.0 | 9000.0 | 7200.0 | 11000.0 | 9000.0 | 11000.0 | 7200.0 | 11000.0 | ... | 8100.0 | 8000.0 | 8000.0 | 9000.0 | 10800.0 | 9000.0 | 13200.0 | 10800.0 | 10800.0 | 11000.0 |
| 2025-05-31 22:00:00+00:00 | 2025-06-30 22:00:00+00:00 | 13000.0 | 9000.0 | 8000.0 | 9000.0 | 11000.0 | 9600.0 | 10000.0 | 11000.0 | ... | 8800.0 | 8800.0 | 11000.0 | 9000.0 | 11000.0 | 11000.0 | 11000.0 | 11000.0 | 11000.0 | 9000.0 |

<!-- @output:AjVT -->

| valid_from | valid_to | a_omsetning_brus | a_omsetning_kaffe | a_omsetning_te | a_omsetning_vin | a_omsetning_øl | b_omsetning_brus | b_omsetning_kaffe | b_omsetning_te | ... | y_omsetning_brus | y_omsetning_kaffe | y_omsetning_te | y_omsetning_vin | y_omsetning_øl | z_omsetning_brus | z_omsetning_kaffe | z_omsetning_te | z_omsetning_vin | z_omsetning_øl |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2024-12-31 23:00:00+00:00 | 2025-01-31 23:00:00+00:00 | 12000.0 | 9000.0 | 9900.0 | 9900.0 | 9900.0 | 9000.0 | 8800.0 | 9900.0 | ... | 12100.0 | 11000.0 | 9000.0 | 9000.0 | 9000.0 | 9900.0 | 12000.0 | 8100.0 | 11000.0 | 9000.0 |
| 2025-01-31 23:00:00+00:00 | 2025-02-28 23:00:00+00:00 | 9000.0 | 11000.0 | 13200.0 | 8000.0 | 11000.0 | 10000.0 | 10000.0 | 8100.0 | ... | 11000.0 | 10000.0 | 8800.0 | 12000.0 | 9000.0 | 10000.0 | 9900.0 | 10000.0 | 11000.0 | 9000.0 |
| 2025-02-28 23:00:00+00:00 | 2025-03-31 22:00:00+00:00 | 11000.0 | 12000.0 | 12100.0 | 10000.0 | 8000.0 | 13200.0 | 11000.0 | 11000.0 | ... | 9000.0 | 12000.0 | 8100.0 | 8000.0 | 9900.0 | 7200.0 | 11000.0 | 13200.0 | 9000.0 | 9000.0 |
| 2025-03-31 22:00:00+00:00 | 2025-04-30 22:00:00+00:00 | 10000.0 | 9000.0 | 9000.0 | 11000.0 | 10000.0 | 10000.0 | 7200.0 | 11000.0 | ... | 9000.0 | 11000.0 | 13200.0 | 7200.0 | 7200.0 | 9000.0 | 11000.0 | 9000.0 | 11000.0 | 12100.0 |
| 2025-04-30 22:00:00+00:00 | 2025-05-31 22:00:00+00:00 | 9000.0 | 9000.0 | 7200.0 | 11000.0 | 9000.0 | 11000.0 | 7200.0 | 11000.0 | ... | 8100.0 | 8000.0 | 8000.0 | 9000.0 | 10800.0 | 9000.0 | 13200.0 | 10800.0 | 10800.0 | 11000.0 |
| 2025-05-31 22:00:00+00:00 | 2025-06-30 22:00:00+00:00 | 13000.0 | 9000.0 | 8000.0 | 9000.0 | 11000.0 | 9600.0 | 10000.0 | 11000.0 | ... | 8800.0 | 8800.0 | 11000.0 | 9000.0 | 11000.0 | 11000.0 | 11000.0 | 11000.0 | 11000.0 | 9000.0 |

```python {.marimo}
omsetning.rename('AZ_omsetning', ('pris', 'omsetning'))
print(omsetning)
```

<!-- @output:xXTn -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">{&#x27;name&#x27;: &#x27;AZ_omsetning&#x27;, &#x27;data_type&#x27;: &#x27;NONE_FROM_TO&#x27;, &#x27;as_of_utc&#x27;: None, &#x27;repository&#x27;: &#x27;tutorials&#x27;, &#x27;series&#x27;: &quot;&#91;&#x27;a_omsetning_brus&#x27;, &#x27;a_omsetning_kaffe&#x27;, &#x27;a_omsetning_te&#x27;, &#x27;a_omsetning_vin&#x27;, &#x27;a_omsetning_øl&#x27;, &#x27;b_omsetning_brus&#x27;, &#x27;b_omsetning_kaffe&#x27;, &#x27;b_omsetning_te&#x27;, &#x27;b_omsetning_vin&#x27;, &#x27;b_omsetning_øl&#x27;, &#x27;c_omsetning_brus&#x27;, &#x27;c_omsetning_kaffe&#x27;, &#x27;c_omsetning_te&#x27;, &#x27;c_omsetning_vin&#x27;, &#x27;c_omsetning_øl&#x27;, &#x27;d_omsetning_brus&#x27;, &#x27;d_omsetning_kaffe&#x27;, &#x27;d_omsetning_te&#x27;, &#x27;d_omsetning_vin&#x27;, &#x27;d_omsetning_øl&#x27;, &#x27;e_omsetning_brus&#x27;, &#x27;e_omsetning_kaffe&#x27;, &#x27;e_omsetning_te&#x27;, &#x27;e_omsetning_vin&#x27;, &#x27;e_omsetning_øl&#x27;, &#x27;f_omsetning_brus&#x27;, &#x27;f_omsetning_kaffe&#x27;, &#x27;f_omsetning_te&#x27;, &#x27;f_omsetning_vin&#x27;, &#x27;f_omsetning_øl&#x27;, &#x27;g_omsetning_brus&#x27;, &#x27;g_omsetning_kaffe&#x27;, &#x27;g_omsetning_te&#x27;, &#x27;g_omsetning_vin&#x27;, &#x27;g_omsetning_øl&#x27;, &#x27;h_omsetning_brus&#x27;, &#x27;h_omsetning_kaffe&#x27;, &#x27;h_omsetning_te&#x27;, &#x27;h_omsetning_vin&#x27;, &#x27;h_omsetning_øl&#x27;, &#x27;i_omsetning_brus&#x27;, &#x27;i_omsetning_kaffe&#x27;, &#x27;i_omsetning_te&#x27;, &#x27;i_omsetning_vin&#x27;, &#x27;i_omsetning_øl&#x27;, &#x27;j_omsetning_brus&#x27;, &#x27;j_omsetning_kaffe&#x27;, &#x27;j_omsetning_te&#x27;, &#x27;j_omsetning_vin&#x27;, &#x27;j_omsetning_øl&#x27;, &#x27;k_omsetning_brus&#x27;, &#x27;k_omsetning_kaffe&#x27;, &#x27;k_omsetning_te&#x27;, &#x27;k_omsetning_vin&#x27;, &#x27;k_omsetning_øl&#x27;, &#x27;l_omsetning_brus&#x27;, &#x27;l_omsetning_kaffe&#x27;, &#x27;l_omsetning_te&#x27;, &#x27;l_omsetning_vin&#x27;, &#x27;l_omsetning_øl&#x27;, &#x27;m_omsetning_brus&#x27;, &#x27;m_omsetning_kaffe&#x27;, &#x27;m_omsetning_te&#x27;, &#x27;m_omsetning_vin&#x27;, &#x27;m_omsetning_øl&#x27;, &#x27;n_omsetning_brus&#x27;, &#x27;n_omsetning_kaffe&#x27;, &#x27;n_omsetning_te&#x27;, &#x27;n_omsetning_vin&#x27;, &#x27;n_omsetning_øl&#x27;, &#x27;o_omsetning_brus&#x27;, &#x27;o_omsetning_kaffe&#x27;, &#x27;o_omsetning_te&#x27;, &#x27;o_omsetning_vin&#x27;, &#x27;o_omsetning_øl&#x27;, &#x27;p_omsetning_brus&#x27;, &#x27;p_omsetning_kaffe&#x27;, &#x27;p_omsetning_te&#x27;, &#x27;p_omsetning_vin&#x27;, &#x27;p_omsetning_øl&#x27;, &#x27;q_omsetning_brus&#x27;, &#x27;q_omsetning_kaffe&#x27;, &#x27;q_omsetning_te&#x27;, &#x27;q_omsetning_vin&#x27;, &#x27;q_omsetning_øl&#x27;, &#x27;r_omsetning_brus&#x27;, &#x27;r_omsetning_kaffe&#x27;, &#x27;r_omsetning_te&#x27;, &#x27;r_omsetning_vin&#x27;, &#x27;r_omsetning_øl&#x27;, &#x27;s_omsetning_brus&#x27;, &#x27;s_omsetning_kaffe&#x27;, &#x27;s_omsetning_te&#x27;, &#x27;s_omsetning_vin&#x27;, &#x27;s_omsetning_øl&#x27;, &#x27;t_omsetning_brus&#x27;, &#x27;t_omsetning_kaffe&#x27;, &#x27;t_omsetning_te&#x27;, &#x27;t_omsetning_vin&#x27;, &#x27;t_omsetning_øl&#x27;, &#x27;u_omsetning_brus&#x27;, &#x27;u_omsetning_kaffe&#x27;, &#x27;u_omsetning_te&#x27;, &#x27;u_omsetning_vin&#x27;, &#x27;u_omsetning_øl&#x27;, &#x27;v_omsetning_brus&#x27;, &#x27;v_omsetning_kaffe&#x27;, &#x27;v_omsetning_te&#x27;, &#x27;v_omsetning_vin&#x27;, &#x27;v_omsetning_øl&#x27;, &#x27;w_omsetning_brus&#x27;, &#x27;w_omsetning_kaffe&#x27;, &#x27;w_omsetning_te&#x27;, &#x27;w_omsetning_vin&#x27;, &#x27;w_omsetning_øl&#x27;, &#x27;x_omsetning_brus&#x27;, &#x27;x_omsetning_kaffe&#x27;, &#x27;x_omsetning_te&#x27;, &#x27;x_omsetning_vin&#x27;, &#x27;x_omsetning_øl&#x27;, &#x27;y_omsetning_brus&#x27;, &#x27;y_omsetning_kaffe&#x27;, &#x27;y_omsetning_te&#x27;, &#x27;y_omsetning_vin&#x27;, &#x27;y_omsetning_øl&#x27;, &#x27;z_omsetning_brus&#x27;, &#x27;z_omsetning_kaffe&#x27;, &#x27;z_omsetning_te&#x27;, &#x27;z_omsetning_vin&#x27;, &#x27;z_omsetning_øl&#x27;&#93;&quot;, &#x27;data&#x27;: (6, 132)}
</pre>

```python {.marimo}
omsetning.nw.to_pandas()
```

```python {.marimo}
# DEBUG: tags are lost in selects above, hence not flowing through
omsetning.tags["series"]["a_omsetning_brus"]
```

<!-- @output:pHFh -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">{&#x27;butikk&#x27;: &#x27;a&#x27;,
 &#x27;dataset&#x27;: &#x27;AZ_omsetning&#x27;,
 &#x27;name&#x27;: &#x27;a_omsetning_brus&#x27;,
 &#x27;repository&#x27;: &#x27;tutorials&#x27;,
 &#x27;temporality&#x27;: &#x27;FROM_TO&#x27;,
 &#x27;vare&#x27;: &#x27;brus&#x27;,
 &#x27;variabel&#x27;: &#x27;pris&#x27;,
 &#x27;versioning&#x27;: &#x27;NONE&#x27;}</pre>

```python {.marimo}
# ... tag maintenance is likely to be necessary after calculations:
omsetning.replace_tags(({'variabel':'pris'},{'variabel':'omsetning'}))
omsetning.tags["series"]["a_omsetning_brus"]
```

<!-- @output:NCOB -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">{&#x27;butikk&#x27;: &#x27;a&#x27;,
 &#x27;dataset&#x27;: &#x27;AZ_omsetning&#x27;,
 &#x27;name&#x27;: &#x27;a_omsetning_brus&#x27;,
 &#x27;repository&#x27;: &#x27;tutorials&#x27;,
 &#x27;temporality&#x27;: &#x27;FROM_TO&#x27;,
 &#x27;vare&#x27;: &#x27;brus&#x27;,
 &#x27;variabel&#x27;: &#x27;omsetning&#x27;,
 &#x27;versioning&#x27;: &#x27;NONE&#x27;}</pre>

```python {.marimo}
# review the data
display(priser.data)
display(antall.data)
display(omsetning.nw.to_pandas())
```

<!-- @output:aqbW -->

<pre class="stderr" style="white-space: pre-wrap; overflow-wrap: break-word;">Traceback (most recent call last):
  File &quot;/home/bernhard/code/ssb-timeseries/.nox/docs/tmp/marimo_140685/__marimo__cell_aqbW_.py&quot;, line 2, in
    display(priser.data)
    ^^^^^^^
NameError: name &#x27;display&#x27; is not defined

</pre>

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">exception: name &#x27;display&#x27; is not defined</pre>

Detagging
---------

```python {.marimo}
estimated_point_in_time = SeriesType('AS_OF', 'AT')
```

```python {.marimo}
tree(repository)
```

<!-- @output:dNNg -->

<pre class="stderr" style="white-space: pre-wrap; overflow-wrap: break-word;">Traceback (most recent call last):
  File &quot;/home/bernhard/code/ssb-timeseries/.nox/docs/tmp/marimo_140685/__marimo__cell_dNNg_.py&quot;, line 1, in
    tree(repository)
    ^^^^
NameError: name &#x27;tree&#x27; is not defined. Did you mean: &#x27;True&#x27;?

</pre>

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">exception: name &#x27;tree&#x27; is not defined. Did you mean: &#x27;True&#x27;?</pre>

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

<!-- @output:wlCL -->

| valid_at | x | y | z |
| --- | --- | --- | --- |
| 2024-03-08 | 90.0 | 110.0 | 120.0 |
| 2024-03-09 | 120.0 | 110.0 | 80.0 |
| 2024-03-10 | 90.0 | 100.0 | 100.0 |
| 2024-03-11 | 100.0 | 100.0 | 100.0 |
| 2024-03-12 | 110.0 | 90.0 | 100.0 |
| 2024-03-13 | 90.0 | 110.0 | 100.0 |
| 2024-03-14 | 90.0 | 110.0 | 100.0 |

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

The catalog
------------------------

Allows inspection and analysis of tags across all sets and series.
Actual maintenance via `Dataset` methods.

```python {.marimo}
from ssb_timeseries import get_catalog
```

```python {.marimo}
our_timeseries_database = get_catalog()
```

```python {.marimo}
all_sets = our_timeseries_database.datasets()
[s.object_name for s in all_sets]
```

<!-- @output:lgWD -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">&#91;&#x27;AZ_drikkevarer&#x27;,
 &#x27;Prices and Volumes&#x27;,
 &#x27;A Sample Dataset&#x27;,
 &#x27;PQR&#x27;,
 &#x27;Sample Data&#x27;,
 &#x27;XYZ&#x27;,
 &#x27;More Prices and Volumes&#x27;,
 &#x27;AZ_omsetning&#x27;,
 &#x27;AZ_drinks&#x27;&#93;</pre>

```python {.marimo}
series_in_xyz = our_timeseries_database.series(tags={'dataset': 'XYZ'})
[s.object_name for s in series_in_xyz]
```

<!-- @output:yOPj -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">&#91;&#x27;x&#x27;, &#x27;y&#x27;, &#x27;z&#x27;&#93;</pre>

```python {.marimo}
import pandas as pd
everything = our_timeseries_database.items()
pd.DataFrame(everything)
```

<!-- @output:fwwy -->

| repository_name | object_name | object_type | object_tags | parent | children |
| --- | --- | --- | --- | --- | --- |
| tutorials | AZ_drikkevarer | dataset | {'name': 'AZ_drikkevarer', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'series': {'a_antall_brus': {'dataset': 'AZ_drikkevarer', 'name': 'a_antall_brus', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'a', 'variabel': 'antall', 'vare': 'brus'}, 'a_antall_kaffe': {'dataset': 'AZ_drikkevarer', 'name': 'a_antall_kaffe', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'a', 'variabel': 'antall', 'vare': 'kaffe'}, 'a_antall_te': {'dataset': 'AZ_drikkevarer', 'name': 'a_antall_te', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'a', 'variabel': 'antall', 'vare': 'te'}, 'a_antall_vin': {'dataset': 'AZ_drikkevarer', 'name': 'a_antall_vin', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'a', 'variabel': 'antall', 'vare': 'vin'}, 'a_antall_øl': {'dataset': 'AZ_drikkevarer', 'name': 'a_antall_øl', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'a', 'variabel': 'antall', 'vare': 'øl'}, 'a_pris_brus': {'dataset': 'AZ_drikkevarer', 'name': 'a_pris_brus', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'a', 'variabel': 'pris', 'vare': 'brus'}, 'a_pris_kaffe': {'dataset': 'AZ_drikkevarer', 'name': 'a_pris_kaffe', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'a', 'variabel': 'pris', 'vare': 'kaffe'}, 'a_pris_te': {'dataset': 'AZ_drikkevarer', 'name': 'a_pris_te', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'a', 'variabel': 'pris', 'vare': 'te'}, 'a_pris_vin': {'dataset': 'AZ_drikkevarer', 'name': 'a_pris_vin', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'a', 'variabel': 'pris', 'vare': 'vin'}, 'a_pris_øl': {'dataset': 'AZ_drikkevarer', 'name': 'a_pris_øl', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'a', 'variabel': 'pris', 'vare': 'øl'}, 'b_antall_brus': {'dataset': 'AZ_drikkevarer', 'name': 'b_antall_brus', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'b', 'variabel': 'antall', 'vare': 'brus'}, 'b_antall_kaffe': {'dataset': 'AZ_drikkevarer', 'name': 'b_antall_kaffe', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'b', 'variabel': 'antall', 'vare': 'kaffe'}, 'b_antall_te': {'dataset': 'AZ_drikkevarer', 'name': 'b_antall_te', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'b', 'variabel': 'antall', 'vare': 'te'}, 'b_antall_vin': {'dataset': 'AZ_drikkevarer', 'name': 'b_antall_vin', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'b', 'variabel': 'antall', 'vare': 'vin'}, 'b_antall_øl': {'dataset': 'AZ_drikkevarer', 'name': 'b_antall_øl', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'b', 'variabel': 'antall', 'vare': 'øl'}, 'b_pris_brus': {'dataset': 'AZ_drikkevarer', 'name': 'b_pris_brus', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'b', 'variabel': 'pris', 'vare': 'brus'}, 'b_pris_kaffe': {'dataset': 'AZ_drikkevarer', 'name': 'b_pris_kaffe', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'b', 'variabel': 'pris', 'vare': 'kaffe'}, 'b_pris_te': {'dataset': 'AZ_drikkevarer', 'name': 'b_pris_te', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'b', 'variabel': 'pris', 'vare': 'te'}, 'b_pris_vin': {'dataset': 'AZ_drikkevarer', 'name': 'b_pris_vin', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'b', 'variabel': 'pris', 'vare': 'vin'}, 'b_pris_øl': {'dataset': 'AZ_drikkevarer', 'name': 'b_pris_øl', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'b', 'variabel': 'pris', 'vare': 'øl'}, 'c_antall_brus': {'dataset': 'AZ_drikkevarer', 'name': 'c_antall_brus', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'c', 'variabel': 'antall', 'vare': 'brus'}, 'c_antall_kaffe': {'dataset': 'AZ_drikkevarer', 'name': 'c_antall_kaffe', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'c', 'variabel': 'antall', 'vare': 'kaffe'}, 'c_antall_te': {'dataset': 'AZ_drikkevarer', 'name': 'c_antall_te', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'c', 'variabel': 'antall', 'vare': 'te'}, 'c_antall_vin': {'dataset': 'AZ_drikkevarer', 'name': 'c_antall_vin', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'c', 'variabel': 'antall', 'vare': 'vin'}, 'c_antall_øl': {'dataset': 'AZ_drikkevarer', 'name': 'c_antall_øl', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'c', 'variabel': 'antall', 'vare': 'øl'}, 'c_pris_brus': {'dataset': 'AZ_drikkevarer', 'name': 'c_pris_brus', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'c', 'variabel': 'pris', 'vare': 'brus'}, 'c_pris_kaffe': {'dataset': 'AZ_drikkevarer', 'name': 'c_pris_kaffe', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'c', 'variabel': 'pris', 'vare': 'kaffe'}, 'c_pris_te': {'dataset': 'AZ_drikkevarer', 'name': 'c_pris_te', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'c', 'variabel': 'pris', 'vare': 'te'}, 'c_pris_vin': {'dataset': 'AZ_drikkevarer', 'name': 'c_pris_vin', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'c', 'variabel': 'pris', 'vare': 'vin'}, 'c_pris_øl': {'dataset': 'AZ_drikkevarer', 'name': 'c_pris_øl', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'c', 'variabel': 'pris', 'vare': 'øl'}, 'd_antall_brus': {'dataset': 'AZ_drikkevarer', 'name': 'd_antall_brus', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'd', 'variabel': 'antall', 'vare': 'brus'}, 'd_antall_kaffe': {'dataset': 'AZ_drikkevarer', 'name': 'd_antall_kaffe', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'd', 'variabel': 'antall', 'vare': 'kaffe'}, 'd_antall_te': {'dataset': 'AZ_drikkevarer', 'name': 'd_antall_te', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'd', 'variabel': 'antall', 'vare': 'te'}, 'd_antall_vin': {'dataset': 'AZ_drikkevarer', 'name': 'd_antall_vin', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'd', 'variabel': 'antall', 'vare': 'vin'}, 'd_antall_øl': {'dataset': 'AZ_drikkevarer', 'name': 'd_antall_øl', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'd', 'variabel': 'antall', 'vare': 'øl'}, 'd_pris_brus': {'dataset': 'AZ_drikkevarer', 'name': 'd_pris_brus', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'd', 'variabel': 'pris', 'vare': 'brus'}, 'd_pris_kaffe': {'dataset': 'AZ_drikkevarer', 'name': 'd_pris_kaffe', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'd', 'variabel': 'pris', 'vare': 'kaffe'}, 'd_pris_te': {'dataset': 'AZ_drikkevarer', 'name': 'd_pris_te', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'd', 'variabel': 'pris', 'vare': 'te'}, 'd_pris_vin': {'dataset': 'AZ_drikkevarer', 'name': 'd_pris_vin', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'd', 'variabel': 'pris', 'vare': 'vin'}, 'd_pris_øl': {'dataset': 'AZ_drikkevarer', 'name': 'd_pris_øl', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'd', 'variabel': 'pris', 'vare': 'øl'}, 'e_antall_brus': {'dataset': 'AZ_drikkevarer', 'name': 'e_antall_brus', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'e', 'variabel': 'antall', 'vare': 'brus'}, 'e_antall_kaffe': {'dataset': 'AZ_drikkevarer', 'name': 'e_antall_kaffe', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'e', 'variabel': 'antall', 'vare': 'kaffe'}, 'e_antall_te': {'dataset': 'AZ_drikkevarer', 'name': 'e_antall_te', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'e', 'variabel': 'antall', 'vare': 'te'}, 'e_antall_vin': {'dataset': 'AZ_drikkevarer', 'name': 'e_antall_vin', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'e', 'variabel': 'antall', 'vare': 'vin'}, 'e_antall_øl': {'dataset': 'AZ_drikkevarer', 'name': 'e_antall_øl', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'e', 'variabel': 'antall', 'vare': 'øl'}, 'e_pris_brus': {'dataset': 'AZ_drikkevarer', 'name': 'e_pris_brus', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'e', 'variabel': 'pris', 'vare': 'brus'}, 'e_pris_kaffe': {'dataset': 'AZ_drikkevarer', 'name': 'e_pris_kaffe', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'e', 'variabel': 'pris', 'vare': 'kaffe'}, 'e_pris_te': {'dataset': 'AZ_drikkevarer', 'name': 'e_pris_te', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'e', 'variabel': 'pris', 'vare': 'te'}, 'e_pris_vin': {'dataset': 'AZ_drikkevarer', 'name': 'e_pris_vin', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'e', 'variabel': 'pris', 'vare': 'vin'}, 'e_pris_øl': {'dataset': 'AZ_drikkevarer', 'name': 'e_pris_øl', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'e', 'variabel': 'pris', 'vare': 'øl'}, 'f_antall_brus': {'dataset': 'AZ_drikkevarer', 'name': 'f_antall_brus', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'f', 'variabel': 'antall', 'vare': 'brus'}, 'f_antall_kaffe': {'dataset': 'AZ_drikkevarer', 'name': 'f_antall_kaffe', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'f', 'variabel': 'antall', 'vare': 'kaffe'}, 'f_antall_te': {'dataset': 'AZ_drikkevarer', 'name': 'f_antall_te', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'f', 'variabel': 'antall', 'vare': 'te'}, 'f_antall_vin': {'dataset': 'AZ_drikkevarer', 'name': 'f_antall_vin', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'f', 'variabel': 'antall', 'vare': 'vin'}, 'f_antall_øl': {'dataset': 'AZ_drikkevarer', 'name': 'f_antall_øl', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'f', 'variabel': 'antall', 'vare': 'øl'}, 'f_pris_brus': {'dataset': 'AZ_drikkevarer', 'name': 'f_pris_brus', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'f', 'variabel': 'pris', 'vare': 'brus'}, 'f_pris_kaffe': {'dataset': 'AZ_drikkevarer', 'name': 'f_pris_kaffe', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'f', 'variabel': 'pris', 'vare': 'kaffe'}, 'f_pris_te': {'dataset': 'AZ_drikkevarer', 'name': 'f_pris_te', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'f', 'variabel': 'pris', 'vare': 'te'}, 'f_pris_vin': {'dataset': 'AZ_drikkevarer', 'name': 'f_pris_vin', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'f', 'variabel': 'pris', 'vare': 'vin'}, 'f_pris_øl': {'dataset': 'AZ_drikkevarer', 'name': 'f_pris_øl', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'f', 'variabel': 'pris', 'vare': 'øl'}, 'g_antall_brus': {'dataset': 'AZ_drikkevarer', 'name': 'g_antall_brus', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'g', 'variabel': 'antall', 'vare': 'brus'}, 'g_antall_kaffe': {'dataset': 'AZ_drikkevarer', 'name': 'g_antall_kaffe', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'g', 'variabel': 'antall', 'vare': 'kaffe'}, 'g_antall_te': {'dataset': 'AZ_drikkevarer', 'name': 'g_antall_te', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'g', 'variabel': 'antall', 'vare': 'te'}, 'g_antall_vin': {'dataset': 'AZ_drikkevarer', 'name': 'g_antall_vin', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'g', 'variabel': 'antall', 'vare': 'vin'}, 'g_antall_øl': {'dataset': 'AZ_drikkevarer', 'name': 'g_antall_øl', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'g', 'variabel': 'antall', 'vare': 'øl'}, 'g_pris_brus': {'dataset': 'AZ_drikkevarer', 'name': 'g_pris_brus', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'g', 'variabel': 'pris', 'vare': 'brus'}, 'g_pris_kaffe': {'dataset': 'AZ_drikkevarer', 'name': 'g_pris_kaffe', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'g', 'variabel': 'pris', 'vare': 'kaffe'}, 'g_pris_te': {'dataset': 'AZ_drikkevarer', 'name': 'g_pris_te', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'g', 'variabel': 'pris', 'vare': 'te'}, 'g_pris_vin': {'dataset': 'AZ_drikkevarer', 'name': 'g_pris_vin', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'g', 'variabel': 'pris', 'vare': 'vin'}, 'g_pris_øl': {'dataset': 'AZ_drikkevarer', 'name': 'g_pris_øl', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'g', 'variabel': 'pris', 'vare': 'øl'}, 'h_antall_brus': {'dataset': 'AZ_drikkevarer', 'name': 'h_antall_brus', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'h', 'variabel': 'antall', 'vare': 'brus'}, 'h_antall_kaffe': {'dataset': 'AZ_drikkevarer', 'name': 'h_antall_kaffe', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'h', 'variabel': 'antall', 'vare': 'kaffe'}, 'h_antall_te': {'dataset': 'AZ_drikkevarer', 'name': 'h_antall_te', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'h', 'variabel': 'antall', 'vare': 'te'}, 'h_antall_vin': {'dataset': 'AZ_drikkevarer', 'name': 'h_antall_vin', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'h', 'variabel': 'antall', 'vare': 'vin'}, 'h_antall_øl': {'dataset': 'AZ_drikkevarer', 'name': 'h_antall_øl', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'h', 'variabel': 'antall', 'vare': 'øl'}, 'h_pris_brus': {'dataset': 'AZ_drikkevarer', 'name': 'h_pris_brus', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'h', 'variabel': 'pris', 'vare': 'brus'}, 'h_pris_kaffe': {'dataset': 'AZ_drikkevarer', 'name': 'h_pris_kaffe', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'h', 'variabel': 'pris', 'vare': 'kaffe'}, 'h_pris_te': {'dataset': 'AZ_drikkevarer', 'name': 'h_pris_te', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'h', 'variabel': 'pris', 'vare': 'te'}, 'h_pris_vin': {'dataset': 'AZ_drikkevarer', 'name': 'h_pris_vin', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'h', 'variabel': 'pris', 'vare': 'vin'}, 'h_pris_øl': {'dataset': 'AZ_drikkevarer', 'name': 'h_pris_øl', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'h', 'variabel': 'pris', 'vare': 'øl'}, 'i_antall_brus': {'dataset': 'AZ_drikkevarer', 'name': 'i_antall_brus', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'i', 'variabel': 'antall', 'vare': 'brus'}, 'i_antall_kaffe': {'dataset': 'AZ_drikkevarer', 'name': 'i_antall_kaffe', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'i', 'variabel': 'antall', 'vare': 'kaffe'}, 'i_antall_te': {'dataset': 'AZ_drikkevarer', 'name': 'i_antall_te', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'i', 'variabel': 'antall', 'vare': 'te'}, 'i_antall_vin': {'dataset': 'AZ_drikkevarer', 'name': 'i_antall_vin', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'i', 'variabel': 'antall', 'vare': 'vin'}, 'i_antall_øl': {'dataset': 'AZ_drikkevarer', 'name': 'i_antall_øl', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'i', 'variabel': 'antall', 'vare': 'øl'}, 'i_pris_brus': {'dataset': 'AZ_drikkevarer', 'name': 'i_pris_brus', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'i', 'variabel': 'pris', 'vare': 'brus'}, 'i_pris_kaffe': {'dataset': 'AZ_drikkevarer', 'name': 'i_pris_kaffe', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'i', 'variabel': 'pris', 'vare': 'kaffe'}, 'i_pris_te': {'dataset': 'AZ_drikkevarer', 'name': 'i_pris_te', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'i', 'variabel': 'pris', 'vare': 'te'}, 'i_pris_vin': {'dataset': 'AZ_drikkevarer', 'name': 'i_pris_vin', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'i', 'variabel': 'pris', 'vare': 'vin'}, 'i_pris_øl': {'dataset': 'AZ_drikkevarer', 'name': 'i_pris_øl', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'i', 'variabel': 'pris', 'vare': 'øl'}, 'j_antall_brus': {'dataset': 'AZ_drikkevarer', 'name': 'j_antall_brus', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'j', 'variabel': 'antall', 'vare': 'brus'}, 'j_antall_kaffe': {'dataset': 'AZ_drikkevarer', 'name': 'j_antall_kaffe', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'j', 'variabel': 'antall', 'vare': 'kaffe'}, 'j_antall_te': {'dataset': 'AZ_drikkevarer', 'name': 'j_antall_te', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'j', 'variabel': 'antall', 'vare': 'te'}, 'j_antall_vin': {'dataset': 'AZ_drikkevarer', 'name': 'j_antall_vin', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'j', 'variabel': 'antall', 'vare': 'vin'}, 'j_antall_øl': {'dataset': 'AZ_drikkevarer', 'name': 'j_antall_øl', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'j', 'variabel': 'antall', 'vare': 'øl'}, 'j_pris_brus': {'dataset': 'AZ_drikkevarer', 'name': 'j_pris_brus', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'j', 'variabel': 'pris', 'vare': 'brus'}, 'j_pris_kaffe': {'dataset': 'AZ_drikkevarer', 'name': 'j_pris_kaffe', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'j', 'variabel': 'pris', 'vare': 'kaffe'}, 'j_pris_te': {'dataset': 'AZ_drikkevarer', 'name': 'j_pris_te', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'j', 'variabel': 'pris', 'vare': 'te'}, 'j_pris_vin': {'dataset': 'AZ_drikkevarer', 'name': 'j_pris_vin', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'j', 'variabel': 'pris', 'vare': 'vin'}, 'j_pris_øl': {'dataset': 'AZ_drikkevarer', 'name': 'j_pris_øl', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'j', 'variabel': 'pris', 'vare': 'øl'}, ...}, 'repository': 'tutorials'} |  | None |
| tutorials | a_antall_brus | series | {'dataset': 'AZ_drikkevarer', 'name': 'a_antall_brus', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'a', 'variabel': 'antall', 'vare': 'brus'} | AZ_drikkevarer | None |
| tutorials | a_antall_kaffe | series | {'dataset': 'AZ_drikkevarer', 'name': 'a_antall_kaffe', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'a', 'variabel': 'antall', 'vare': 'kaffe'} | AZ_drikkevarer | None |
| tutorials | a_antall_te | series | {'dataset': 'AZ_drikkevarer', 'name': 'a_antall_te', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'a', 'variabel': 'antall', 'vare': 'te'} | AZ_drikkevarer | None |
| tutorials | a_antall_vin | series | {'dataset': 'AZ_drikkevarer', 'name': 'a_antall_vin', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'butikk': 'a', 'variabel': 'antall', 'vare': 'vin'} | AZ_drikkevarer | None |
| ... | ... | ... | ... | ... | ... |
| tutorials | z_volume_wine_NW | series | {'dataset': 'AZ_drinks', 'name': 'z_volume_wine_NW', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'z', 'variable': 'volume', 'product': 'wine', 'region': 'NW'} | AZ_drinks | None |
| tutorials | z_volume_wine_S | series | {'dataset': 'AZ_drinks', 'name': 'z_volume_wine_S', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'z', 'variable': 'volume', 'product': 'wine', 'region': 'S'} | AZ_drinks | None |
| tutorials | z_volume_wine_SE | series | {'dataset': 'AZ_drinks', 'name': 'z_volume_wine_SE', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'z', 'variable': 'volume', 'product': 'wine', 'region': 'SE'} | AZ_drinks | None |
| tutorials | z_volume_wine_SW | series | {'dataset': 'AZ_drinks', 'name': 'z_volume_wine_SW', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'z', 'variable': 'volume', 'product': 'wine', 'region': 'SW'} | AZ_drinks | None |
| tutorials | z_volume_wine_W | series | {'dataset': 'AZ_drinks', 'name': 'z_volume_wine_W', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'z', 'variable': 'volume', 'product': 'wine', 'region': 'W'} | AZ_drinks | None |

### Consuming KLASS

```python {.marimo}
from klass import get_classification
from klass import KlassClassification # Import the class for KlassClassifications
```

```python {.marimo}
print(get_classification(157))
```

<!-- @output:jxvo -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">Classification 157: Standard for klassifisering av energibalanseposter
        Owning Section: 425 - Seksjon for energi-, miljø- og ​transportstatistikk
        Contact Person:
	name: Bjelvert, Malin
	email: Malin.Bjelvert@ssb.no
	phone:

        Statistical Units: Foretak, Aktivitet/produkt/tjeneste/vare, Person, Bedrift
        Number of versions: 1

Klassifisering av energibalanseposter er en klassifisering av tilgang og anvendelse av energiprodukter. Energibalansen følger en territorial avgrensing og omfatter all
flyt av energiprodukter på norsk jord, uavhengig av nasjonalitet.

</pre>

```python {.marimo}
from ssb_timeseries.meta import Taxonomy
```

```python {.marimo}
print('"Energy balance posts" - KLASS 157 is an example of a taxonomy with hierarchical structure.')
klass157 = Taxonomy(klass_id=157)
klass157.entities # <-- arrow table, with an extra row 0
```

<!-- @output:CcZR -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">&quot;Energy balance posts&quot; - KLASS 157 is an example of a taxonomy with hierarchical structure.
</pre>

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">pyarrow.Table
code: string not null
parentCode: string
name: string not null
level: string
shortName: string
presentationName: string
validFrom: string
validTo: string
notes: string
----
code: &#91;&#91;&quot;0&quot;,&quot;1&quot;,&quot;1.1&quot;,&quot;1.1.1&quot;,&quot;1.1.2&quot;,...,&quot;8.6&quot;,&quot;8.7&quot;,&quot;8.8&quot;,&quot;8.9&quot;,&quot;9&quot;&#93;&#93;
parentCode: &#91;&#91;null,&quot;0&quot;,&quot;1&quot;,&quot;1.1&quot;,&quot;1.1&quot;,...,&quot;8&quot;,&quot;8&quot;,&quot;8&quot;,&quot;8&quot;,&quot;0&quot;&#93;&#93;
name: &#91;&#91;&quot;KLASS-157&quot;,&quot;Produksjon av primære energiprodukter&quot;,&quot;Av dette fra fornybare kilder&quot;,&quot;Av dette i vannkraftverk&quot;,&quot;Av dette i vindkraftverk&quot;,...,&quot;Vindkraftstasjoner&quot;,&quot;Varmekraftverk&quot;,&quot;Kraftvarmeverk&quot;,&quot;Fjernvarmeverk&quot;,&quot;Svinn&quot;&#93;&#93;
level: &#91;&#91;&quot;0&quot;,&quot;1&quot;,&quot;2&quot;,&quot;3&quot;,&quot;3&quot;,...,&quot;2&quot;,&quot;2&quot;,&quot;2&quot;,&quot;2&quot;,&quot;1&quot;&#93;&#93;
shortName: &#91;&#91;&quot;&quot;,&quot;&quot;,&quot;&quot;,&quot;&quot;,&quot;&quot;,...,&quot;&quot;,&quot;&quot;,&quot;&quot;,&quot;&quot;,&quot;&quot;&#93;&#93;
presentationName: &#91;&#91;&quot;&quot;,&quot;&quot;,&quot;&quot;,&quot;&quot;,&quot;&quot;,...,&quot;&quot;,&quot;&quot;,&quot;&quot;,&quot;&quot;,&quot;&quot;&#93;&#93;
validFrom: &#91;&#91;&quot;&quot;,null,null,null,null,...,null,null,null,null,null&#93;&#93;
validTo: &#91;&#91;&quot;&quot;,null,null,null,null,...,null,null,null,null,null&#93;&#93;
notes: &#91;&#91;&quot;&quot;,&quot;Produksjon av primære energiprodukter omfatter utvinning av brensel eller energi fra naturlige fo (... 239 chars omitted)&quot;,&quot;Produksjon av primære energiprodukter fra fornybare energikilder, for eksempel produksjon av biod (... 72 chars omitted)&quot;,&quot;Elektrisitet produsert i verk som drives av frisk, flytende eller fallende vann.&quot;,&quot;Elektrisitet produsert i verk som drives av vindkraft&quot;,...,&quot;&quot;,&quot;I varmekraftverk omvandles termisk energi til elektrisitet ved bruk av brennbare energiprodukter,  (... 107 chars omitted)&quot;,&quot;Forbruk av energi i anlegg som produserer både varme og elektrisk kraft.&quot;,&quot;I fjernvarmeverk omvandles termisk energi til varme ved bruk av brennbare energiprodukter, dvs. en (... 100 chars omitted)&quot;,&quot;Svinn er tap under overføring, fordeling og transport av brensel, varme og elektrisitet.&quot;&#93;&#93;</pre>

```python {.marimo}
# ... is inserted by the Taxonomy() bto create a tree structure with a single root node
klass157.print_tree()
```

<!-- @output:YWSi -->

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

```python {.marimo}
print(klass157.leaf_nodes)
```

<!-- @output:zlud -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">&#91;&#x27;1.1.1&#x27;, &#x27;1.1.2&#x27;, &#x27;1.1.3&#x27;, &#x27;1.2&#x27;, &#x27;11.1&#x27;, &#x27;11.2&#x27;, &#x27;12.1.1&#x27;, &#x27;12.1.10&#x27;, &#x27;12.1.11&#x27;, &#x27;12.1.12&#x27;, &#x27;12.1.13&#x27;, &#x27;12.1.2&#x27;, &#x27;12.1.3&#x27;, &#x27;12.1.4&#x27;, &#x27;12.1.5&#x27;, &#x27;12.1.6&#x27;, &#x27;12.1.7&#x27;, &#x27;12.1.8&#x27;, &#x27;12.1.9&#x27;, &#x27;12.2.1&#x27;, &#x27;12.2.2&#x27;, &#x27;12.2.3&#x27;, &#x27;12.2.4&#x27;, &#x27;12.2.5&#x27;, &#x27;12.3.1&#x27;, &#x27;12.3.2&#x27;, &#x27;12.3.3&#x27;, &#x27;12.3.4&#x27;, &#x27;13&#x27;, &#x27;14&#x27;, &#x27;15&#x27;, &#x27;2&#x27;, &#x27;3&#x27;, &#x27;4.1&#x27;, &#x27;4.2&#x27;, &#x27;5&#x27;, &#x27;6&#x27;, &#x27;7.1&#x27;, &#x27;7.2&#x27;, &#x27;7.3&#x27;, &#x27;7.4&#x27;, &#x27;7.5&#x27;, &#x27;7.6&#x27;, &#x27;8.1&#x27;, &#x27;8.2&#x27;, &#x27;8.3&#x27;, &#x27;8.4&#x27;, &#x27;8.5&#x27;, &#x27;8.6&#x27;, &#x27;8.7&#x27;, &#x27;8.8&#x27;, &#x27;8.9&#x27;, &#x27;9&#x27;&#93;
</pre>

```python {.marimo}
print(klass157.parent_nodes)
```

<!-- @output:tZnO -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">&#91;&#x27;1&#x27;, &#x27;0&#x27;, &#x27;1.1&#x27;, &#x27;11&#x27;, &#x27;12&#x27;, &#x27;12.1&#x27;, &#x27;12.2&#x27;, &#x27;12.3&#x27;, &#x27;4&#x27;, &#x27;7&#x27;, &#x27;8&#x27;&#93;
</pre>

```python {.marimo}
# read/write to file -> taxonomies can be defined outside KLASS
klass157.save('klass157.json')
file157 = Taxonomy(path='klass157.json')
```

```python {.marimo}
klass157 == file157
```

<!-- @output:CLip -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">True</pre>
