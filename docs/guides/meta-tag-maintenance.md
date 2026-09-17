---
title: Meta Tag Maintenance
marimo-version: 0.24.0
---

Tag maintentance
================
<!---->
Scope
-----

As shown in the basics, a dataset gets a few mandatory technical attributes on creation.

Additional descriptive metadata may be provided, that is the dataset and its series may be tagged.
The tagging is a one time operation that neeed should not need to be repeated.
That is, unless mistakes or omissions have been made, or new series are added to the set.

For calculations that derive new data.
Some functions will automaticly update the metadata.
Others will require that to be handled by the user.
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

The technical metadata is added at creation time.

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

Tags can be used immediately.

```python {.marimo}
pqr[{'vare': 'kaffe'}].data
```

<!-- @output:ROlb -->

| valid_at | p |
| --- | --- |
| 2020-01-01 | 90.0 |
| 2020-01-02 | 90.0 |
| 2020-01-03 | 110.0 |
| 2020-01-04 | 110.0 |
| 2020-01-05 | 100.0 |
| ... | ... |
| 2025-05-28 | 90.0 |
| 2025-05-29 | 90.0 |
| 2025-05-30 | 100.0 |
| 2025-05-31 | 110.0 |
| 2025-06-01 | 90.0 |

```python {.marimo}
pqr.save()
```

Autotagging
-----------

```python {.marimo}
interval_data = SeriesType('NONE', 'FROM_TO')
```

```python {.marimo}
def mock_interval_data_from_file_or_query(start, end):
    a_to_z = [chr(i) for i in range(ord('a'), ord('z') + 1)]
    variables = ['volume', 'price']
    goods = ['coffe', 'tea', 'softdrinks', 'beer', 'wine']
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

<!-- @output:ecfG -->

| valid_from | valid_to | a_volume_coffe | a_volume_tea | a_volume_softdrinks | a_volume_beer | a_volume_wine | a_price_coffe | a_price_tea | a_price_softdrinks | a_price_beer | a_price_wine | b_volume_coffe | b_volume_tea | b_volume_softdrinks | b_volume_beer | b_volume_wine | b_price_coffe | b_price_tea | b_price_softdrinks | b_price_beer | b_price_wine | c_volume_coffe | c_volume_tea | c_volume_softdrinks | c_volume_beer | c_volume_wine | c_price_coffe | c_price_tea | c_price_softdrinks | c_price_beer | c_price_wine | d_volume_coffe | d_volume_tea | d_volume_softdrinks | d_volume_beer | d_volume_wine | … | w_volume_beer | w_volume_wine | w_price_coffe | w_price_tea | w_price_softdrinks | w_price_beer | w_price_wine | x_volume_coffe | x_volume_tea | x_volume_softdrinks | x_volume_beer | x_volume_wine | x_price_coffe | x_price_tea | x_price_softdrinks | x_price_beer | x_price_wine | y_volume_coffe | y_volume_tea | y_volume_softdrinks | y_volume_beer | y_volume_wine | y_price_coffe | y_price_tea | y_price_softdrinks | y_price_beer | y_price_wine | z_volume_coffe | z_volume_tea | z_volume_softdrinks | z_volume_beer | z_volume_wine | z_price_coffe | z_price_tea | z_price_softdrinks | z_price_beer | z_price_wine |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| datetime[μs] | datetime[μs] | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | … | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 |
| 2025-01-01 00:00:00 | 2025-02-01 00:00:00 | 100.0 | 100.0 | 110.0 | 100.0 | 110.0 | 110.0 | 100.0 | 90.0 | 90.0 | 100.0 | 90.0 | 110.0 | 90.0 | 90.0 | 90.0 | 110.0 | 100.0 | 90.0 | 100.0 | 100.0 | 120.0 | 100.0 | 110.0 | 80.0 | 100.0 | 110.0 | 110.0 | 100.0 | 100.0 | 100.0 | 90.0 | 90.0 | 110.0 | 90.0 | 100.0 | … | 100.0 | 110.0 | 90.0 | 100.0 | 110.0 | 90.0 | 100.0 | 100.0 | 80.0 | 110.0 | 100.0 | 80.0 | 120.0 | 110.0 | 100.0 | 100.0 | 110.0 | 100.0 | 80.0 | 120.0 | 110.0 | 100.0 | 80.0 | 90.0 | 100.0 | 90.0 | 100.0 | 100.0 | 90.0 | 110.0 | 90.0 | 90.0 | 100.0 | 80.0 | 100.0 | 90.0 | 110.0 |
| 2025-02-01 00:00:00 | 2025-03-01 00:00:00 | 100.0 | 90.0 | 80.0 | 120.0 | 100.0 | 80.0 | 110.0 | 100.0 | 90.0 | 100.0 | 110.0 | 100.0 | 100.0 | 100.0 | 100.0 | 110.0 | 110.0 | 110.0 | 100.0 | 100.0 | 120.0 | 70.0 | 110.0 | 80.0 | 90.0 | 120.0 | 90.0 | 100.0 | 110.0 | 70.0 | 110.0 | 90.0 | 90.0 | 100.0 | 100.0 | … | 100.0 | 100.0 | 90.0 | 100.0 | 110.0 | 120.0 | 90.0 | 110.0 | 110.0 | 110.0 | 100.0 | 100.0 | 110.0 | 90.0 | 100.0 | 90.0 | 110.0 | 110.0 | 100.0 | 90.0 | 100.0 | 110.0 | 100.0 | 100.0 | 100.0 | 80.0 | 100.0 | 110.0 | 110.0 | 100.0 | 100.0 | 100.0 | 110.0 | 110.0 | 90.0 | 80.0 | 110.0 |
| 2025-03-01 00:00:00 | 2025-04-01 00:00:00 | 100.0 | 100.0 | 100.0 | 80.0 | 90.0 | 90.0 | 100.0 | 90.0 | 110.0 | 100.0 | 110.0 | 80.0 | 100.0 | 100.0 | 110.0 | 110.0 | 90.0 | 100.0 | 90.0 | 100.0 | 100.0 | 100.0 | 100.0 | 110.0 | 90.0 | 90.0 | 100.0 | 100.0 | 100.0 | 80.0 | 130.0 | 110.0 | 80.0 | 100.0 | 110.0 | … | 100.0 | 100.0 | 100.0 | 110.0 | 80.0 | 90.0 | 120.0 | 80.0 | 90.0 | 100.0 | 100.0 | 110.0 | 70.0 | 90.0 | 120.0 | 90.0 | 90.0 | 110.0 | 110.0 | 110.0 | 100.0 | 90.0 | 70.0 | 100.0 | 100.0 | 100.0 | 90.0 | 110.0 | 90.0 | 90.0 | 110.0 | 100.0 | 100.0 | 110.0 | 100.0 | 100.0 | 110.0 |
| 2025-04-01 00:00:00 | 2025-05-01 00:00:00 | 100.0 | 100.0 | 80.0 | 100.0 | 100.0 | 100.0 | 100.0 | 80.0 | 110.0 | 100.0 | 110.0 | 110.0 | 100.0 | 100.0 | 100.0 | 80.0 | 120.0 | 90.0 | 90.0 | 100.0 | 80.0 | 90.0 | 110.0 | 110.0 | 90.0 | 100.0 | 90.0 | 110.0 | 100.0 | 90.0 | 90.0 | 110.0 | 90.0 | 120.0 | 110.0 | … | 120.0 | 130.0 | 100.0 | 80.0 | 90.0 | 110.0 | 90.0 | 100.0 | 100.0 | 110.0 | 110.0 | 110.0 | 100.0 | 90.0 | 100.0 | 110.0 | 100.0 | 90.0 | 100.0 | 100.0 | 90.0 | 90.0 | 100.0 | 120.0 | 90.0 | 90.0 | 100.0 | 100.0 | 100.0 | 90.0 | 90.0 | 110.0 | 90.0 | 100.0 | 100.0 | 110.0 | 90.0 |
| 2025-05-01 00:00:00 | 2025-06-01 00:00:00 | 100.0 | 90.0 | 110.0 | 100.0 | 90.0 | 100.0 | 80.0 | 110.0 | 110.0 | 100.0 | 120.0 | 100.0 | 100.0 | 80.0 | 80.0 | 90.0 | 110.0 | 90.0 | 90.0 | 110.0 | 100.0 | 110.0 | 100.0 | 100.0 | 110.0 | 120.0 | 110.0 | 110.0 | 110.0 | 100.0 | 100.0 | 90.0 | 100.0 | 110.0 | 110.0 | … | 100.0 | 110.0 | 100.0 | 100.0 | 100.0 | 90.0 | 100.0 | 100.0 | 100.0 | 100.0 | 90.0 | 90.0 | 90.0 | 100.0 | 90.0 | 100.0 | 90.0 | 100.0 | 90.0 | 90.0 | 90.0 | 110.0 | 100.0 | 100.0 | 100.0 | 110.0 | 90.0 | 120.0 | 100.0 | 110.0 | 100.0 | 90.0 | 120.0 | 90.0 | 100.0 | 120.0 | 90.0 |
| 2025-06-01 00:00:00 | 2025-07-01 00:00:00 | 90.0 | 110.0 | 90.0 | 100.0 | 100.0 | 100.0 | 110.0 | 110.0 | 100.0 | 90.0 | 90.0 | 110.0 | 90.0 | 110.0 | 70.0 | 100.0 | 110.0 | 110.0 | 80.0 | 110.0 | 120.0 | 90.0 | 110.0 | 90.0 | 100.0 | 120.0 | 80.0 | 90.0 | 80.0 | 90.0 | 100.0 | 110.0 | 100.0 | 110.0 | 100.0 | … | 100.0 | 100.0 | 110.0 | 120.0 | 90.0 | 90.0 | 110.0 | 110.0 | 100.0 | 90.0 | 90.0 | 100.0 | 100.0 | 100.0 | 90.0 | 90.0 | 80.0 | 100.0 | 110.0 | 120.0 | 90.0 | 110.0 | 90.0 | 90.0 | 110.0 | 90.0 | 110.0 | 90.0 | 100.0 | 90.0 | 110.0 | 90.0 | 90.0 | 100.0 | 110.0 | 80.0 | 110.0 |

```python {.marimo}
az = Dataset(
    name = 'AZ Drinks',
    data_type = interval_data,
    data = bigger_data,
    attributes=['store','variable','product'], # <-- this is the clever part
)
az.save()
```

```python {.marimo}
len(az.series)
```

<!-- @output:ZBYS -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">260</pre>

```python {.marimo}
az_selection = az[{'product': 'tea', 'variable': 'price'}]
az_selection.data
```

<!-- @output:aLJB -->

| valid_from | valid_to | a_price_tea | b_price_tea | c_price_tea | d_price_tea | e_price_tea | f_price_tea | g_price_tea | h_price_tea | i_price_tea | j_price_tea | k_price_tea | l_price_tea | m_price_tea | n_price_tea | o_price_tea | p_price_tea | q_price_tea | r_price_tea | s_price_tea | t_price_tea | u_price_tea | v_price_tea | w_price_tea | x_price_tea | y_price_tea | z_price_tea |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| datetime[μs] | datetime[μs] | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 |
| 2025-01-01 00:00:00 | 2025-02-01 00:00:00 | 100.0 | 100.0 | 110.0 | 110.0 | 80.0 | 100.0 | 120.0 | 100.0 | 100.0 | 100.0 | 90.0 | 100.0 | 110.0 | 110.0 | 100.0 | 100.0 | 90.0 | 100.0 | 90.0 | 100.0 | 100.0 | 90.0 | 100.0 | 110.0 | 90.0 | 80.0 |
| 2025-02-01 00:00:00 | 2025-03-01 00:00:00 | 110.0 | 110.0 | 90.0 | 110.0 | 110.0 | 100.0 | 130.0 | 90.0 | 110.0 | 100.0 | 90.0 | 80.0 | 100.0 | 110.0 | 120.0 | 90.0 | 110.0 | 100.0 | 100.0 | 80.0 | 90.0 | 120.0 | 100.0 | 90.0 | 100.0 | 110.0 |
| 2025-03-01 00:00:00 | 2025-04-01 00:00:00 | 100.0 | 90.0 | 100.0 | 100.0 | 110.0 | 110.0 | 100.0 | 90.0 | 100.0 | 100.0 | 90.0 | 100.0 | 110.0 | 80.0 | 100.0 | 110.0 | 90.0 | 100.0 | 100.0 | 120.0 | 110.0 | 110.0 | 110.0 | 90.0 | 100.0 | 110.0 |
| 2025-04-01 00:00:00 | 2025-05-01 00:00:00 | 100.0 | 120.0 | 90.0 | 100.0 | 100.0 | 120.0 | 110.0 | 100.0 | 100.0 | 120.0 | 120.0 | 110.0 | 90.0 | 100.0 | 110.0 | 90.0 | 100.0 | 90.0 | 80.0 | 90.0 | 110.0 | 100.0 | 80.0 | 90.0 | 120.0 | 100.0 |
| 2025-05-01 00:00:00 | 2025-06-01 00:00:00 | 80.0 | 110.0 | 110.0 | 90.0 | 100.0 | 100.0 | 90.0 | 90.0 | 120.0 | 100.0 | 100.0 | 110.0 | 90.0 | 100.0 | 90.0 | 90.0 | 90.0 | 100.0 | 80.0 | 80.0 | 90.0 | 110.0 | 100.0 | 100.0 | 100.0 | 90.0 |
| 2025-06-01 00:00:00 | 2025-07-01 00:00:00 | 110.0 | 110.0 | 80.0 | 90.0 | 100.0 | 100.0 | 100.0 | 90.0 | 90.0 | 90.0 | 110.0 | 110.0 | 110.0 | 80.0 | 100.0 | 100.0 | 110.0 | 110.0 | 110.0 | 100.0 | 90.0 | 90.0 | 120.0 | 100.0 | 90.0 | 100.0 |

```python {.marimo}
len(az_selection.series)
```

<!-- @output:nHfw -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">26</pre>

By supplying the `attributes` parameter, we utilised the fact that names were structured as underscore separated strings.
This way, we managed to tag 26 * 2 * 5 attributes across 260 series.

The autotagging is quite powerful.
Additional parameters may be supplied to specify other separators, substitutions, or more complex patterns with regexes.
<!---->
Updating tags after calculations
--------------------------------

```python {.marimo}
prices = Dataset('AZ Drinks')[{'variable':'price'}]
volumes = Dataset('AZ Drinks')[{'variable':'volume'}]
revenues = prices * volumes
```

The new `Dataset` instance `revenues` gets an autogenerated name.
The series names are also inherited from the inputs to the calculation.

```python {.marimo}
print(revenues.name)
print(revenues.series)
```

<!-- @output:aqbW -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">(COPY of(AZ Drinks SELECTED by names (), pattern: , regex:  tags: &#91;{&#x27;variable&#x27;: &#x27;price&#x27;}&#93;).multiply.COPY of(AZ Drinks SELECTED by names (), pattern: , regex:  tags: &#91;{&#x27;variable&#x27;: &#x27;volume&#x27;}&#93;))
&#91;&#x27;a_price_beer&#x27;, &#x27;a_price_coffe&#x27;, &#x27;a_price_softdrinks&#x27;, &#x27;a_price_tea&#x27;, &#x27;a_price_wine&#x27;, &#x27;b_price_beer&#x27;, &#x27;b_price_coffe&#x27;, &#x27;b_price_softdrinks&#x27;, &#x27;b_price_tea&#x27;, &#x27;b_price_wine&#x27;, &#x27;c_price_beer&#x27;, &#x27;c_price_coffe&#x27;, &#x27;c_price_softdrinks&#x27;, &#x27;c_price_tea&#x27;, &#x27;c_price_wine&#x27;, &#x27;d_price_beer&#x27;, &#x27;d_price_coffe&#x27;, &#x27;d_price_softdrinks&#x27;, &#x27;d_price_tea&#x27;, &#x27;d_price_wine&#x27;, &#x27;e_price_beer&#x27;, &#x27;e_price_coffe&#x27;, &#x27;e_price_softdrinks&#x27;, &#x27;e_price_tea&#x27;, &#x27;e_price_wine&#x27;, &#x27;f_price_beer&#x27;, &#x27;f_price_coffe&#x27;, &#x27;f_price_softdrinks&#x27;, &#x27;f_price_tea&#x27;, &#x27;f_price_wine&#x27;, &#x27;g_price_beer&#x27;, &#x27;g_price_coffe&#x27;, &#x27;g_price_softdrinks&#x27;, &#x27;g_price_tea&#x27;, &#x27;g_price_wine&#x27;, &#x27;h_price_beer&#x27;, &#x27;h_price_coffe&#x27;, &#x27;h_price_softdrinks&#x27;, &#x27;h_price_tea&#x27;, &#x27;h_price_wine&#x27;, &#x27;i_price_beer&#x27;, &#x27;i_price_coffe&#x27;, &#x27;i_price_softdrinks&#x27;, &#x27;i_price_tea&#x27;, &#x27;i_price_wine&#x27;, &#x27;j_price_beer&#x27;, &#x27;j_price_coffe&#x27;, &#x27;j_price_softdrinks&#x27;, &#x27;j_price_tea&#x27;, &#x27;j_price_wine&#x27;, &#x27;k_price_beer&#x27;, &#x27;k_price_coffe&#x27;, &#x27;k_price_softdrinks&#x27;, &#x27;k_price_tea&#x27;, &#x27;k_price_wine&#x27;, &#x27;l_price_beer&#x27;, &#x27;l_price_coffe&#x27;, &#x27;l_price_softdrinks&#x27;, &#x27;l_price_tea&#x27;, &#x27;l_price_wine&#x27;, &#x27;m_price_beer&#x27;, &#x27;m_price_coffe&#x27;, &#x27;m_price_softdrinks&#x27;, &#x27;m_price_tea&#x27;, &#x27;m_price_wine&#x27;, &#x27;n_price_beer&#x27;, &#x27;n_price_coffe&#x27;, &#x27;n_price_softdrinks&#x27;, &#x27;n_price_tea&#x27;, &#x27;n_price_wine&#x27;, &#x27;o_price_beer&#x27;, &#x27;o_price_coffe&#x27;, &#x27;o_price_softdrinks&#x27;, &#x27;o_price_tea&#x27;, &#x27;o_price_wine&#x27;, &#x27;p_price_beer&#x27;, &#x27;p_price_coffe&#x27;, &#x27;p_price_softdrinks&#x27;, &#x27;p_price_tea&#x27;, &#x27;p_price_wine&#x27;, &#x27;q_price_beer&#x27;, &#x27;q_price_coffe&#x27;, &#x27;q_price_softdrinks&#x27;, &#x27;q_price_tea&#x27;, &#x27;q_price_wine&#x27;, &#x27;r_price_beer&#x27;, &#x27;r_price_coffe&#x27;, &#x27;r_price_softdrinks&#x27;, &#x27;r_price_tea&#x27;, &#x27;r_price_wine&#x27;, &#x27;s_price_beer&#x27;, &#x27;s_price_coffe&#x27;, &#x27;s_price_softdrinks&#x27;, &#x27;s_price_tea&#x27;, &#x27;s_price_wine&#x27;, &#x27;t_price_beer&#x27;, &#x27;t_price_coffe&#x27;, &#x27;t_price_softdrinks&#x27;, &#x27;t_price_tea&#x27;, &#x27;t_price_wine&#x27;, &#x27;u_price_beer&#x27;, &#x27;u_price_coffe&#x27;, &#x27;u_price_softdrinks&#x27;, &#x27;u_price_tea&#x27;, &#x27;u_price_wine&#x27;, &#x27;v_price_beer&#x27;, &#x27;v_price_coffe&#x27;, &#x27;v_price_softdrinks&#x27;, &#x27;v_price_tea&#x27;, &#x27;v_price_wine&#x27;, &#x27;w_price_beer&#x27;, &#x27;w_price_coffe&#x27;, &#x27;w_price_softdrinks&#x27;, &#x27;w_price_tea&#x27;, &#x27;w_price_wine&#x27;, &#x27;x_price_beer&#x27;, &#x27;x_price_coffe&#x27;, &#x27;x_price_softdrinks&#x27;, &#x27;x_price_tea&#x27;, &#x27;x_price_wine&#x27;, &#x27;y_price_beer&#x27;, &#x27;y_price_coffe&#x27;, &#x27;y_price_softdrinks&#x27;, &#x27;y_price_tea&#x27;, &#x27;y_price_wine&#x27;, &#x27;z_price_beer&#x27;, &#x27;z_price_coffe&#x27;, &#x27;z_price_softdrinks&#x27;, &#x27;z_price_tea&#x27;, &#x27;z_price_wine&#x27;&#93;
</pre>

```python {.marimo}

```

```python {.marimo}
revenues.rename('AZ Revenue', ('price', 'revenue'))
print(revenues.name)
print(revenues.series)
```

<!-- @output:TXez -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">AZ Revenue
&#91;&#x27;a_revenue_beer&#x27;, &#x27;a_revenue_coffe&#x27;, &#x27;a_revenue_softdrinks&#x27;, &#x27;a_revenue_tea&#x27;, &#x27;a_revenue_wine&#x27;, &#x27;b_revenue_beer&#x27;, &#x27;b_revenue_coffe&#x27;, &#x27;b_revenue_softdrinks&#x27;, &#x27;b_revenue_tea&#x27;, &#x27;b_revenue_wine&#x27;, &#x27;c_revenue_beer&#x27;, &#x27;c_revenue_coffe&#x27;, &#x27;c_revenue_softdrinks&#x27;, &#x27;c_revenue_tea&#x27;, &#x27;c_revenue_wine&#x27;, &#x27;d_revenue_beer&#x27;, &#x27;d_revenue_coffe&#x27;, &#x27;d_revenue_softdrinks&#x27;, &#x27;d_revenue_tea&#x27;, &#x27;d_revenue_wine&#x27;, &#x27;e_revenue_beer&#x27;, &#x27;e_revenue_coffe&#x27;, &#x27;e_revenue_softdrinks&#x27;, &#x27;e_revenue_tea&#x27;, &#x27;e_revenue_wine&#x27;, &#x27;f_revenue_beer&#x27;, &#x27;f_revenue_coffe&#x27;, &#x27;f_revenue_softdrinks&#x27;, &#x27;f_revenue_tea&#x27;, &#x27;f_revenue_wine&#x27;, &#x27;g_revenue_beer&#x27;, &#x27;g_revenue_coffe&#x27;, &#x27;g_revenue_softdrinks&#x27;, &#x27;g_revenue_tea&#x27;, &#x27;g_revenue_wine&#x27;, &#x27;h_revenue_beer&#x27;, &#x27;h_revenue_coffe&#x27;, &#x27;h_revenue_softdrinks&#x27;, &#x27;h_revenue_tea&#x27;, &#x27;h_revenue_wine&#x27;, &#x27;i_revenue_beer&#x27;, &#x27;i_revenue_coffe&#x27;, &#x27;i_revenue_softdrinks&#x27;, &#x27;i_revenue_tea&#x27;, &#x27;i_revenue_wine&#x27;, &#x27;j_revenue_beer&#x27;, &#x27;j_revenue_coffe&#x27;, &#x27;j_revenue_softdrinks&#x27;, &#x27;j_revenue_tea&#x27;, &#x27;j_revenue_wine&#x27;, &#x27;k_revenue_beer&#x27;, &#x27;k_revenue_coffe&#x27;, &#x27;k_revenue_softdrinks&#x27;, &#x27;k_revenue_tea&#x27;, &#x27;k_revenue_wine&#x27;, &#x27;l_revenue_beer&#x27;, &#x27;l_revenue_coffe&#x27;, &#x27;l_revenue_softdrinks&#x27;, &#x27;l_revenue_tea&#x27;, &#x27;l_revenue_wine&#x27;, &#x27;m_revenue_beer&#x27;, &#x27;m_revenue_coffe&#x27;, &#x27;m_revenue_softdrinks&#x27;, &#x27;m_revenue_tea&#x27;, &#x27;m_revenue_wine&#x27;, &#x27;n_revenue_beer&#x27;, &#x27;n_revenue_coffe&#x27;, &#x27;n_revenue_softdrinks&#x27;, &#x27;n_revenue_tea&#x27;, &#x27;n_revenue_wine&#x27;, &#x27;o_revenue_beer&#x27;, &#x27;o_revenue_coffe&#x27;, &#x27;o_revenue_softdrinks&#x27;, &#x27;o_revenue_tea&#x27;, &#x27;o_revenue_wine&#x27;, &#x27;p_revenue_beer&#x27;, &#x27;p_revenue_coffe&#x27;, &#x27;p_revenue_softdrinks&#x27;, &#x27;p_revenue_tea&#x27;, &#x27;p_revenue_wine&#x27;, &#x27;q_revenue_beer&#x27;, &#x27;q_revenue_coffe&#x27;, &#x27;q_revenue_softdrinks&#x27;, &#x27;q_revenue_tea&#x27;, &#x27;q_revenue_wine&#x27;, &#x27;r_revenue_beer&#x27;, &#x27;r_revenue_coffe&#x27;, &#x27;r_revenue_softdrinks&#x27;, &#x27;r_revenue_tea&#x27;, &#x27;r_revenue_wine&#x27;, &#x27;s_revenue_beer&#x27;, &#x27;s_revenue_coffe&#x27;, &#x27;s_revenue_softdrinks&#x27;, &#x27;s_revenue_tea&#x27;, &#x27;s_revenue_wine&#x27;, &#x27;t_revenue_beer&#x27;, &#x27;t_revenue_coffe&#x27;, &#x27;t_revenue_softdrinks&#x27;, &#x27;t_revenue_tea&#x27;, &#x27;t_revenue_wine&#x27;, &#x27;u_revenue_beer&#x27;, &#x27;u_revenue_coffe&#x27;, &#x27;u_revenue_softdrinks&#x27;, &#x27;u_revenue_tea&#x27;, &#x27;u_revenue_wine&#x27;, &#x27;v_revenue_beer&#x27;, &#x27;v_revenue_coffe&#x27;, &#x27;v_revenue_softdrinks&#x27;, &#x27;v_revenue_tea&#x27;, &#x27;v_revenue_wine&#x27;, &#x27;w_revenue_beer&#x27;, &#x27;w_revenue_coffe&#x27;, &#x27;w_revenue_softdrinks&#x27;, &#x27;w_revenue_tea&#x27;, &#x27;w_revenue_wine&#x27;, &#x27;x_revenue_beer&#x27;, &#x27;x_revenue_coffe&#x27;, &#x27;x_revenue_softdrinks&#x27;, &#x27;x_revenue_tea&#x27;, &#x27;x_revenue_wine&#x27;, &#x27;y_revenue_beer&#x27;, &#x27;y_revenue_coffe&#x27;, &#x27;y_revenue_softdrinks&#x27;, &#x27;y_revenue_tea&#x27;, &#x27;y_revenue_wine&#x27;, &#x27;z_revenue_beer&#x27;, &#x27;z_revenue_coffe&#x27;, &#x27;z_revenue_softdrinks&#x27;, &#x27;z_revenue_tea&#x27;, &#x27;z_revenue_wine&#x27;&#93;
</pre>

A similar operation is required for tags:

```python {.marimo}
# DEBUG: tags are lost in selects above, hence not flowing through
revenues.tags["series"]["a_revenue_beer"]
```

<!-- @output:yCnT -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">{&#x27;dataset&#x27;: &#x27;AZ Revenue&#x27;,
 &#x27;name&#x27;: &#x27;a_revenue_beer&#x27;,
 &#x27;product&#x27;: &#x27;beer&#x27;,
 &#x27;repository&#x27;: &#x27;tutorials&#x27;,
 &#x27;store&#x27;: &#x27;a&#x27;,
 &#x27;temporality&#x27;: &#x27;FROM_TO&#x27;,
 &#x27;variable&#x27;: &#x27;price&#x27;,
 &#x27;versioning&#x27;: &#x27;NONE&#x27;}</pre>

```python {.marimo}
# ... tag maintenance is likely to be necessary after calculations:
revenues.replace_tags(({'variable':'price'},{'variable':'revenue'}))
revenues.tags["series"]["a_revenue_beer"]
```

<!-- @output:wlCL -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">{&#x27;dataset&#x27;: &#x27;AZ Revenue&#x27;,
 &#x27;name&#x27;: &#x27;a_revenue_beer&#x27;,
 &#x27;product&#x27;: &#x27;beer&#x27;,
 &#x27;repository&#x27;: &#x27;tutorials&#x27;,
 &#x27;store&#x27;: &#x27;a&#x27;,
 &#x27;temporality&#x27;: &#x27;FROM_TO&#x27;,
 &#x27;variable&#x27;: &#x27;revenue&#x27;,
 &#x27;versioning&#x27;: &#x27;NONE&#x27;}</pre>

Detagging
---------
<!---->
If mistakes have been made, it may be necessary to remove tags.

```python {.marimo}
from copy import deepcopy
deepcopy(pqr.tags)
```

<!-- @output:rEll -->

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
pqr.detag_series('varegruppe', vare='knekkebrød')
```

```python {.marimo}
pqr.detag_series( vare='knekkebrød' )
pqr.tags
```

<!-- @output:SdmI -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">{&#x27;name&#x27;: &#x27;PQR&#x27;,
 &#x27;repository&#x27;: &#x27;tutorials&#x27;,
 &#x27;series&#x27;: {&#x27;p&#x27;: {&#x27;dataset&#x27;: &#x27;PQR&#x27;,
                  &#x27;name&#x27;: &#x27;p&#x27;,
                  &#x27;repository&#x27;: &#x27;tutorials&#x27;,
                  &#x27;temporality&#x27;: &#x27;AT&#x27;,
                  &#x27;vare&#x27;: &#x27;kaffe&#x27;,
                  &#x27;variabel&#x27;: &#x27;pris&#x27;,
                  &#x27;versioning&#x27;: &#x27;NONE&#x27;},
            &#x27;q&#x27;: {&#x27;dataset&#x27;: &#x27;PQR&#x27;,
                  &#x27;name&#x27;: &#x27;q&#x27;,
                  &#x27;repository&#x27;: &#x27;tutorials&#x27;,
                  &#x27;temporality&#x27;: &#x27;AT&#x27;,
                  &#x27;variabel&#x27;: &#x27;pris&#x27;,
                  &#x27;versioning&#x27;: &#x27;NONE&#x27;},
            &#x27;r&#x27;: {&#x27;dataset&#x27;: &#x27;PQR&#x27;,
                  &#x27;name&#x27;: &#x27;r&#x27;,
                  &#x27;repository&#x27;: &#x27;tutorials&#x27;,
                  &#x27;temporality&#x27;: &#x27;AT&#x27;,
                  &#x27;vare&#x27;: &#x27;brunost&#x27;,
                  &#x27;variabel&#x27;: &#x27;pris&#x27;,
                  &#x27;versioning&#x27;: &#x27;NONE&#x27;}},
 &#x27;temporality&#x27;: &#x27;AT&#x27;,
 &#x27;varegruppe&#x27;: &#x27;nødvendigheter&#x27;,
 &#x27;variabel&#x27;: &#x27;pris&#x27;,
 &#x27;versioning&#x27;: &#x27;NONE&#x27;}</pre>

```python {.marimo}

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

<!-- @output:urSm -->

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

```python {.marimo}
series_in_xyz = our_timeseries_database.series(tags={'dataset': 'XYZ'})
[s.object_name for s in series_in_xyz]
```

<!-- @output:jxvo -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">&#91;&#x27;x&#x27;, &#x27;y&#x27;, &#x27;z&#x27;&#93;</pre>

```python {.marimo}
import pandas as pd
everything = our_timeseries_database.items()
pd.DataFrame(everything)
```

<!-- @output:mWxS -->

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
<!---->
The `Dataset` and `Series` attributes are *technically* just key-value pairs.
It is, however, possible (even recommended) to rely on more formal taxonomies top structure these.

```python {.marimo}
from klass import get_classification
from klass import KlassClassification # Import the class for KlassClassifications
```

```python {.marimo}
print(get_classification(157))
```

<!-- @output:tZnO -->

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

<!-- @output:CLip -->

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

<!-- @output:YECM -->

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

<!-- @output:cEAS -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">&#91;&#x27;1.1.1&#x27;, &#x27;1.1.2&#x27;, &#x27;1.1.3&#x27;, &#x27;1.2&#x27;, &#x27;11.1&#x27;, &#x27;11.2&#x27;, &#x27;12.1.1&#x27;, &#x27;12.1.10&#x27;, &#x27;12.1.11&#x27;, &#x27;12.1.12&#x27;, &#x27;12.1.13&#x27;, &#x27;12.1.2&#x27;, &#x27;12.1.3&#x27;, &#x27;12.1.4&#x27;, &#x27;12.1.5&#x27;, &#x27;12.1.6&#x27;, &#x27;12.1.7&#x27;, &#x27;12.1.8&#x27;, &#x27;12.1.9&#x27;, &#x27;12.2.1&#x27;, &#x27;12.2.2&#x27;, &#x27;12.2.3&#x27;, &#x27;12.2.4&#x27;, &#x27;12.2.5&#x27;, &#x27;12.3.1&#x27;, &#x27;12.3.2&#x27;, &#x27;12.3.3&#x27;, &#x27;12.3.4&#x27;, &#x27;13&#x27;, &#x27;14&#x27;, &#x27;15&#x27;, &#x27;2&#x27;, &#x27;3&#x27;, &#x27;4.1&#x27;, &#x27;4.2&#x27;, &#x27;5&#x27;, &#x27;6&#x27;, &#x27;7.1&#x27;, &#x27;7.2&#x27;, &#x27;7.3&#x27;, &#x27;7.4&#x27;, &#x27;7.5&#x27;, &#x27;7.6&#x27;, &#x27;8.1&#x27;, &#x27;8.2&#x27;, &#x27;8.3&#x27;, &#x27;8.4&#x27;, &#x27;8.5&#x27;, &#x27;8.6&#x27;, &#x27;8.7&#x27;, &#x27;8.8&#x27;, &#x27;8.9&#x27;, &#x27;9&#x27;&#93;
</pre>

```python {.marimo}
print(klass157.parent_nodes)
```

<!-- @output:iXej -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">&#91;&#x27;1&#x27;, &#x27;0&#x27;, &#x27;1.1&#x27;, &#x27;11&#x27;, &#x27;12&#x27;, &#x27;12.1&#x27;, &#x27;12.2&#x27;, &#x27;12.3&#x27;, &#x27;4&#x27;, &#x27;7&#x27;, &#x27;8&#x27;&#93;
</pre>

```python {.marimo}
# read/write to file -> taxonomies can be defined outside KLASS
#klass157.save('klass157.json')
#file157 = Taxonomy(path='klass157.json')
```

```python {.marimo}
#klass157 == file157
```
