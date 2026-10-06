---
title: Meta Tag Maintenance
marimo-version: 0.24.2
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
pqr.tag_dataset(tags={'variabel': 'pris','varegruppe': 'nødvendigheter'})

pqr.tag_series('p',tags={'vare': 'kaffe'})
pqr.tag_series('q',tags={'vare': 'knekkebrød'})
pqr.tag_series('r',tags={'vare': 'brunost'})

pqr.tags
```

<!-- @output:iLit -->

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

Tags can be used immediately.

```python {.marimo}
pqr[{'vare': 'kaffe'}].data
```

<!-- @output:ROlb -->

| valid_at | p |
| --- | --- |
| 2019-12-31 23:00:00+00:00 | 100.0 |
| 2020-01-01 23:00:00+00:00 | 100.0 |
| 2020-01-02 23:00:00+00:00 | 120.0 |
| 2020-01-03 23:00:00+00:00 | 80.0 |
| 2020-01-04 23:00:00+00:00 | 90.0 |
| ... | ... |
| 2025-05-27 22:00:00+00:00 | 100.0 |
| 2025-05-28 22:00:00+00:00 | 90.0 |
| 2025-05-29 22:00:00+00:00 | 110.0 |
| 2025-05-30 22:00:00+00:00 | 90.0 |
| 2025-05-31 22:00:00+00:00 | 100.0 |

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
    products = ['coffee', 'tea', 'soft-drinks', 'beer', 'wine']
    regions = ['N', 'E', 'W', 'S', 'NE', 'NW', 'SE', 'SW']
    return create_df(
        a_to_z, variables, products, regions,
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

| valid_from | valid_to | a_volume_coffee_N | a_volume_coffee_E | a_volume_coffee_W | a_volume_coffee_S | a_volume_coffee_NE | a_volume_coffee_NW | a_volume_coffee_SE | a_volume_coffee_SW | a_volume_tea_N | a_volume_tea_E | a_volume_tea_W | a_volume_tea_S | a_volume_tea_NE | a_volume_tea_NW | a_volume_tea_SE | a_volume_tea_SW | a_volume_soft-drinks_N | a_volume_soft-drinks_E | a_volume_soft-drinks_W | a_volume_soft-drinks_S | a_volume_soft-drinks_NE | a_volume_soft-drinks_NW | a_volume_soft-drinks_SE | a_volume_soft-drinks_SW | a_volume_beer_N | a_volume_beer_E | a_volume_beer_W | a_volume_beer_S | a_volume_beer_NE | a_volume_beer_NW | a_volume_beer_SE | a_volume_beer_SW | a_volume_wine_N | a_volume_wine_E | a_volume_wine_W | … | z_price_coffee_S | z_price_coffee_NE | z_price_coffee_NW | z_price_coffee_SE | z_price_coffee_SW | z_price_tea_N | z_price_tea_E | z_price_tea_W | z_price_tea_S | z_price_tea_NE | z_price_tea_NW | z_price_tea_SE | z_price_tea_SW | z_price_soft-drinks_N | z_price_soft-drinks_E | z_price_soft-drinks_W | z_price_soft-drinks_S | z_price_soft-drinks_NE | z_price_soft-drinks_NW | z_price_soft-drinks_SE | z_price_soft-drinks_SW | z_price_beer_N | z_price_beer_E | z_price_beer_W | z_price_beer_S | z_price_beer_NE | z_price_beer_NW | z_price_beer_SE | z_price_beer_SW | z_price_wine_N | z_price_wine_E | z_price_wine_W | z_price_wine_S | z_price_wine_NE | z_price_wine_NW | z_price_wine_SE | z_price_wine_SW |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| datetime[μs, Europe/Oslo] | datetime[μs, Europe/Oslo] | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | … | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 |
| 2025-01-01 00:00:00 CET | 2025-02-01 00:00:00 CET | 100.0 | 90.0 | 90.0 | 100.0 | 100.0 | 100.0 | 100.0 | 90.0 | 110.0 | 100.0 | 120.0 | 90.0 | 90.0 | 90.0 | 100.0 | 120.0 | 130.0 | 100.0 | 110.0 | 100.0 | 110.0 | 70.0 | 90.0 | 80.0 | 100.0 | 100.0 | 100.0 | 90.0 | 80.0 | 90.0 | 110.0 | 110.0 | 120.0 | 110.0 | 100.0 | … | 110.0 | 100.0 | 100.0 | 100.0 | 100.0 | 90.0 | 120.0 | 100.0 | 110.0 | 110.0 | 100.0 | 90.0 | 100.0 | 110.0 | 90.0 | 100.0 | 110.0 | 110.0 | 100.0 | 90.0 | 110.0 | 90.0 | 100.0 | 100.0 | 90.0 | 110.0 | 110.0 | 100.0 | 100.0 | 100.0 | 100.0 | 90.0 | 80.0 | 120.0 | 90.0 | 80.0 | 100.0 |
| 2025-02-01 00:00:00 CET | 2025-03-01 00:00:00 CET | 100.0 | 110.0 | 70.0 | 80.0 | 100.0 | 80.0 | 100.0 | 80.0 | 90.0 | 100.0 | 90.0 | 110.0 | 100.0 | 80.0 | 100.0 | 80.0 | 100.0 | 110.0 | 110.0 | 90.0 | 90.0 | 120.0 | 100.0 | 80.0 | 110.0 | 90.0 | 90.0 | 120.0 | 100.0 | 80.0 | 110.0 | 100.0 | 80.0 | 100.0 | 80.0 | … | 120.0 | 90.0 | 120.0 | 90.0 | 110.0 | 110.0 | 120.0 | 110.0 | 100.0 | 90.0 | 110.0 | 100.0 | 100.0 | 90.0 | 90.0 | 110.0 | 90.0 | 100.0 | 100.0 | 100.0 | 100.0 | 100.0 | 110.0 | 100.0 | 110.0 | 100.0 | 110.0 | 100.0 | 110.0 | 100.0 | 90.0 | 90.0 | 90.0 | 100.0 | 110.0 | 100.0 | 100.0 |
| 2025-03-01 00:00:00 CET | 2025-04-01 00:00:00 CEST | 100.0 | 110.0 | 100.0 | 120.0 | 110.0 | 90.0 | 110.0 | 110.0 | 100.0 | 90.0 | 90.0 | 100.0 | 110.0 | 90.0 | 120.0 | 100.0 | 80.0 | 90.0 | 90.0 | 110.0 | 110.0 | 100.0 | 100.0 | 90.0 | 110.0 | 100.0 | 100.0 | 90.0 | 100.0 | 100.0 | 110.0 | 100.0 | 100.0 | 90.0 | 110.0 | … | 80.0 | 100.0 | 120.0 | 120.0 | 80.0 | 110.0 | 100.0 | 90.0 | 90.0 | 90.0 | 100.0 | 110.0 | 90.0 | 110.0 | 90.0 | 120.0 | 110.0 | 110.0 | 110.0 | 100.0 | 110.0 | 100.0 | 100.0 | 90.0 | 110.0 | 90.0 | 100.0 | 110.0 | 90.0 | 100.0 | 110.0 | 90.0 | 100.0 | 110.0 | 120.0 | 90.0 | 120.0 |
| 2025-04-01 00:00:00 CEST | 2025-05-01 00:00:00 CEST | 90.0 | 100.0 | 100.0 | 110.0 | 100.0 | 120.0 | 90.0 | 90.0 | 80.0 | 90.0 | 100.0 | 90.0 | 120.0 | 90.0 | 110.0 | 110.0 | 120.0 | 80.0 | 100.0 | 100.0 | 100.0 | 110.0 | 80.0 | 100.0 | 100.0 | 110.0 | 120.0 | 100.0 | 90.0 | 90.0 | 90.0 | 110.0 | 110.0 | 80.0 | 110.0 | … | 90.0 | 100.0 | 100.0 | 80.0 | 120.0 | 110.0 | 110.0 | 100.0 | 100.0 | 100.0 | 100.0 | 100.0 | 80.0 | 80.0 | 110.0 | 90.0 | 90.0 | 110.0 | 80.0 | 100.0 | 90.0 | 100.0 | 120.0 | 80.0 | 100.0 | 100.0 | 100.0 | 120.0 | 110.0 | 100.0 | 90.0 | 100.0 | 100.0 | 100.0 | 80.0 | 90.0 | 90.0 |
| 2025-05-01 00:00:00 CEST | 2025-06-01 00:00:00 CEST | 100.0 | 100.0 | 110.0 | 90.0 | 90.0 | 110.0 | 100.0 | 90.0 | 100.0 | 100.0 | 90.0 | 120.0 | 100.0 | 110.0 | 110.0 | 100.0 | 90.0 | 80.0 | 90.0 | 110.0 | 90.0 | 80.0 | 120.0 | 100.0 | 100.0 | 110.0 | 100.0 | 90.0 | 100.0 | 100.0 | 90.0 | 110.0 | 100.0 | 100.0 | 100.0 | … | 100.0 | 90.0 | 100.0 | 100.0 | 90.0 | 100.0 | 100.0 | 100.0 | 90.0 | 100.0 | 100.0 | 90.0 | 120.0 | 80.0 | 100.0 | 100.0 | 90.0 | 90.0 | 100.0 | 90.0 | 90.0 | 110.0 | 80.0 | 110.0 | 100.0 | 120.0 | 90.0 | 110.0 | 90.0 | 90.0 | 100.0 | 100.0 | 90.0 | 110.0 | 80.0 | 100.0 | 110.0 |
| 2025-06-01 00:00:00 CEST | 2025-07-01 00:00:00 CEST | 110.0 | 90.0 | 110.0 | 90.0 | 100.0 | 100.0 | 100.0 | 90.0 | 100.0 | 90.0 | 110.0 | 90.0 | 100.0 | 90.0 | 100.0 | 90.0 | 110.0 | 100.0 | 110.0 | 110.0 | 110.0 | 110.0 | 110.0 | 100.0 | 90.0 | 100.0 | 110.0 | 100.0 | 100.0 | 110.0 | 100.0 | 110.0 | 90.0 | 100.0 | 110.0 | … | 100.0 | 70.0 | 110.0 | 100.0 | 90.0 | 100.0 | 130.0 | 100.0 | 100.0 | 100.0 | 90.0 | 120.0 | 100.0 | 100.0 | 110.0 | 110.0 | 90.0 | 110.0 | 100.0 | 100.0 | 100.0 | 100.0 | 110.0 | 110.0 | 100.0 | 90.0 | 100.0 | 100.0 | 110.0 | 90.0 | 100.0 | 100.0 | 100.0 | 100.0 | 110.0 | 100.0 | 70.0 |

```python {.marimo}
az = Dataset(
    name = 'AZ_drinks',
    data_type = interval_data,
    data = bigger_data,
    attributes=['store','variable','product', 'region'], # <-- this is the clever part
)
az.save()
```

```python {.marimo}
len(az.series)
```

<!-- @output:ZBYS -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">2080</pre>

```python {.marimo}
az_selection = az[{'product': 'tea', 'variable': 'price'}]
az_selection.data
```

<!-- @output:aLJB -->

| valid_from | valid_to | a_price_tea_E | a_price_tea_N | a_price_tea_NE | a_price_tea_NW | a_price_tea_S | a_price_tea_SE | a_price_tea_SW | a_price_tea_W | b_price_tea_E | b_price_tea_N | b_price_tea_NE | b_price_tea_NW | b_price_tea_S | b_price_tea_SE | b_price_tea_SW | b_price_tea_W | c_price_tea_E | c_price_tea_N | c_price_tea_NE | c_price_tea_NW | c_price_tea_S | c_price_tea_SE | c_price_tea_SW | c_price_tea_W | d_price_tea_E | d_price_tea_N | d_price_tea_NE | d_price_tea_NW | d_price_tea_S | d_price_tea_SE | d_price_tea_SW | d_price_tea_W | e_price_tea_E | e_price_tea_N | e_price_tea_NE | … | v_price_tea_NW | v_price_tea_S | v_price_tea_SE | v_price_tea_SW | v_price_tea_W | w_price_tea_E | w_price_tea_N | w_price_tea_NE | w_price_tea_NW | w_price_tea_S | w_price_tea_SE | w_price_tea_SW | w_price_tea_W | x_price_tea_E | x_price_tea_N | x_price_tea_NE | x_price_tea_NW | x_price_tea_S | x_price_tea_SE | x_price_tea_SW | x_price_tea_W | y_price_tea_E | y_price_tea_N | y_price_tea_NE | y_price_tea_NW | y_price_tea_S | y_price_tea_SE | y_price_tea_SW | y_price_tea_W | z_price_tea_E | z_price_tea_N | z_price_tea_NE | z_price_tea_NW | z_price_tea_S | z_price_tea_SE | z_price_tea_SW | z_price_tea_W |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| datetime[ns, UTC] | datetime[ns, UTC] | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | … | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 | f64 |
| 2024-12-31 23:00:00 UTC | 2025-01-31 23:00:00 UTC | 80.0 | 90.0 | 110.0 | 110.0 | 100.0 | 90.0 | 110.0 | 110.0 | 90.0 | 90.0 | 100.0 | 120.0 | 120.0 | 100.0 | 100.0 | 100.0 | 90.0 | 110.0 | 100.0 | 90.0 | 100.0 | 100.0 | 100.0 | 120.0 | 100.0 | 90.0 | 90.0 | 90.0 | 110.0 | 90.0 | 90.0 | 100.0 | 100.0 | 110.0 | 100.0 | … | 110.0 | 100.0 | 100.0 | 120.0 | 100.0 | 100.0 | 90.0 | 100.0 | 100.0 | 100.0 | 90.0 | 100.0 | 110.0 | 90.0 | 90.0 | 80.0 | 100.0 | 100.0 | 90.0 | 90.0 | 100.0 | 110.0 | 100.0 | 110.0 | 110.0 | 100.0 | 120.0 | 110.0 | 100.0 | 120.0 | 90.0 | 110.0 | 100.0 | 110.0 | 90.0 | 100.0 | 100.0 |
| 2025-01-31 23:00:00 UTC | 2025-02-28 23:00:00 UTC | 90.0 | 100.0 | 90.0 | 80.0 | 90.0 | 130.0 | 100.0 | 120.0 | 110.0 | 100.0 | 90.0 | 110.0 | 120.0 | 100.0 | 90.0 | 120.0 | 110.0 | 110.0 | 90.0 | 100.0 | 100.0 | 100.0 | 100.0 | 110.0 | 90.0 | 120.0 | 110.0 | 100.0 | 100.0 | 90.0 | 110.0 | 90.0 | 80.0 | 90.0 | 100.0 | … | 90.0 | 100.0 | 100.0 | 90.0 | 90.0 | 90.0 | 100.0 | 100.0 | 120.0 | 100.0 | 100.0 | 90.0 | 80.0 | 100.0 | 110.0 | 90.0 | 90.0 | 100.0 | 90.0 | 100.0 | 90.0 | 110.0 | 110.0 | 100.0 | 100.0 | 120.0 | 100.0 | 100.0 | 90.0 | 120.0 | 110.0 | 90.0 | 110.0 | 100.0 | 100.0 | 100.0 | 110.0 |
| 2025-02-28 23:00:00 UTC | 2025-03-31 22:00:00 UTC | 110.0 | 110.0 | 110.0 | 80.0 | 100.0 | 110.0 | 100.0 | 80.0 | 90.0 | 100.0 | 100.0 | 100.0 | 110.0 | 80.0 | 100.0 | 90.0 | 100.0 | 100.0 | 120.0 | 80.0 | 80.0 | 100.0 | 90.0 | 100.0 | 100.0 | 90.0 | 100.0 | 90.0 | 100.0 | 100.0 | 100.0 | 110.0 | 100.0 | 100.0 | 100.0 | … | 100.0 | 90.0 | 100.0 | 100.0 | 100.0 | 100.0 | 100.0 | 130.0 | 100.0 | 100.0 | 100.0 | 90.0 | 90.0 | 110.0 | 100.0 | 90.0 | 80.0 | 100.0 | 80.0 | 100.0 | 90.0 | 80.0 | 100.0 | 100.0 | 100.0 | 100.0 | 90.0 | 80.0 | 100.0 | 100.0 | 110.0 | 90.0 | 100.0 | 90.0 | 110.0 | 90.0 | 90.0 |
| 2025-03-31 22:00:00 UTC | 2025-04-30 22:00:00 UTC | 110.0 | 90.0 | 110.0 | 80.0 | 110.0 | 100.0 | 100.0 | 100.0 | 100.0 | 100.0 | 100.0 | 100.0 | 100.0 | 110.0 | 100.0 | 80.0 | 120.0 | 100.0 | 90.0 | 110.0 | 120.0 | 100.0 | 80.0 | 90.0 | 90.0 | 100.0 | 110.0 | 100.0 | 130.0 | 90.0 | 110.0 | 80.0 | 100.0 | 100.0 | 120.0 | … | 100.0 | 80.0 | 100.0 | 90.0 | 110.0 | 110.0 | 110.0 | 100.0 | 90.0 | 90.0 | 100.0 | 90.0 | 100.0 | 100.0 | 110.0 | 100.0 | 90.0 | 100.0 | 110.0 | 90.0 | 100.0 | 100.0 | 110.0 | 120.0 | 90.0 | 110.0 | 110.0 | 100.0 | 110.0 | 110.0 | 110.0 | 100.0 | 100.0 | 100.0 | 100.0 | 80.0 | 100.0 |
| 2025-04-30 22:00:00 UTC | 2025-05-31 22:00:00 UTC | 110.0 | 90.0 | 90.0 | 130.0 | 110.0 | 110.0 | 90.0 | 90.0 | 100.0 | 110.0 | 90.0 | 90.0 | 110.0 | 90.0 | 110.0 | 100.0 | 110.0 | 100.0 | 90.0 | 80.0 | 110.0 | 100.0 | 100.0 | 80.0 | 90.0 | 110.0 | 90.0 | 100.0 | 100.0 | 100.0 | 110.0 | 120.0 | 100.0 | 90.0 | 90.0 | … | 90.0 | 100.0 | 100.0 | 100.0 | 100.0 | 100.0 | 100.0 | 120.0 | 90.0 | 100.0 | 80.0 | 110.0 | 90.0 | 110.0 | 110.0 | 100.0 | 90.0 | 100.0 | 90.0 | 110.0 | 100.0 | 100.0 | 100.0 | 110.0 | 100.0 | 110.0 | 90.0 | 120.0 | 110.0 | 100.0 | 100.0 | 100.0 | 100.0 | 90.0 | 90.0 | 120.0 | 100.0 |
| 2025-05-31 22:00:00 UTC | 2025-06-30 22:00:00 UTC | 100.0 | 100.0 | 100.0 | 100.0 | 110.0 | 110.0 | 110.0 | 70.0 | 110.0 | 110.0 | 110.0 | 100.0 | 100.0 | 100.0 | 90.0 | 110.0 | 110.0 | 100.0 | 110.0 | 110.0 | 110.0 | 110.0 | 100.0 | 100.0 | 90.0 | 100.0 | 110.0 | 110.0 | 80.0 | 110.0 | 100.0 | 100.0 | 80.0 | 100.0 | 90.0 | … | 100.0 | 90.0 | 100.0 | 110.0 | 110.0 | 110.0 | 110.0 | 90.0 | 90.0 | 100.0 | 100.0 | 100.0 | 110.0 | 100.0 | 100.0 | 90.0 | 100.0 | 100.0 | 120.0 | 120.0 | 90.0 | 130.0 | 100.0 | 110.0 | 110.0 | 90.0 | 100.0 | 110.0 | 110.0 | 130.0 | 100.0 | 100.0 | 90.0 | 100.0 | 120.0 | 100.0 | 100.0 |

```python {.marimo}
len(az_selection.series)
```

<!-- @output:nHfw -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">208</pre>

By supplying the `attributes` parameter, we utilised the fact that names were structured as underscore separated strings.
This way, we managed to tag 26 * 2 * 5 attributes across 260 series.

The autotagging is quite powerful.
Additional parameters may be supplied to specify other separators, substitutions, or more complex patterns with regexes.
<!---->
Updating tags after calculations
--------------------------------

```python {.marimo}
prices = Dataset('AZ_drinks')[{'variable':'price'}]
volumes = Dataset('AZ_drinks')[{'variable':'volume'}]
revenues = prices * volumes
```

The new `Dataset` instance `revenues` gets an autogenerated name.
The series names are also inherited from the inputs to the calculation.

```python {.marimo}
print(revenues.name)
print(len(revenues.series), 'series, first five:')
print(revenues.series[:5])
```

<!-- @output:aqbW -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">(COPY of(AZ_drinks SELECTED by names (), pattern: , regex:  tags: &#91;{&#x27;variable&#x27;: &#x27;price&#x27;}&#93;).multiply.COPY of(AZ_drinks SELECTED by names (), pattern: , regex:  tags: &#91;{&#x27;variable&#x27;: &#x27;volume&#x27;}&#93;))
1040 series, first five:
&#91;&#x27;a_price_beer_E&#x27;, &#x27;a_price_beer_N&#x27;, &#x27;a_price_beer_NE&#x27;, &#x27;a_price_beer_NW&#x27;, &#x27;a_price_beer_S&#x27;&#93;
</pre>

```python {.marimo}

```

```python {.marimo}
revenues.rename('AZ Revenue', ('price', 'revenue'))
print(revenues.name)
print(len(revenues.series), 'series, first five:')
print(revenues.series[:5])
```

<!-- @output:TXez -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">AZ Revenue
1040 series, first five:
&#91;&#x27;a_revenue_beer_E&#x27;, &#x27;a_revenue_beer_N&#x27;, &#x27;a_revenue_beer_NE&#x27;, &#x27;a_revenue_beer_NW&#x27;, &#x27;a_revenue_beer_S&#x27;&#93;
</pre>

A similar operation is required for tags:

```python {.marimo}
# DEBUG: tags are lost in selects above, hence not flowing through
revenues.tags["series"]["a_revenue_beer_E"]
```

<!-- @output:yCnT -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">{&#x27;dataset&#x27;: &#x27;AZ Revenue&#x27;,
 &#x27;name&#x27;: &#x27;a_revenue_beer_E&#x27;,
 &#x27;product&#x27;: &#x27;beer&#x27;,
 &#x27;region&#x27;: &#x27;E&#x27;,
 &#x27;repository&#x27;: &#x27;tutorials&#x27;,
 &#x27;store&#x27;: &#x27;a&#x27;,
 &#x27;temporality&#x27;: &#x27;FROM_TO&#x27;,
 &#x27;variable&#x27;: &#x27;price&#x27;,
 &#x27;versioning&#x27;: &#x27;NONE&#x27;}</pre>

```python {.marimo}
# ... tag maintenance is likely to be necessary after calculations:
revenues.replace_tags(({'variable':'price'},{'variable':'revenue'}))
revenues.tags["series"]["a_revenue_beer_E"]
```

<!-- @output:wlCL -->

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">{&#x27;dataset&#x27;: &#x27;AZ Revenue&#x27;,
 &#x27;name&#x27;: &#x27;a_revenue_beer_E&#x27;,
 &#x27;product&#x27;: &#x27;beer&#x27;,
 &#x27;region&#x27;: &#x27;E&#x27;,
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
pqr.detag_series('varegruppe', vare='knekkebrød')
```

```python {.marimo}
pqr.detag_series( vare='knekkebrød' )
pqr.tags
```

<!-- @output:SdmI -->

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
                  &#x27;variabel&#x27;: &#x27;pris&#x27;,
                  &#x27;variable&#x27;: &#x27;price&#x27;,
                  &#x27;versioning&#x27;: &#x27;NONE&#x27;},
            &#x27;q&#x27;: {&#x27;dataset&#x27;: &#x27;PQR&#x27;,
                  &#x27;name&#x27;: &#x27;q&#x27;,
                  &#x27;product&#x27;: &#x27;crispbread&#x27;,
                  &#x27;product group&#x27;: &#91;&#x27;essential&#x27;, &#x27;essentials&#x27;&#93;,
                  &#x27;repository&#x27;: &#x27;tutorials&#x27;,
                  &#x27;temporality&#x27;: &#x27;AT&#x27;,
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
                  &#x27;variabel&#x27;: &#x27;pris&#x27;,
                  &#x27;variable&#x27;: &#x27;price&#x27;,
                  &#x27;versioning&#x27;: &#x27;NONE&#x27;}},
 &#x27;temporality&#x27;: &#x27;AT&#x27;,
 &#x27;varegruppe&#x27;: &#x27;nødvendigheter&#x27;,
 &#x27;variabel&#x27;: &#x27;pris&#x27;,
 &#x27;variable&#x27;: &#x27;price&#x27;,
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

<pre style="white-space: pre-wrap; overflow-wrap: break-word;">&#91;&#x27;AZ_beverages&#x27;,
 &#x27;AZ_drinks&#x27;,
 &#x27;BNO&#x27;,
 &#x27;More Prices and Volumes&#x27;,
 &#x27;POPU06&#x27;,
 &#x27;PQR&#x27;,
 &#x27;Prices and Volumes&#x27;,
 &#x27;SampleDataset&#x27;,
 &#x27;XYZ&#x27;&#93;</pre>

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
| tutorials | AZ_beverages | dataset | {'name': 'AZ_beverages', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'series': {'a_price_beer': {'dataset': 'AZ_beverages', 'name': 'a_price_beer', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'a', 'variable': 'price', 'product': 'beer'}, 'a_price_coffee': {'dataset': 'AZ_beverages', 'name': 'a_price_coffee', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'a', 'variable': 'price', 'product': 'coffee'}, 'a_price_soda': {'dataset': 'AZ_beverages', 'name': 'a_price_soda', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'a', 'variable': 'price', 'product': 'soda'}, 'a_price_tea': {'dataset': 'AZ_beverages', 'name': 'a_price_tea', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'a', 'variable': 'price', 'product': 'tea'}, 'a_price_wine': {'dataset': 'AZ_beverages', 'name': 'a_price_wine', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'a', 'variable': 'price', 'product': 'wine'}, 'a_quantity_beer': {'dataset': 'AZ_beverages', 'name': 'a_quantity_beer', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'a', 'variable': 'quantity', 'product': 'beer'}, 'a_quantity_coffee': {'dataset': 'AZ_beverages', 'name': 'a_quantity_coffee', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'a', 'variable': 'quantity', 'product': 'coffee'}, 'a_quantity_soda': {'dataset': 'AZ_beverages', 'name': 'a_quantity_soda', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'a', 'variable': 'quantity', 'product': 'soda'}, 'a_quantity_tea': {'dataset': 'AZ_beverages', 'name': 'a_quantity_tea', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'a', 'variable': 'quantity', 'product': 'tea'}, 'a_quantity_wine': {'dataset': 'AZ_beverages', 'name': 'a_quantity_wine', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'a', 'variable': 'quantity', 'product': 'wine'}, 'b_price_beer': {'dataset': 'AZ_beverages', 'name': 'b_price_beer', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'b', 'variable': 'price', 'product': 'beer'}, 'b_price_coffee': {'dataset': 'AZ_beverages', 'name': 'b_price_coffee', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'b', 'variable': 'price', 'product': 'coffee'}, 'b_price_soda': {'dataset': 'AZ_beverages', 'name': 'b_price_soda', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'b', 'variable': 'price', 'product': 'soda'}, 'b_price_tea': {'dataset': 'AZ_beverages', 'name': 'b_price_tea', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'b', 'variable': 'price', 'product': 'tea'}, 'b_price_wine': {'dataset': 'AZ_beverages', 'name': 'b_price_wine', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'b', 'variable': 'price', 'product': 'wine'}, 'b_quantity_beer': {'dataset': 'AZ_beverages', 'name': 'b_quantity_beer', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'b', 'variable': 'quantity', 'product': 'beer'}, 'b_quantity_coffee': {'dataset': 'AZ_beverages', 'name': 'b_quantity_coffee', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'b', 'variable': 'quantity', 'product': 'coffee'}, 'b_quantity_soda': {'dataset': 'AZ_beverages', 'name': 'b_quantity_soda', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'b', 'variable': 'quantity', 'product': 'soda'}, 'b_quantity_tea': {'dataset': 'AZ_beverages', 'name': 'b_quantity_tea', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'b', 'variable': 'quantity', 'product': 'tea'}, 'b_quantity_wine': {'dataset': 'AZ_beverages', 'name': 'b_quantity_wine', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'b', 'variable': 'quantity', 'product': 'wine'}, 'c_price_beer': {'dataset': 'AZ_beverages', 'name': 'c_price_beer', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'c', 'variable': 'price', 'product': 'beer'}, 'c_price_coffee': {'dataset': 'AZ_beverages', 'name': 'c_price_coffee', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'c', 'variable': 'price', 'product': 'coffee'}, 'c_price_soda': {'dataset': 'AZ_beverages', 'name': 'c_price_soda', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'c', 'variable': 'price', 'product': 'soda'}, 'c_price_tea': {'dataset': 'AZ_beverages', 'name': 'c_price_tea', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'c', 'variable': 'price', 'product': 'tea'}, 'c_price_wine': {'dataset': 'AZ_beverages', 'name': 'c_price_wine', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'c', 'variable': 'price', 'product': 'wine'}, 'c_quantity_beer': {'dataset': 'AZ_beverages', 'name': 'c_quantity_beer', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'c', 'variable': 'quantity', 'product': 'beer'}, 'c_quantity_coffee': {'dataset': 'AZ_beverages', 'name': 'c_quantity_coffee', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'c', 'variable': 'quantity', 'product': 'coffee'}, 'c_quantity_soda': {'dataset': 'AZ_beverages', 'name': 'c_quantity_soda', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'c', 'variable': 'quantity', 'product': 'soda'}, 'c_quantity_tea': {'dataset': 'AZ_beverages', 'name': 'c_quantity_tea', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'c', 'variable': 'quantity', 'product': 'tea'}, 'c_quantity_wine': {'dataset': 'AZ_beverages', 'name': 'c_quantity_wine', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'c', 'variable': 'quantity', 'product': 'wine'}, 'd_price_beer': {'dataset': 'AZ_beverages', 'name': 'd_price_beer', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'd', 'variable': 'price', 'product': 'beer'}, 'd_price_coffee': {'dataset': 'AZ_beverages', 'name': 'd_price_coffee', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'd', 'variable': 'price', 'product': 'coffee'}, 'd_price_soda': {'dataset': 'AZ_beverages', 'name': 'd_price_soda', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'd', 'variable': 'price', 'product': 'soda'}, 'd_price_tea': {'dataset': 'AZ_beverages', 'name': 'd_price_tea', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'd', 'variable': 'price', 'product': 'tea'}, 'd_price_wine': {'dataset': 'AZ_beverages', 'name': 'd_price_wine', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'd', 'variable': 'price', 'product': 'wine'}, 'd_quantity_beer': {'dataset': 'AZ_beverages', 'name': 'd_quantity_beer', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'd', 'variable': 'quantity', 'product': 'beer'}, 'd_quantity_coffee': {'dataset': 'AZ_beverages', 'name': 'd_quantity_coffee', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'd', 'variable': 'quantity', 'product': 'coffee'}, 'd_quantity_soda': {'dataset': 'AZ_beverages', 'name': 'd_quantity_soda', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'd', 'variable': 'quantity', 'product': 'soda'}, 'd_quantity_tea': {'dataset': 'AZ_beverages', 'name': 'd_quantity_tea', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'd', 'variable': 'quantity', 'product': 'tea'}, 'd_quantity_wine': {'dataset': 'AZ_beverages', 'name': 'd_quantity_wine', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'd', 'variable': 'quantity', 'product': 'wine'}, 'e_price_beer': {'dataset': 'AZ_beverages', 'name': 'e_price_beer', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'e', 'variable': 'price', 'product': 'beer'}, 'e_price_coffee': {'dataset': 'AZ_beverages', 'name': 'e_price_coffee', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'e', 'variable': 'price', 'product': 'coffee'}, 'e_price_soda': {'dataset': 'AZ_beverages', 'name': 'e_price_soda', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'e', 'variable': 'price', 'product': 'soda'}, 'e_price_tea': {'dataset': 'AZ_beverages', 'name': 'e_price_tea', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'e', 'variable': 'price', 'product': 'tea'}, 'e_price_wine': {'dataset': 'AZ_beverages', 'name': 'e_price_wine', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'e', 'variable': 'price', 'product': 'wine'}, 'e_quantity_beer': {'dataset': 'AZ_beverages', 'name': 'e_quantity_beer', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'e', 'variable': 'quantity', 'product': 'beer'}, 'e_quantity_coffee': {'dataset': 'AZ_beverages', 'name': 'e_quantity_coffee', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'e', 'variable': 'quantity', 'product': 'coffee'}, 'e_quantity_soda': {'dataset': 'AZ_beverages', 'name': 'e_quantity_soda', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'e', 'variable': 'quantity', 'product': 'soda'}, 'e_quantity_tea': {'dataset': 'AZ_beverages', 'name': 'e_quantity_tea', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'e', 'variable': 'quantity', 'product': 'tea'}, 'e_quantity_wine': {'dataset': 'AZ_beverages', 'name': 'e_quantity_wine', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'e', 'variable': 'quantity', 'product': 'wine'}, 'f_price_beer': {'dataset': 'AZ_beverages', 'name': 'f_price_beer', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'f', 'variable': 'price', 'product': 'beer'}, 'f_price_coffee': {'dataset': 'AZ_beverages', 'name': 'f_price_coffee', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'f', 'variable': 'price', 'product': 'coffee'}, 'f_price_soda': {'dataset': 'AZ_beverages', 'name': 'f_price_soda', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'f', 'variable': 'price', 'product': 'soda'}, 'f_price_tea': {'dataset': 'AZ_beverages', 'name': 'f_price_tea', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'f', 'variable': 'price', 'product': 'tea'}, 'f_price_wine': {'dataset': 'AZ_beverages', 'name': 'f_price_wine', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'f', 'variable': 'price', 'product': 'wine'}, 'f_quantity_beer': {'dataset': 'AZ_beverages', 'name': 'f_quantity_beer', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'f', 'variable': 'quantity', 'product': 'beer'}, 'f_quantity_coffee': {'dataset': 'AZ_beverages', 'name': 'f_quantity_coffee', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'f', 'variable': 'quantity', 'product': 'coffee'}, 'f_quantity_soda': {'dataset': 'AZ_beverages', 'name': 'f_quantity_soda', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'f', 'variable': 'quantity', 'product': 'soda'}, 'f_quantity_tea': {'dataset': 'AZ_beverages', 'name': 'f_quantity_tea', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'f', 'variable': 'quantity', 'product': 'tea'}, 'f_quantity_wine': {'dataset': 'AZ_beverages', 'name': 'f_quantity_wine', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'f', 'variable': 'quantity', 'product': 'wine'}, 'g_price_beer': {'dataset': 'AZ_beverages', 'name': 'g_price_beer', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'g', 'variable': 'price', 'product': 'beer'}, 'g_price_coffee': {'dataset': 'AZ_beverages', 'name': 'g_price_coffee', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'g', 'variable': 'price', 'product': 'coffee'}, 'g_price_soda': {'dataset': 'AZ_beverages', 'name': 'g_price_soda', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'g', 'variable': 'price', 'product': 'soda'}, 'g_price_tea': {'dataset': 'AZ_beverages', 'name': 'g_price_tea', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'g', 'variable': 'price', 'product': 'tea'}, 'g_price_wine': {'dataset': 'AZ_beverages', 'name': 'g_price_wine', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'g', 'variable': 'price', 'product': 'wine'}, 'g_quantity_beer': {'dataset': 'AZ_beverages', 'name': 'g_quantity_beer', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'g', 'variable': 'quantity', 'product': 'beer'}, 'g_quantity_coffee': {'dataset': 'AZ_beverages', 'name': 'g_quantity_coffee', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'g', 'variable': 'quantity', 'product': 'coffee'}, 'g_quantity_soda': {'dataset': 'AZ_beverages', 'name': 'g_quantity_soda', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'g', 'variable': 'quantity', 'product': 'soda'}, 'g_quantity_tea': {'dataset': 'AZ_beverages', 'name': 'g_quantity_tea', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'g', 'variable': 'quantity', 'product': 'tea'}, 'g_quantity_wine': {'dataset': 'AZ_beverages', 'name': 'g_quantity_wine', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'g', 'variable': 'quantity', 'product': 'wine'}, 'h_price_beer': {'dataset': 'AZ_beverages', 'name': 'h_price_beer', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'h', 'variable': 'price', 'product': 'beer'}, 'h_price_coffee': {'dataset': 'AZ_beverages', 'name': 'h_price_coffee', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'h', 'variable': 'price', 'product': 'coffee'}, 'h_price_soda': {'dataset': 'AZ_beverages', 'name': 'h_price_soda', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'h', 'variable': 'price', 'product': 'soda'}, 'h_price_tea': {'dataset': 'AZ_beverages', 'name': 'h_price_tea', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'h', 'variable': 'price', 'product': 'tea'}, 'h_price_wine': {'dataset': 'AZ_beverages', 'name': 'h_price_wine', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'h', 'variable': 'price', 'product': 'wine'}, 'h_quantity_beer': {'dataset': 'AZ_beverages', 'name': 'h_quantity_beer', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'h', 'variable': 'quantity', 'product': 'beer'}, 'h_quantity_coffee': {'dataset': 'AZ_beverages', 'name': 'h_quantity_coffee', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'h', 'variable': 'quantity', 'product': 'coffee'}, 'h_quantity_soda': {'dataset': 'AZ_beverages', 'name': 'h_quantity_soda', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'h', 'variable': 'quantity', 'product': 'soda'}, 'h_quantity_tea': {'dataset': 'AZ_beverages', 'name': 'h_quantity_tea', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'h', 'variable': 'quantity', 'product': 'tea'}, 'h_quantity_wine': {'dataset': 'AZ_beverages', 'name': 'h_quantity_wine', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'h', 'variable': 'quantity', 'product': 'wine'}, 'i_price_beer': {'dataset': 'AZ_beverages', 'name': 'i_price_beer', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'i', 'variable': 'price', 'product': 'beer'}, 'i_price_coffee': {'dataset': 'AZ_beverages', 'name': 'i_price_coffee', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'i', 'variable': 'price', 'product': 'coffee'}, 'i_price_soda': {'dataset': 'AZ_beverages', 'name': 'i_price_soda', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'i', 'variable': 'price', 'product': 'soda'}, 'i_price_tea': {'dataset': 'AZ_beverages', 'name': 'i_price_tea', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'i', 'variable': 'price', 'product': 'tea'}, 'i_price_wine': {'dataset': 'AZ_beverages', 'name': 'i_price_wine', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'i', 'variable': 'price', 'product': 'wine'}, 'i_quantity_beer': {'dataset': 'AZ_beverages', 'name': 'i_quantity_beer', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'i', 'variable': 'quantity', 'product': 'beer'}, 'i_quantity_coffee': {'dataset': 'AZ_beverages', 'name': 'i_quantity_coffee', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'i', 'variable': 'quantity', 'product': 'coffee'}, 'i_quantity_soda': {'dataset': 'AZ_beverages', 'name': 'i_quantity_soda', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'i', 'variable': 'quantity', 'product': 'soda'}, 'i_quantity_tea': {'dataset': 'AZ_beverages', 'name': 'i_quantity_tea', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'i', 'variable': 'quantity', 'product': 'tea'}, 'i_quantity_wine': {'dataset': 'AZ_beverages', 'name': 'i_quantity_wine', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'i', 'variable': 'quantity', 'product': 'wine'}, 'j_price_beer': {'dataset': 'AZ_beverages', 'name': 'j_price_beer', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'j', 'variable': 'price', 'product': 'beer'}, 'j_price_coffee': {'dataset': 'AZ_beverages', 'name': 'j_price_coffee', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'j', 'variable': 'price', 'product': 'coffee'}, 'j_price_soda': {'dataset': 'AZ_beverages', 'name': 'j_price_soda', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'j', 'variable': 'price', 'product': 'soda'}, 'j_price_tea': {'dataset': 'AZ_beverages', 'name': 'j_price_tea', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'j', 'variable': 'price', 'product': 'tea'}, 'j_price_wine': {'dataset': 'AZ_beverages', 'name': 'j_price_wine', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'j', 'variable': 'price', 'product': 'wine'}, 'j_quantity_beer': {'dataset': 'AZ_beverages', 'name': 'j_quantity_beer', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'j', 'variable': 'quantity', 'product': 'beer'}, 'j_quantity_coffee': {'dataset': 'AZ_beverages', 'name': 'j_quantity_coffee', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'j', 'variable': 'quantity', 'product': 'coffee'}, 'j_quantity_soda': {'dataset': 'AZ_beverages', 'name': 'j_quantity_soda', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'j', 'variable': 'quantity', 'product': 'soda'}, 'j_quantity_tea': {'dataset': 'AZ_beverages', 'name': 'j_quantity_tea', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'j', 'variable': 'quantity', 'product': 'tea'}, 'j_quantity_wine': {'dataset': 'AZ_beverages', 'name': 'j_quantity_wine', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'j', 'variable': 'quantity', 'product': 'wine'}, ...}, 'repository': 'tutorials'} |  | None |
| tutorials | a_price_beer | series | {'dataset': 'AZ_beverages', 'name': 'a_price_beer', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'a', 'variable': 'price', 'product': 'beer'} | AZ_beverages | None |
| tutorials | a_price_coffee | series | {'dataset': 'AZ_beverages', 'name': 'a_price_coffee', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'a', 'variable': 'price', 'product': 'coffee'} | AZ_beverages | None |
| tutorials | a_price_soda | series | {'dataset': 'AZ_beverages', 'name': 'a_price_soda', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'a', 'variable': 'price', 'product': 'soda'} | AZ_beverages | None |
| tutorials | a_price_tea | series | {'dataset': 'AZ_beverages', 'name': 'a_price_tea', 'versioning': 'NONE', 'temporality': 'FROM_TO', 'repository': 'tutorials', 'store': 'a', 'variable': 'price', 'product': 'tea'} | AZ_beverages | None |
| ... | ... | ... | ... | ... | ... |
| tutorials | z | series | {'dataset': 'SampleDataset', 'name': 'z'} | SampleDataset | None |
| tutorials | XYZ | dataset | {'name': 'XYZ', 'versioning': 'NONE', 'temporality': 'AT', 'series': {'x': {'dataset': 'XYZ', 'name': 'x'}, 'y': {'dataset': 'XYZ', 'name': 'y'}, 'z': {'dataset': 'XYZ', 'name': 'z'}}, 'repository': 'tutorials'} |  | None |
| tutorials | x | series | {'dataset': 'XYZ', 'name': 'x'} | XYZ | None |
| tutorials | y | series | {'dataset': 'XYZ', 'name': 'y'} | XYZ | None |
| tutorials | z | series | {'dataset': 'XYZ', 'name': 'z'} | XYZ | None |

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
