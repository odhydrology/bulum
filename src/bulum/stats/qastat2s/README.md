# QAStat2s Report Card

`qastat2s` is the bulum subpackage for generating hydrology report cards that
compare observed and modelled streamflow. It is part of the
[bulum](https://github.com/odhydrology/bulum) ecosystem.

## Installation

Ships with bulum:
```
pip install bulum
```

## Quick Start

```python
import bulum.stats.qastat2s as qastat2s

fig = qastat2s.report_card(
    obs_file="observed_flow.csv",
    mod_file="modelled_flow.csv",
    rain_file="rainfall.csv",  # optional
    heading="My Catchment"
)
```

CSV files are read with `bulum.io.read_ts_csv`, so any date format and layout
supported by the rest of bulum is accepted. The first data column of each file
is used as the flow/rainfall series.

## API Reference

### `qastat2s.report_card()`

Create a report card from CSV files.

```python
report_card(
    obs_file,                     # Path to observed flow CSV
    mod_file,                     # Path to modelled flow CSV
    rain_file=None,               # Path to rainfall CSV (optional)
    heading="Report Card",        # Title text
    start_date=None,              # Analysis start, e.g. "2000-01-01"
    end_date=None,                # Analysis end, e.g. "2020-12-31"
    p_low=0.79,                   # Low flow exceedance threshold
    p_high=0.1,                   # High flow exceedance threshold
    water_year_start_month=7,     # 1-12, default July (Australian)
    colors=None,                  # Custom color palette (dict)
    output_file=None,             # Save path, e.g. "report.png"
    dpi=300                       # Resolution
)
```

### `qastat2s.report_card_from_data()`

Create a report card from in-memory data (useful when data is already loaded).
Each input may be a pandas `Series` or a single-column `DataFrame` / bulum
`TimeseriesDataframe`; the index may be datetime-like or bulum-style
`'YYYY-MM-DD'` date strings.

```python
import bulum.stats.qastat2s as qastat2s
from bulum import io

obs = io.read_ts_csv("observed_flow.csv")   # a TimeseriesDataframe
mod = io.read_ts_csv("modelled_flow.csv")

fig = qastat2s.report_card_from_data(
    obs=obs[["146012A"]],     # Series or single-column TSDF/DataFrame (ML/d)
    mod=mod[["result_flow"]], # Series or single-column TSDF/DataFrame (ML/d)
    rain=None,                # optional (mm/d)
    heading="My Catchment",
    # ... same optional parameters as report_card()
)
```

### `qastat2s.get_colors()`

Get the default color palette for customization.

```python
import bulum.stats.qastat2s as qastat2s

colors = qastat2s.get_colors()
# {'primary': '#3B6E8F', 'observed': '#00008B', 'modelled': '#D02090', ...}

colors['observed'] = '#FF0000'  # Red observed line
colors['modelled'] = '#00FF00'  # Green modelled line

fig = qastat2s.report_card(..., colors=colors)
```

**Color keys:**
| Key | Default | Usage |
|-----|---------|-------|
| `primary` | `#3B6E8F` | Titles, axes, table headers |
| `observed` | `#00008B` | Observed flow lines |
| `modelled` | `#D02090` | Modelled flow lines |
| `rainfall` | `#7F7F7F` | Rainfall data and axis |
| `subtle` | `#7F7F7F` | Threshold lines, footnotes |
| `text_secondary` | `#15467A` | Table body text |
| `grid` | `lightgray` | Grid lines |

## CSV Format

All CSV files should have:
- First column: dates (DD/MM/YYYY or YYYY-MM-DD)
- Second column: values (flow in ML/d, rainfall in mm/d)
- Header row

## Water Year

The `water_year_start_month` parameter controls the annual aggregation period:
- `7` (default): July-June (Australian convention)
- `10`: October-September (US convention)
- `1`: Calendar year

## Command Line (legacy `.in` files)

The original QAStat `.in` configuration files are still supported via the
module's command-line entry point:

```bash
python -m bulum.stats.qastat2s.reportcard_plot input.in output.png
```
