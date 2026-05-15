# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) from version 0.3.0 forward.

## [[Unreleased]]

!! IMPORTANT !! - This repo no longer supports Python 3.9 (end of life). The new minimum tested python version is 3.11.

## 0.3.2

### Fixes
- 'Unreleased' below changed to 'v0.3.1' - whoops!
- utils.standardize_datestring_format(): should now return YYYY-MM-DD format strings without timestamp components (e.g., "2000-01-01" instead of "2000-01-01T00:00:00.000000")
- utils.to_np_datetimes64d(): now handles pandas Series inputs correctly by converting to list before processing
- io.read_iqqm_lqn_output(): now correctly processes stochastic model outputs with dates outside numpy datetime64's typical range (see above)

### Changed
- utils.standardize_datestring_format(): now uses numpy datetime64 for efficient processing while ensuring clean YYYY-MM-DD string output
- utils.to_np_datetimes64d(): now issues UserWarning instead of raising ValueError for non-consecutive dates, still returns all dates in the range


## 0.3.1

### Added

- read_iqqm_lqn_output(): `dropna` flag
- stats.StochasticDataComparison(): added daily correlation outputs
- stats.StochasticDataComparison(): added monthly statistic charts grouped by timeseries
- stats.StochasticDataComparison(): added optional 'show_bands' argument to show range of outcomes as grey band on statistic charts
- stats.StorageLevelAssessment: 
    - can apply arbitrary functions to aggregate reults
    - additional parameter for annual calculations to ignore events of length less than parameter
    - added plotting methods `plot_events_ranked` and `plot_event_length_frequency`
    - added `add_trigger()` method
    - added `trigger_names` parameter which is displayed in `Summary()` table
    - mean event length computation
    - minimum event length and water year day count parameters
- utils.interpolation: added proper deprecation warnings with migration guidance
- stats.standardise_datestring_format(): Australian spelling.

### Fixes

- stats.StochasticDataComparison(): no longer ignores wy_month argument in cropping input datasets
- utils.to_np_datetimes64d(): fixed OverflowError when end_date is 9999-12-31

### Changed

- stats.StochasticDataComparison(): legend in charts is now interactive, subset of input datasets can now be shown
- stats.StochasticDataComparison(): adjusted correlation charts so that input dataset colour is consistent with other charts
- stats.StochasticDataComparison(): sort order of all chart colours now matches input dataset order
- utils.standardize_datestring_format(): added fallback when date format is not consistent, likely when excel has saved a timeseries file.
- utils.to_np_datetimes64d(): added check_length parameter

## 0.3.0

### Added

- Sphinx documentation, using autodoc; the `.rst` files are under `sphinx-docs/`.
- Optional argument to ``bulum.io.read_iqqm_lqn_output`` to specify data start line.
- Native Python reader/engine for ``bulum.io.IqqmOutReader``, i.e. no reliance on iqqmgui external program.
- New fn: ``bulum.stats.annual_sum``
- Negflo implementation (was previously a "beta" implementation, so to speak.)

### Fixes

- `bulum.stoch.from_pattern()` no longer uses mutable default arguments.

### Changed

- Updated docstrings to numpy format and for compatibility with Sphinx documentation (ongoing effort).
- Native IDX reader header byte skipping changed - only skips 4 bytes instead of the whole first row, and this time based off file size instead of reading the data and changing it post-hoc.
- IQQM OUT Reader: changed some technically-public variables introduced in various methods to be explicitly private. This is not considered a breaking change.

### Deprecated

- ``bulum.plots.exceedence_plot()``: use ``bulum.plots.exceedance_plot()`` (spelling fix)
- ``bulum.utils.interp()``: use ``np.interp()``
- ``bulum.io.iqqm_out_reader``: use ``bulum.io.IqqmOutReader`` (pythonic naming)

