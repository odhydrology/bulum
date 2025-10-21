# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) from version 0.3.0 forward.

## Unreleased

### Added

- stats.StochasticDataComparison(): added daily correlation outputs
- stats.StochasticDataComparison(): added monthly statistic charts grouped by timeseries
- stats.StochasticDataComparison(): added optional 'show_bands' argument to show range of outcomes as grey band on statistic charts
- utils: added comprehensive type hints to all datetime and dataframe functions
- utils.interpolation: added proper deprecation warnings with migration guidance

### Fixes

- stats.StochasticDataComparison(): no longer ignores wy_month argument in cropping input datasets
- utils.to_np_datetimes64d(): fixed OverflowError when end_date is 9999-12-31

### Changed

- stats.StochasticDataComparison(): legend in charts is now interactive, subset of input datasets can now be shown
- stats.StochasticDataComparison(): adjusted correlation charts so that input dataset colour is consistent with other charts
- stats.StochasticDataComparison(): sort order of all chart colours now matches input dataset order
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

