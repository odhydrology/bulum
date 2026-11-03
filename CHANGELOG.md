# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) from version 0.3.0 forward.

## Unreleased

### Added
- negflo `Negflo.from_flows()`: construct from measured and modelled flows; the residual is missing wherever either input is missing.
- negflo `Negflo.segments`: public attribute holding the SM6 segments.

### Changed
- negflo `Negflo`: reimplemented around a single event-based algorithm, verified against NEGFLO6 rebuilt from its C source. Missing data (NaN) is now handled consistently by every method: missing days contribute nothing to any total, break flow events, stay missing in the results, and negatives are carried across them. NEGFLO6's other known bugs are not reproduced (see the module documentation).
- negflo `Negflo.neg_overflows` renamed to `Negflo.unallocated_negatives`. It now reports, for every method, all negative volume that was not redistributed (including volume discarded by SM3/SM5 and negatives outside SM6 segments), so that `sum(result) == sum(residual) - unallocated negatives`.
- negflo `Negflo` SM2-SM5/SM7: flows equal to the flow limit no longer form part of a flow event (matching NEGFLO6).
- negflo `Negflo.sm4()`: negative flows before the first event are no longer carried into it; as in NEGFLO6, they have no preceding event and are not redistributed (they are reported in the unallocated negatives).
- negflo `Negflo.sm1()`: the flow limit is no longer used, matching the NEGFLO6 documentation and making SM6 equivalent to SM1 within each segment.
- negflo `Negflo.sm6()`: negative flows outside every segment are now set to zero; generated segments are no longer stored on the instance.
- negflo `Negflo.sm7()`: an event can absorb negative flows from both sides.
- negflo `Negflo`: unredistributed negative flow is logged at INFO rather than WARNING level.
- negflo `Negflo`: `segments` is now keyword-only, and a negative `flow_limit` raises `ValueError`.

### Removed
- negflo `ContiguousIndexTracker`, `NegfloFileType`, `Negflo.log()`, the unfinished private `Negflo._from_config_file()`, and the unused `num_segments` parameter.

### Fixed
- negflo `Negflo.sm1()`: a single missing value no longer sets the whole series to zero.
- negflo `Negflo` SM2/SM4: a positive period followed by a gap is no longer discarded without being smoothed.
- negflo `Negflo.run_all()`: now includes CL1 and no longer raises `NotImplementedError` at the end.
- negflo `Negflo.to_file()`: an `out_filename` with an extension no longer has a second extension appended.

## 0.3.3

!! IMPORTANT !! - This repo no longer supports Python 3.9 (end of life). The new minimum tested python version is 3.11.

### Added
- utils.get_wy() now accepts a single string argument, returning a single integer
- utils.DataframeEnsemble.map() - apply function over all dataframes in ensemble. Note this is distinct to the inbuilt `map` as it returns a new DataframeEnsemble.
- utils.TimeseriesDataframe.tsdf_apply() which does not clobber metadata - see also changes in Changed subsection.
- utils.TimeseriesDataframe and utils.DataframeEnsemble - serialisation and deserialisation methods with unzipped folder, zip folder, pickle and json save formats.
- utils.to_np_datetimes64d(): added `mode` parameter with "generate" (default) and "parse" options
  - "generate" mode: Creates all dates between first and last date (default behavior)
  - "parse" mode: Individually parses each date string, preserving gaps and non-consecutive dates
- utils.to_np_datetimes64d(): added `check_dates` parameter with three validation modes
  - `False`: No validation (suppress all warnings/errors)
  - `True` or `"warn"`: Issue UserWarning if lengths don't match (default, backward compatible)
  - `"strict"`: Raise ValueError if lengths don't match
- io: roundtrip tests for all IO reader/writer pairs (idx_io, res_csv_io, csv_io) to detect data size issues
- iqqm_out_reader: Converted from os.path to pathlib
- iqqm_out_reader: Improved IQN file parsing robustness by filtering comment lines instead of using hardcoded line indices
- utils.standardise_datestring_format() as_index argument to coerce to Index
- utils.get_wy() as_list argument to return coerced list or numpy array (used in calculation).
- negflo `Negflo`: `sm*`, `rw1`, and `cl1` methods now return the resulting `pd.DataFrame`, enabling functional-style usage (e.g. `df = negflo.sm2()`).
- negflo `Negflo.df_residual`: exposed as a read-only property; direct assignment now raises `AttributeError`.
- negflo `Negflo.to_file()`: new `folder` keyword parameter to specify the output directory when using auto-named files.
- negflo `Negflo.run_all()`: new `folder` parameter (replaces the old `filename` prefix parameter) to place all output files in a directory.

### Changed
- utils.get_wy(using_end_year) is now a keyword argument
- utils.get_wy() now accepts `as_list` keyword argument (default True) to return a numpy array instead of a list; implementation vectorised with numpy
- utils.TimeseriesDataframe.add_tag() now accepts lists of strings as tags
- utils.TimeseriesDataframe now defines _constructor and _metadata - standard pandas operations will now return a TimeseriesDataframe as opposed to a pandas.DataFrame for most standard operations.
- utils.DataframeEnsemble - None and bool is now explicitly unsupported as key values - this can be overriden by assigning directly to the underlying ensemble dict but compatibility of methods is not guaranteed
- utils.to_np_datetimes64d(): `check_dates` parameter only applies in "generate" mode (ignored in "parse" mode, default behaviour checks dates)
- utils.standardize_datestring_format(): now uses `check_dates=False` internally for non-consecutive date support
- utils.get_wy(): now uses `check_dates=False` internally for non-consecutive date support
- io: consolidated idx_io_native.py into idx_io.py (all IDX I/O now in single file)
- io.idx_io: updated module docstring to clarify support for reading IQQM .OUT binary files
- io.read_res_csv() now raises error instead of silently failing
- negflo `Negflo`: each `sm*`, `rw1`, and `cl1` call now automatically resets from the stored raw residual before computing, so repeated or interleaved calls are independent of one another.
- negflo `Negflo`: `sm*`, `rw1`, and `cl1` now return a copy of the residual dataframe, so multiple results held simultaneously are independent of each other and of `df_residual`.
- negflo `Negflo.df_residual`: property now returns a copy, making the read-only guarantee meaningful.
- negflo `Negflo.run_all()`: parameter renamed from `filename` (string prefix) to `folder` (output directory, default `"."`); file naming is now delegated to `to_file()`.

### Fixed
- utils.TimeseriesDataframe: arithmetic operations with pandas Series (e.g., `tsdf - tsdf.mean()`) now correctly preserve metadata. Overridden arithmetic operators (`__add__`, `__sub__`, `__mul__`, `__truediv__`, etc.) ensure metadata is preserved for operations with scalars and Series. Note: binary operations between two TimeseriesDataframes have no guarantees about which operand's metadata is preserved.
- io.write_idx_native(): fixed bug that doubled data size on round-trip by using structured arrays instead of plain numpy arrays
- io._detect_header_bytes(): fixed incorrect header byte detection for single-column IDX files
- negflo - longstanding bug with forward/backward implementations creating new rows instead of modifying residual.
- negflo `Negflo` SM4/SM5: when the series ends on a positive value with `carry_negative=True`, remaining negative accumulation is now distributed into the trailing positive period rather than going to overflow.


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

