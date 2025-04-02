# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) from version 0.3.0 forward.

## [Unreleased]

### Added

- Sphinx documentation, using `sphinx.ext.autodoc`; `.rst` files under `sphinx-docs/`

### Fixes

- `bulum.stoch.from_pattern()` no longer uses mutable default arguments.

### Changed

- Updated docstrings to numpy format.

### Deprecated

- ``bulum.plots.exceedence_plot()`` superseded by ``bulum.plots.exceedance_plot()`` (spelling fix)
- Marked ``bulum.utils.interp()`` as deprecated.

