"""
QAStat2s - Hydrology report card comparing modelled and observed flows.

Generates a one-page "report card" figure comparing a modelled flow series
against observed flow (optionally with rainfall), including flow-exceedance
curves, the largest flood events, annual water-year totals, a residual-mass
plot and a table of univariate/bivariate goodness-of-fit statistics.

This subpackage is used like :mod:`bulum.stats.swflo2s` - import it explicitly::

    import bulum.stats.qastat2s as qastat2s

    # From CSV files (read via bulum.io.read_ts_csv):
    fig = qastat2s.report_card(
        obs_file="observed_flow.csv",
        mod_file="modelled_flow.csv",
        rain_file="rainfall.csv",       # optional
        heading="My Catchment",
    )

    # From in-memory data (pandas Series or bulum TimeseriesDataframe columns):
    fig = qastat2s.report_card_from_data(obs=obs_series, mod=mod_series)
"""

from .reportcard_plot import (
    report_card,
    report_card_from_data,
    get_colors,
    create_report_card,
)
