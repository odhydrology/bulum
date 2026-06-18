"""
QAStat - Hydrology Report Card Plotting Module

Creates the report card visualization using matplotlib.
"""

import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import matplotlib.dates as mdates
import numpy as np
from datetime import datetime
from pathlib import Path
from typing import Optional, Union
import pandas as pd

from bulum import io
from .reportcard_data import ReportCardProcessor, ReportCardConfig, FloodEvent, parse_input_file


# Color scheme (matching original R script)
COLORS = {
    'primary': '#3B6E8F',       # Titles, axes, spines, table headers/borders
    'observed': '#00008B',      # Observed flow lines
    'modelled': '#D02090',      # Modelled flow lines
    'rainfall': '#7F7F7F',      # Rainfall data, axis, spine
    'subtle': '#7F7F7F',        # Threshold lines, footnotes
    'text_secondary': '#15467A', # Table body text
    'grid': 'lightgray',
    'white': 'white',
    'black': 'black',
}


class ReportCardPlotter:
    """
    Creates the report card visualization.
    """

    def __init__(self, processor: ReportCardProcessor, colors: Optional[dict] = None):
        self.processor = processor
        self.config = processor.config

        # Merge user colors with defaults
        self.colors = COLORS.copy()
        if colors:
            self.colors.update(colors)

        # Figure dimensions for A4 landscape (in inches)
        # A4 = 297mm x 210mm, at 100 DPI that's ~11.7 x 8.3 inches
        self.fig_width = 11.7
        self.fig_height = 8.3

        # Line widths
        self.lw = 1.5
        self.lw_axes = 1.0
        self.lw_grid = 0.5

    def create_report_card(self, output_path: Optional[str] = None,
                           dpi: int = 300) -> plt.Figure:
        """
        Create the complete report card figure.

        Args:
            output_path: Path to save the figure (PNG, PDF, or TIFF)
            dpi: Resolution for saved figure
        """
        # Create figure with custom layout
        fig = plt.figure(figsize=(self.fig_width, self.fig_height), facecolor=self.colors['white'])

        # Outer GridSpec: separates header from plots with minimal spacing
        gs_outer = gridspec.GridSpec(
            2, 1,
            figure=fig,
            height_ratios=[0.12, 1],
            hspace=0.15,
            left=0.08,
            right=0.94,
            top=0.95,
            bottom=0.08
        )

        # Header row
        ax_header = fig.add_subplot(gs_outer[0])
        self._plot_header(ax_header)

        # Inner GridSpec: 3x3 grid for plots with more spacing
        gs_inner = gridspec.GridSpecFromSubplotSpec(
            3, 3,
            subplot_spec=gs_outer[1],
            height_ratios=[1, 1, 1],
            width_ratios=[1, 1, 1],
            hspace=0.55,
            wspace=0.3
        )

        # Row 0: Exceedance (low flow) | Flood #1 | Flood #2
        ax_exc_low = fig.add_subplot(gs_inner[0, 0])
        self._plot_exceedance_low_flow(ax_exc_low)

        ax_flood1 = fig.add_subplot(gs_inner[0, 1])
        self._plot_flood_event(ax_flood1, self.processor.flood_events[0])

        ax_flood2 = fig.add_subplot(gs_inner[0, 2])
        self._plot_flood_event(ax_flood2, self.processor.flood_events[1])

        # Row 1: Exceedance (high flow) | Annual time series (spans 2 cols)
        ax_exc_high = fig.add_subplot(gs_inner[1, 0])
        self._plot_exceedance_high_flow(ax_exc_high)

        ax_annual = fig.add_subplot(gs_inner[1, 1:])
        self._plot_annual_time_series(ax_annual)

        # Row 2: Statistics table | Residual mass (spans 2 cols)
        ax_stats = fig.add_subplot(gs_inner[2, 0])
        self._plot_statistics_table(ax_stats)

        ax_residual = fig.add_subplot(gs_inner[2, 1:])
        self._plot_residual_mass(ax_residual)

        if output_path:
            fig.savefig(output_path, dpi=dpi, bbox_inches='tight',
                       facecolor=self.colors['white'], edgecolor='none')
            print(f"Report card saved to: {output_path}")

        return fig

    def _plot_header(self, ax: plt.Axes) -> None:
        """Plot the combined header (title, subtitle, availability)."""
        ax.axis('off')
        stats = self.processor.statistics

        # Main title
        ax.text(0.5, 0.75, self.config.heading,
                fontsize=18, fontweight='bold', color=self.colors['primary'],
                ha='center', va='center', transform=ax.transAxes)

        # Subtitle - analysis period
        subtitle = f"Period of analysis: {stats.analysis_start:%d/%m/%Y} to {stats.analysis_end:%d/%m/%Y}"
        ax.text(0.5, 0.4, subtitle,
                fontsize=12, color=self.colors['primary'],
                ha='center', va='center', transform=ax.transAxes)

        # Data availability note
        note = f"(observed flow is available for {stats.percent_available:.1f}% of days in this period)"
        ax.text(0.5, 0.1, note,
                fontsize=9, color=self.colors['primary'],
                ha='center', va='center', transform=ax.transAxes)

    def _plot_exceedance_low_flow(self, ax: plt.Axes) -> None:
        """Plot exceedance curve with log Y scale (showing low flow detail)."""
        exc = self.processor.exceedance

        ax.semilogy(exc.probabilities, exc.obs_values,
                    color=self.colors['observed'], linewidth=self.lw, label='obs')
        ax.semilogy(exc.probabilities, exc.mod_values,
                    color=self.colors['modelled'], linewidth=self.lw, label='mod')

        # Add vertical lines at probability thresholds (subtle gray)
        ax.axvline(x=self.config.p_high, color=self.colors['subtle'],
                   linestyle='--', linewidth=self.lw)
        ax.axvline(x=self.config.p_low, color=self.colors['subtle'],
                   linestyle='--', linewidth=self.lw)

        ax.set_xlabel('Fraction of time flow is equalled or exceeded',
                      fontsize=8, color=self.colors['primary'])
        ax.set_ylabel('Flow (ML/d) - LOG scale', fontsize=8, color=self.colors['primary'])
        ax.set_title('Flow exceedance showing low flow',
                     fontsize=10, color=self.colors['primary'], fontweight='bold')

        ax.set_xlim(0, 1)
        ax.set_ylim(bottom=1)
        ax.grid(True, which='both', linestyle=':', color=self.colors['grid'], linewidth=self.lw_grid)
        self._add_legend(ax)
        self._style_axes(ax)

    def _plot_exceedance_high_flow(self, ax: plt.Axes) -> None:
        """Plot exceedance curve with log X scale (showing high flow detail)."""
        exc = self.processor.exceedance

        # Filter to avoid log(0)
        mask = exc.probabilities > 0
        probs = exc.probabilities[mask]
        obs = exc.obs_values[mask]
        mod = exc.mod_values[mask]

        ax.semilogx(probs, obs, color=self.colors['observed'], linewidth=self.lw, label='obs')
        ax.semilogx(probs, mod, color=self.colors['modelled'], linewidth=self.lw, label='mod')

        # Add vertical lines at probability thresholds (subtle gray)
        ax.axvline(x=self.config.p_high, color=self.colors['subtle'],
                   linestyle='--', linewidth=self.lw)
        ax.axvline(x=self.config.p_low, color=self.colors['subtle'],
                   linestyle='--', linewidth=self.lw)

        ax.set_xlabel('Fraction of time flow is equalled or exceeded - LOG scale',
                      fontsize=8, color=self.colors['primary'])
        ax.set_ylabel('Flow (ML/d)', fontsize=8, color=self.colors['primary'])
        ax.set_title('Flow exceedance showing high flow',
                     fontsize=10, color=self.colors['primary'], fontweight='bold')

        ax.set_xlim(0.0001, 1)
        ax.set_ylim(bottom=0)
        ax.grid(True, which='both', linestyle=':', color=self.colors['grid'], linewidth=self.lw_grid)
        self._add_legend(ax)
        self._style_axes(ax)

    def _plot_flood_event(self, ax: plt.Axes, event: FloodEvent) -> None:
        """Plot a flood event time series with rainfall."""
        dates = event.dates

        # Primary axis: flow
        ax.plot(dates, event.obs_flow, color=self.colors['observed'],
                linewidth=self.lw, label='obs')
        ax.plot(dates, event.mod_flow, color=self.colors['modelled'],
                linewidth=self.lw, label='mod')

        ax.set_ylabel('Flow (GL/d)', fontsize=8, color=self.colors['primary'])
        ax.set_title(f'Largest Flood #{event.rank}',
                     fontsize=10, color=self.colors['primary'], fontweight='bold')

        # Set y-axis limits
        max_flow = max(np.nanmax(event.obs_flow), np.nanmax(event.mod_flow))
        ax.set_ylim(0, max_flow * 1.05)

        # Secondary axis: rainfall (inverted)
        ax2 = ax.twinx()
        rain_max = 1000  # Fixed scale for rainfall

        # Convert rainfall to inverted position
        if event.rainfall is not None and len(event.rainfall) > 0:
            rain_scaled = max_flow * (1 - event.rainfall / rain_max)
            ax2.step(dates, rain_scaled, where='pre',
                    color=self.colors['rainfall'], linewidth=self.lw, label='rainfall')

        ax2.set_ylim(0, max_flow * 1.05)
        ax2.set_ylabel('Rainfall (mm/d)', fontsize=7, color=self.colors['rainfall'], labelpad=2)
        ax2.yaxis.set_ticks([max_flow * (1 - r/rain_max) for r in [0, 200, 400, 600, 800]])
        ax2.yaxis.set_ticklabels(['0', '200', '400', '600', '800'])
        ax2.tick_params(axis='y', colors=self.colors['rainfall'], labelsize=6, pad=1)

        # Style secondary axis - hide all spines except right, make it "float"
        ax2.spines['top'].set_visible(False)
        ax2.spines['bottom'].set_visible(False)
        ax2.spines['left'].set_visible(False)
        ax2.spines['right'].set_color(self.colors['rainfall'])
        ax2.spines['right'].set_linewidth(self.lw_axes)

        # Format x-axis dates
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%d/%m/%Y'))
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=10))

        ax.grid(True, linestyle=':', color=self.colors['grid'], linewidth=self.lw_grid)

        # Combined legend
        lines1, labels1 = ax.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax.legend(lines1 + lines2, labels1 + labels2, loc='upper right',
                  fontsize=6, framealpha=0.8, edgecolor=self.colors['white'], facecolor=self.colors['white'])

        self._style_axes(ax)

        # Override x-axis date label size (must be after _style_axes)
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=0, ha='center', fontsize=5)

    def _plot_annual_time_series(self, ax: plt.Axes) -> None:
        """Plot annual water year totals."""
        wy = self.processor.water_year

        # Plot incomplete years with dashed step lines
        ax.plot(wy.years, wy.obs_totals_with_gaps, color=self.colors['observed'],
                linewidth=self.lw/2, linestyle='--', alpha=0.7, drawstyle='steps-pre')
        ax.plot(wy.years, wy.mod_totals_with_gaps, color=self.colors['modelled'],
                linewidth=self.lw/2, linestyle='--', alpha=0.7, drawstyle='steps-pre')

        # Plot complete years with solid step lines
        ax.plot(wy.years, wy.obs_totals, color=self.colors['observed'],
                linewidth=self.lw, linestyle='-', label='obs', drawstyle='steps-pre')
        ax.plot(wy.years, wy.mod_totals, color=self.colors['modelled'],
                linewidth=self.lw, linestyle='-', label='mod', drawstyle='steps-pre')

        ax.set_xlabel('', fontsize=8, color=self.colors['primary'])
        ax.set_ylabel('Flow (GL/y)', fontsize=8, color=self.colors['primary'])

        # Build title with water year period
        month_names = ['January', 'February', 'March', 'April', 'May', 'June',
                       'July', 'August', 'September', 'October', 'November', 'December']
        start_month = self.config.water_year_start_month
        end_month = start_month - 1 if start_month > 1 else 12
        title = f'Annual time series ({month_names[start_month - 1]} to {month_names[end_month - 1]})'
        ax.set_title(title, fontsize=10, color=self.colors['primary'], fontweight='bold')

        ax.set_ylim(bottom=0)
        ax.grid(True, linestyle=':', color=self.colors['grid'], linewidth=self.lw_grid)

        # Add note about missing data
        ax.text(0.02, 0.95, 'Years with missing data\nrepresented with dotted lines',
                transform=ax.transAxes, fontsize=6, color=self.colors['primary'],
                va='top', ha='left',
                bbox=dict(boxstyle='round', facecolor=self.colors['white'], edgecolor='none', alpha=0.8))

        self._add_legend(ax)
        self._style_axes(ax)

    def _plot_residual_mass(self, ax: plt.Axes) -> None:
        """Plot residual mass series."""
        rm = self.processor.residual_mass

        ax.plot(rm.dates, rm.obs_residual, color=self.colors['observed'],
                linewidth=self.lw, label='obs')
        ax.plot(rm.dates, rm.mod_residual, color=self.colors['modelled'],
                linewidth=self.lw, label='mod')

        # Add zero line
        ax.axhline(y=0, color=self.colors['black'], linewidth=self.lw)

        ax.set_xlabel('', fontsize=8, color=self.colors['primary'])
        ax.set_ylabel('Residual Mass (GL)', fontsize=8, color=self.colors['primary'])
        ax.set_title('Residual mass',
                     fontsize=10, color=self.colors['primary'], fontweight='bold')

        # Format x-axis dates
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%d/%m/%Y'))
        ax.xaxis.set_major_locator(mdates.YearLocator(10))

        ax.grid(True, linestyle=':', color=self.colors['grid'], linewidth=self.lw_grid)
        self._add_legend(ax)
        self._style_axes(ax)

    def _draw_table(self, ax: plt.Axes, data: list, col_widths: list,
                    y_start: float, x_start: float, row_height: float) -> float:
        """
        Draw a table with header row styling and borders.
        Returns the y position of the table bottom.
        """
        table_width = sum(col_widths)

        # Draw header row background
        ax.fill([x_start, x_start + table_width, x_start + table_width, x_start],
                [y_start - row_height/2, y_start - row_height/2,
                 y_start + row_height/2, y_start + row_height/2],
                color=self.colors['primary'], transform=ax.transAxes, clip_on=False)

        # Draw cell text
        for i, row in enumerate(data):
            y = y_start - i * row_height
            x = x_start
            is_header = (i == 0)

            for j, (cell, width) in enumerate(zip(row, col_widths)):
                color = self.colors['white'] if is_header else self.colors['text_secondary']
                fontweight = 'bold' if is_header else 'normal'
                ha = 'left' if j == 0 else 'center'
                x_pos = x + 0.01 if j == 0 else x + width/2

                ax.text(x_pos, y, cell, fontsize=6, fontweight=fontweight,
                        color=color, ha=ha, va='center',
                        transform=ax.transAxes, clip_on=False)
                x += width

        # Draw borders
        y_top = y_start + row_height / 2
        y_bottom = y_start - len(data) * row_height + row_height / 2

        for i in range(len(data) + 1):
            y_line = y_start - i * row_height + row_height / 2
            ax.plot([x_start, x_start + table_width], [y_line, y_line],
                    color=self.colors['primary'], linewidth=0.5,
                    transform=ax.transAxes, clip_on=False)

        x_pos = x_start
        for j in range(len(col_widths) + 1):
            ax.plot([x_pos, x_pos], [y_top, y_bottom],
                    color=self.colors['primary'], linewidth=0.5,
                    transform=ax.transAxes, clip_on=False)
            if j < len(col_widths):
                x_pos += col_widths[j]

        return y_bottom

    def _plot_statistics_table(self, ax: plt.Axes) -> None:
        """Plot the statistics table."""
        ax.axis('off')

        stats = self.processor.statistics
        p = self.processor

        # Univariate statistics data
        uni_data = [
            ['Univariate Statistic', 'Obs', 'Mod\u0394', 'Rating'],
            ['Total Flow (ML)', f'{stats.total_flow_obs:,.0f}', f'{stats.total_flow_bias:.1f}%',
             p.get_rating(stats.total_flow_bias)],
            ['Total Low Flow (ML)*', f'{stats.low_flow_obs:,.0f}', f'{stats.low_flow_bias:.1f}%',
             p.get_rating(stats.low_flow_bias)],
            ['Total Medium Flow (ML)*', f'{stats.med_flow_obs:,.0f}', f'{stats.med_flow_bias:.1f}%',
             p.get_rating(stats.med_flow_bias)],
            ['Total High Flow (ML)*', f'{stats.high_flow_obs:,.0f}', f'{stats.high_flow_bias:.1f}%',
             p.get_rating(stats.high_flow_bias)],
            ['Mean Flow Volume (ML/d)', f'{stats.mean_flow_obs:.0f}', f'{stats.mean_flow_bias:.1f}%',
             p.get_rating(stats.mean_flow_bias)],
            ['Driest 3 Year Mean (ML/d)', f'{stats.driest_3yr_obs:.0f}', f'{stats.driest_3yr_bias:.1f}%',
             p.get_rating(stats.driest_3yr_bias)],
            ['Zero Flow Days (%)+', f'{stats.zero_flow_days_obs:.1f}%', f'{stats.zero_flow_days_diff:.1f}%^',
             p.get_rating(stats.zero_flow_days_diff, is_absolute=True)],
            ['Standard Deviation (ML/d)', f'{stats.std_dev_obs:.0f}', f'{stats.std_dev_bias:.1f}%',
             p.get_rating(stats.std_dev_bias)],
        ]

        # Bivariate statistics data
        bi_data = [
            ['Bivariate Statistic', 'Value', 'Rating'],
            ['Nash-Sutcliffe Efficiency(NSE)', f'{stats.nse:.2f}', p.get_nse_rating(stats.nse)],
            ['Non-matching Zero Flow Days', f'{stats.non_matching_zero_days:.1f}%',
             p.get_rating(stats.non_matching_zero_days, is_absolute=True)],
        ]

        # Table positioning
        row_height = 0.075
        x_start = -0.05

        # Draw univariate table
        uni_bottom = self._draw_table(ax, uni_data, [0.50, 0.19, 0.19, 0.17],
                                       y_start=0.98, x_start=x_start, row_height=row_height)

        # Draw bivariate table (below univariate with gap)
        bi_y_start = uni_bottom - 0.05 - row_height / 2
        bi_bottom = self._draw_table(ax, bi_data, [0.50, 0.275, 0.275],
                                      y_start=bi_y_start, x_start=x_start, row_height=row_height)

        # Add footnotes
        footnotes = [
            f"* Flow exceedance categories: High (0-{self.config.p_high}), Medium ({self.config.p_high}-{self.config.p_low}), Low ({self.config.p_low}-1)",
            "+ Zero flow refers to flow < 1ML/d",
            "^ Absolute difference in percentage between observed and modelled",
        ]

        y_footnote = bi_bottom - 0.035
        for i, note in enumerate(footnotes):
            ax.text(x_start, y_footnote - i * 0.055, note,
                    fontsize=5, color=self.colors['subtle'],
                    ha='left', va='top', transform=ax.transAxes)

    def _style_axes(self, ax: plt.Axes) -> None:
        """Apply consistent styling to axes."""
        ax.tick_params(axis='both', which='major', labelsize=7,
                       colors=self.colors['primary'])
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['bottom'].set_color(self.colors['primary'])
        ax.spines['left'].set_color(self.colors['primary'])
        ax.spines['bottom'].set_linewidth(self.lw_axes)
        ax.spines['left'].set_linewidth(self.lw_axes)

    def _add_legend(self, ax: plt.Axes, fontsize: int = 7) -> None:
        """Add consistently styled legend to axes."""
        ax.legend(loc='upper right', fontsize=fontsize, framealpha=0.8,
                  edgecolor=self.colors['white'], facecolor=self.colors['white'])


def create_report_card(input_file: str, output_file: Optional[str] = None,
                       dpi: int = 300, show: bool = False) -> plt.Figure:
    """
    Create a report card from a .in configuration file.

    .. deprecated::
        Prefer `report_card()` or `report_card_from_data()` for new code.
        This function is retained for command-line usage.

    Args:
        input_file: Path to the .in configuration file
        output_file: Path to save the output (PNG, PDF, TIFF)
        dpi: Resolution for saved figure
        show: Whether to display the figure

    Returns:
        matplotlib Figure object
    """
    # Parse config and process data
    config = parse_input_file(input_file)
    processor = ReportCardProcessor(config)
    processor.process_all()

    # Create plot
    plotter = ReportCardPlotter(processor)
    fig = plotter.create_report_card(output_file, dpi=dpi)

    if show:
        plt.show()

    return fig


def _parse_date(date_str: str) -> Optional[datetime]:
    """Parse a date string in YYYY-MM-DD or DD/MM/YYYY format."""
    for fmt in ['%Y-%m-%d', '%d/%m/%Y']:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None


def get_colors() -> dict:
    """
    Get a copy of the default color palette used by report cards.

    Returns a dictionary with the following keys:
        - 'primary': Titles, axes, spines, table headers/borders
        - 'observed': Observed flow lines
        - 'modelled': Modelled flow lines
        - 'rainfall': Rainfall data, axis, spine
        - 'subtle': Threshold lines, footnotes
        - 'text_secondary': Table body text
        - 'grid': Grid lines
        - 'white': Background color
        - 'black': Text color

    Example:
        >>> import bulum.stats.qastat2s as qastat2s
        >>> colors = qastat2s.get_colors()
        >>> print(colors)
        {'primary': '#3B6E8F', 'observed': '#00008B', ...}

        # Modify and pass to report_card:
        >>> colors['observed'] = '#FF0000'  # Red observed line
        >>> fig = qastat2s.report_card(..., colors=colors)
    """
    return COLORS.copy()


def report_card(
    obs_file: str,
    mod_file: str,
    rain_file: Optional[str] = None,
    heading: str = "Report Card",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    p_low: float = 0.79,
    p_high: float = 0.1,
    water_year_start_month: int = 7,
    colors: Optional[dict] = None,
    output_file: Optional[str] = None,
    dpi: int = 300
) -> plt.Figure:
    """
    Create a report card directly from CSV file paths - ideal for Jupyter notebook use.

    Files are read with :func:`bulum.io.read_ts_csv`, so any date format and
    CSV layout supported by the rest of the bulum ecosystem is accepted. The
    first data column of each file is used (e.g. the flow column of an
    observed-flow CSV that also carries a quality column).

    The returned figure displays inline automatically in Jupyter notebooks.

    Args:
        obs_file: Path to observed flow CSV (first data column = flow in ML/d)
        mod_file: Path to modelled flow CSV (first data column = flow in ML/d)
        rain_file: Path to rainfall CSV (first data column = rainfall in mm/d), optional
        heading: Title for the report card
        start_date: Analysis start date as 'YYYY-MM-DD' or 'DD/MM/YYYY' (auto if None)
        end_date: Analysis end date as 'YYYY-MM-DD' or 'DD/MM/YYYY' (auto if None)
        p_low: Low flow exceedance threshold (default 0.79)
        p_high: High flow exceedance threshold (default 0.1)
        water_year_start_month: Month when water year starts, 1-12 (default 7 = July)
        colors: Custom color palette (dict). Use get_colors() to see keys.
        output_file: Path to save the figure (optional)
        dpi: Resolution for saved/displayed figure (default 300)

    Returns:
        matplotlib Figure object (displays inline in Jupyter)

    Example:
        >>> import bulum.stats.qastat2s as qastat2s
        >>> fig = qastat2s.report_card(
        ...     obs_file="observed_flow.csv",
        ...     mod_file="modelled_flow.csv",
        ...     rain_file="rainfall.csv",
        ...     heading="My Catchment Report Card"
        ... )

        # With custom colors:
        >>> colors = qastat2s.get_colors()
        >>> colors['observed'] = '#FF0000'  # Red observed line
        >>> fig = qastat2s.report_card(..., colors=colors)
    """
    # Read each file into a bulum TimeseriesDataframe and take its first data
    # column. allow_nonnumeric lets us tolerate quality/flag columns alongside
    # the flow column without erroring; only the flow column is used.
    obs = io.read_ts_csv(obs_file, allow_nonnumeric=True).iloc[:, 0]
    mod = io.read_ts_csv(mod_file, allow_nonnumeric=True).iloc[:, 0]
    rain = io.read_ts_csv(rain_file, allow_nonnumeric=True).iloc[:, 0] if rain_file else None

    return report_card_from_data(
        obs=obs,
        mod=mod,
        rain=rain,
        heading=heading,
        start_date=start_date,
        end_date=end_date,
        p_low=p_low,
        p_high=p_high,
        water_year_start_month=water_year_start_month,
        colors=colors,
        output_file=output_file,
        dpi=dpi,
    )


def _coerce_series(data, value_name: str) -> pd.Series:
    """
    Coerce a supported input into a numeric pandas Series with a DatetimeIndex.

    Accepts a pandas Series, or a single-column DataFrame / bulum
    :class:`~bulum.utils.TimeseriesDataframe`. The index may be datetime-like or
    bulum-style ``'YYYY-MM-DD'`` date strings; either way it is converted to a
    DatetimeIndex. Values are coerced to numeric (non-numeric become NaN).

    Args:
        data: A pandas Series, or single-column DataFrame / TimeseriesDataframe.
        value_name: Label used in error messages (e.g. ``"obs"``).

    Returns:
        A numeric pandas Series indexed by a DatetimeIndex.
    """
    if isinstance(data, pd.DataFrame):
        if data.shape[1] != 1:
            raise ValueError(
                f"'{value_name}' must have exactly one data column, "
                f"got {data.shape[1]}: {list(data.columns)}. "
                f"Select a single column, e.g. df[['col']] or df['col']."
            )
        series = data.iloc[:, 0]
    elif isinstance(data, pd.Series):
        series = data
    else:
        raise TypeError(
            f"'{value_name}' must be a pandas Series or single-column DataFrame/"
            f"TimeseriesDataframe, got {type(data).__name__}."
        )

    series = pd.to_numeric(series, errors='coerce')
    series.index = pd.to_datetime(series.index)
    return series


def report_card_from_data(
    obs: Union[pd.Series, pd.DataFrame],
    mod: Union[pd.Series, pd.DataFrame],
    rain: Optional[Union[pd.Series, pd.DataFrame]] = None,
    heading: str = "Report Card",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    p_low: float = 0.79,
    p_high: float = 0.1,
    water_year_start_month: int = 7,
    colors: Optional[dict] = None,
    output_file: Optional[str] = None,
    dpi: int = 300
) -> plt.Figure:
    """
    Create a report card from in-memory data - ideal for Jupyter notebook use.

    This is the native bulum entry point: each of ``obs``, ``mod`` and ``rain``
    may be a pandas Series or a single-column DataFrame / bulum
    :class:`~bulum.utils.TimeseriesDataframe`. The index may be datetime-like or
    bulum-style ``'YYYY-MM-DD'`` date strings.

    The returned figure displays inline automatically in Jupyter notebooks.

    Args:
        obs: Observed flow (values in ML/d) as a Series or single-column
            DataFrame/TimeseriesDataframe.
        mod: Modelled flow (values in ML/d) as a Series or single-column
            DataFrame/TimeseriesDataframe.
        rain: Rainfall (values in mm/d) as a Series or single-column
            DataFrame/TimeseriesDataframe, optional.
        heading: Title for the report card
        start_date: Analysis start date as 'YYYY-MM-DD' or 'DD/MM/YYYY' (auto if None)
        end_date: Analysis end date as 'YYYY-MM-DD' or 'DD/MM/YYYY' (auto if None)
        p_low: Low flow exceedance threshold (default 0.79)
        p_high: High flow exceedance threshold (default 0.1)
        water_year_start_month: Month when water year starts, 1-12 (default 7 = July)
        colors: Custom color palette (dict). Use get_colors() to see keys.
        output_file: Path to save the figure (optional)
        dpi: Resolution for saved/displayed figure (default 300)

    Returns:
        matplotlib Figure object (displays inline in Jupyter)

    Example:
        >>> import bulum.stats.qastat2s as qastat2s
        >>> from bulum import io
        >>> obs = io.read_ts_csv("observed_flow.csv")  # a TimeseriesDataframe
        >>> mod = io.read_ts_csv("modelled_flow.csv")
        >>> fig = qastat2s.report_card_from_data(
        ...     obs=obs[["146012A"]],
        ...     mod=mod[["result_flow"]],
        ...     heading="My Catchment Report Card"
        ... )

        # Plain pandas Series work too:
        >>> fig = qastat2s.report_card_from_data(obs=obs_series, mod=mod_series)
    """
    # Create config programmatically (file paths not used)
    config = ReportCardConfig(
        input_file_path=Path('.'),
        obs_file="",
        mod_file="",
        rain_file="",
        heading=heading,
        p_low=p_low,
        p_high=p_high,
        water_year_start_month=water_year_start_month,
    )

    # Parse dates if provided
    if start_date and end_date:
        config.date_range_auto = False
        config.start_date = _parse_date(start_date)
        config.end_date = _parse_date(end_date)
    else:
        config.date_range_auto = True

    # Create processor and inject data directly. _coerce_series accepts Series or
    # single-column DataFrame/TimeseriesDataframe and normalises to a numeric
    # Series with a DatetimeIndex.
    processor = ReportCardProcessor(config)

    obs_series = _coerce_series(obs, "obs")
    mod_series = _coerce_series(mod, "mod")
    processor.obs_raw = pd.DataFrame({'obs': obs_series})
    processor.mod_raw = pd.DataFrame({'mod': mod_series})

    if rain is not None:
        processor.rain_raw = pd.DataFrame({'rain': _coerce_series(rain, "rain")})
    else:
        # Create empty rainfall series matching obs dates
        processor.rain_raw = pd.DataFrame({'rain': 0.0}, index=processor.obs_raw.index)

    # Skip load_data, go straight to alignment and processing
    processor.align_data()
    processor.compute_exceedance()
    processor.compute_flood_events()
    processor.compute_water_year_totals()
    processor.compute_residual_mass()
    processor.compute_statistics()

    # Create plot
    plotter = ReportCardPlotter(processor, colors=colors)
    fig = plotter.create_report_card(output_file, dpi=dpi)

    # Set figure DPI for notebook display
    fig.set_dpi(dpi)

    return fig


def main():
    """Example usage."""
    import sys

    if len(sys.argv) < 2:
        print("Usage: python reportcard_plot.py <input_file.in> [output_file.png]")
        print("\nRunning with default qa_input.in...")
        input_file = "qa_input.in"
        output_file = "qa_input_python_report_card.png"
    else:
        input_file = sys.argv[1]
        output_file = sys.argv[2] if len(sys.argv) > 2 else input_file.replace('.in', '_report_card.png')

    fig = create_report_card(input_file, output_file, dpi=150, show=True)


if __name__ == "__main__":
    main()
