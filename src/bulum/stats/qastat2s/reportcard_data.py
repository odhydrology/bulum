"""
QAStat - Hydrology Report Card Data Processing Module

This module handles:
1. Parsing input configuration files (.in format)
2. Reading and aligning observed, modelled, and rainfall time series
3. Computing derived datasets for report card plots and statistics
"""

import pandas as pd
import numpy as np
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional, Tuple, List, Dict
from datetime import datetime, timedelta

from bulum import utils


@dataclass
class ReportCardConfig:
    """Configuration parsed from .in input file"""
    input_file_path: Path
    date_range_auto: bool = True
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    percentiles_auto: bool = True
    p_low: float = 0.79  # Low flow threshold (exceedance probability)
    p_high: float = 0.1  # High flow threshold (exceedance probability)
    num_lags: int = 10
    num_flow_bands: int = 0
    num_seasons: int = 4
    season_starts: List[int] = field(default_factory=lambda: [0, 3, 6, 9])
    obs_file: str = ""
    obs_format: int = 3
    obs_column: int = 0
    mod_file: str = ""
    mod_format: int = 3
    mod_column: int = 0
    rain_file: str = ""
    rain_format: int = 3
    rain_column: int = 0
    heading: str = "Report Card"
    output_flags: List[int] = field(default_factory=lambda: [1, 1, 1, 1, 1, 10])
    water_year_start_month: int = 7  # Month when water year starts (1-12, default 7 = July)


@dataclass
class FloodEvent:
    """Data for a flood event window"""
    start_date: datetime
    peak_date: datetime
    end_date: datetime
    obs_flow: np.ndarray
    mod_flow: np.ndarray
    rainfall: np.ndarray
    dates: pd.DatetimeIndex
    peak_value: float
    rank: int  # 1 = largest, 2 = second largest


@dataclass
class WaterYearData:
    """Annual water year totals (July-June)"""
    years: np.ndarray  # Water year (year of July)
    obs_totals: np.ndarray  # GL/year
    mod_totals: np.ndarray  # GL/year
    complete_years: np.ndarray  # Boolean mask for years with complete data
    obs_totals_with_gaps: np.ndarray  # Includes incomplete years
    mod_totals_with_gaps: np.ndarray


@dataclass
class ExceedanceData:
    """Exceedance curve data"""
    probabilities: np.ndarray  # Exceedance probability (0 to 1)
    obs_values: np.ndarray  # Sorted observed values (descending)
    mod_values: np.ndarray  # Sorted modelled values (descending)
    n_points: int


@dataclass
class ResidualMassData:
    """Residual mass series data"""
    dates: pd.DatetimeIndex
    obs_residual: np.ndarray  # Cumulative obs - cumulative mean
    mod_residual: np.ndarray  # Cumulative mod - cumulative mean


@dataclass
class Statistics:
    """Computed statistics for the report card"""
    # Univariate statistics
    total_flow_obs: float  # ML
    total_flow_mod: float
    total_flow_bias: float  # %

    low_flow_obs: float  # ML
    low_flow_mod: float
    low_flow_bias: float  # %

    med_flow_obs: float  # ML
    med_flow_mod: float
    med_flow_bias: float  # %

    high_flow_obs: float  # ML
    high_flow_mod: float
    high_flow_bias: float  # %

    mean_flow_obs: float  # ML/d
    mean_flow_mod: float
    mean_flow_bias: float  # %

    driest_3yr_obs: float  # ML/d
    driest_3yr_mod: float
    driest_3yr_bias: float  # %

    zero_flow_days_obs: float  # %
    zero_flow_days_mod: float
    zero_flow_days_diff: float  # Absolute difference in %

    std_dev_obs: float  # ML/d
    std_dev_mod: float
    std_dev_bias: float  # %

    # Bivariate statistics
    nse: float  # Nash-Sutcliffe Efficiency
    non_matching_zero_days: float  # %

    # Metadata
    analysis_start: datetime
    analysis_end: datetime
    total_days: int
    valid_days: int
    percent_available: float


def parse_input_file(filepath: str | Path) -> ReportCardConfig:
    """
    Parse the .in configuration file.

    Format:
        Line 1: Version/flag
        Line 2: Date range (* = auto, or start_date end_date)
        Line 3: Percentiles (* = auto, or p_low p_high)
        Line 4: Number of lags
        Line 5: Number of flow bands
        Line 6: Number of seasons
        Line 7: Season start months
        Line 8: Percentiles for seasons (ignored for now)
        Line 9: Observed data format (format_type column_index)
        Line 10: Observed data filename
        Line 11: Heading/title
        Line 12: Modelled data format
        Line 13: Modelled data filename
        Line 14: Rainfall data format
        Line 15: Rainfall data filename
        Line 16: Output flags
    """
    filepath = Path(filepath)
    config = ReportCardConfig(input_file_path=filepath)

    with open(filepath, 'r') as f:
        lines = f.readlines()

    # Strip comments (everything after //) and whitespace
    def clean_line(line: str) -> str:
        if '//' in line:
            line = line[:line.index('//')]
        return line.strip()

    cleaned = [clean_line(line) for line in lines]

    # Line 1: Version (skip for now)
    # Line 2: Date range
    if cleaned[1] == '*':
        config.date_range_auto = True
    else:
        parts = cleaned[1].split()
        config.date_range_auto = False
        config.start_date = datetime.strptime(parts[0], '%d/%m/%Y')
        config.end_date = datetime.strptime(parts[1], '%d/%m/%Y')

    # Line 3: Percentiles
    if cleaned[2] == '*':
        config.percentiles_auto = True
    else:
        parts = cleaned[2].split()
        config.percentiles_auto = False
        config.p_high = float(parts[0])
        config.p_low = float(parts[1])

    # Line 4: Number of lags
    config.num_lags = int(cleaned[3])

    # Line 5: Number of flow bands
    config.num_flow_bands = int(cleaned[4])

    # Line 6: Number of seasons
    config.num_seasons = int(cleaned[5])

    # Line 7: Season start months
    config.season_starts = [int(x) for x in cleaned[6].split()]

    # Line 8: Season percentiles (skip for now)

    # Line 9-10: Observed data
    obs_parts = cleaned[8].split()
    config.obs_format = int(obs_parts[0])
    config.obs_column = int(obs_parts[1])
    config.obs_file = cleaned[9]

    # Line 11: Heading
    config.heading = cleaned[10]

    # Line 12-13: Modelled data
    mod_parts = cleaned[11].split()
    config.mod_format = int(mod_parts[0])
    config.mod_column = int(mod_parts[1])
    config.mod_file = cleaned[12]

    # Line 14-15: Rainfall data
    rain_parts = cleaned[13].split()
    config.rain_format = int(rain_parts[0])
    config.rain_column = int(rain_parts[1])
    config.rain_file = cleaned[14]

    # Line 16: Output flags
    config.output_flags = [int(x) for x in cleaned[15].split()]

    return config


def read_timeseries_csv(filepath: str | Path, format_type: int,
                         data_column: int) -> pd.DataFrame:
    """
    Read a time series CSV file.

    Format types:
        3: CSV with header row, date in first column

    Returns DataFrame with 'date' index and 'value' column.
    """
    filepath = Path(filepath)

    if format_type == 3:
        # CSV with header
        df = pd.read_csv(filepath)
        date_col = df.columns[0]
        value_col = df.columns[data_column + 1]  # +1 because column 0 is date

        df['date'] = pd.to_datetime(df[date_col], dayfirst=True)
        df['value'] = pd.to_numeric(df[value_col], errors='coerce')
        df = df[['date', 'value']].set_index('date')

    else:
        # Assume headerless CSV with date in column 0
        df = pd.read_csv(filepath, header=None)
        df['date'] = pd.to_datetime(df.iloc[:, 0], dayfirst=True)
        df['value'] = pd.to_numeric(df.iloc[:, data_column + 1], errors='coerce')
        df = df[['date', 'value']].set_index('date')

    return df


class ReportCardProcessor:
    """
    Main class for processing hydrology report card data.

    Handles data loading, alignment, and computation of derived datasets.
    """

    def __init__(self, config: ReportCardConfig):
        self.config = config
        self.base_path = config.input_file_path.parent

        # Raw data
        self.obs_raw: Optional[pd.DataFrame] = None
        self.mod_raw: Optional[pd.DataFrame] = None
        self.rain_raw: Optional[pd.DataFrame] = None

        # Aligned data (common date range, NaN where missing)
        self.aligned_data: Optional[pd.DataFrame] = None

        # Analysis period
        self.start_date: Optional[datetime] = None
        self.end_date: Optional[datetime] = None

        # Computed datasets
        self.exceedance: Optional[ExceedanceData] = None
        self.flood_events: List[FloodEvent] = []
        self.water_year: Optional[WaterYearData] = None
        self.residual_mass: Optional[ResidualMassData] = None
        self.statistics: Optional[Statistics] = None

        # Cached paired data (days where both obs and mod available)
        self._paired_data: Optional[pd.DataFrame] = None

        # Zero flow threshold (ML/d)
        self.zero_threshold = 1.0

    def load_data(self) -> None:
        """Load all input CSV files."""
        self.obs_raw = read_timeseries_csv(
            self.base_path / self.config.obs_file,
            self.config.obs_format,
            self.config.obs_column
        )
        self.obs_raw.columns = ['obs']

        self.mod_raw = read_timeseries_csv(
            self.base_path / self.config.mod_file,
            self.config.mod_format,
            self.config.mod_column
        )
        self.mod_raw.columns = ['mod']

        self.rain_raw = read_timeseries_csv(
            self.base_path / self.config.rain_file,
            self.config.rain_format,
            self.config.rain_column
        )
        self.rain_raw.columns = ['rain']

    def align_data(self) -> None:
        """
        Align observed, modelled, and rainfall data to a common date range.

        The analysis period is determined by:
        - If auto: intersection of obs and mod date ranges
        - If specified: use config dates

        Creates self.aligned_data DataFrame with columns: obs, mod, rain
        Missing values are NaN.
        """
        if self.obs_raw is None:
            self.load_data()

        # Determine analysis period
        if self.config.date_range_auto:
            # Use intersection of obs and mod ranges
            obs_start = self.obs_raw.index.min()
            obs_end = self.obs_raw.index.max()
            mod_start = self.mod_raw.index.min()
            mod_end = self.mod_raw.index.max()

            self.start_date = max(obs_start, mod_start)
            self.end_date = min(obs_end, mod_end)
        else:
            self.start_date = pd.Timestamp(self.config.start_date)
            self.end_date = pd.Timestamp(self.config.end_date)

        # Create complete date range
        date_range = pd.date_range(self.start_date, self.end_date, freq='D')

        # Merge all data
        self.aligned_data = pd.DataFrame(index=date_range)
        self.aligned_data = self.aligned_data.join(self.obs_raw, how='left')
        self.aligned_data = self.aligned_data.join(self.mod_raw, how='left')
        self.aligned_data = self.aligned_data.join(self.rain_raw, how='left')

        self.aligned_data.index.name = 'date'

    def get_paired_data(self) -> pd.DataFrame:
        """
        Get data where BOTH observed and modelled values are available.
        Used for exceedance curves and most statistics. Result is cached.
        """
        if self._paired_data is not None:
            return self._paired_data

        if self.aligned_data is None:
            self.align_data()

        mask = self.aligned_data['obs'].notna() & self.aligned_data['mod'].notna()
        self._paired_data = self.aligned_data[mask].copy()
        return self._paired_data

    def compute_exceedance(self) -> ExceedanceData:
        """
        Compute exceedance curve data.

        Only includes days where both obs and mod are available.
        Values are sorted descending, probabilities are Weibull plotting positions.
        """
        paired = self.get_paired_data()
        n = len(paired)

        # Sort both series independently (descending)
        obs_sorted = np.sort(paired['obs'].values)[::-1]
        mod_sorted = np.sort(paired['mod'].values)[::-1]

        # Weibull plotting position: P = rank / (n + 1)
        ranks = np.arange(1, n + 1)
        probabilities = ranks / (n + 1)

        self.exceedance = ExceedanceData(
            probabilities=probabilities,
            obs_values=obs_sorted,
            mod_values=mod_sorted,
            n_points=n
        )
        return self.exceedance

    def compute_flood_events(self, window_days: int = 51,
                             num_events: int = 2) -> List[FloodEvent]:
        """
        Find the largest flood events based on observed peak flow.

        For each event, extracts a window of data centered (approximately)
        on the peak, including observed, modelled, and rainfall.

        Args:
            window_days: Number of days in the event window
            num_events: Number of events to find (default 2)
        """
        if self.aligned_data is None:
            self.align_data()

        data = self.aligned_data.copy()
        obs = data['obs'].copy()

        self.flood_events = []
        half_window = window_days // 2

        for rank in range(1, num_events + 1):
            # Find peak (maximum observed flow)
            peak_idx = obs.idxmax()
            peak_value = obs[peak_idx]

            # Define window
            start_date = peak_idx - timedelta(days=half_window - 10)  # Asymmetric window
            end_date = start_date + timedelta(days=window_days - 1)

            # Ensure within data range
            start_date = max(start_date, data.index.min())
            end_date = min(end_date, data.index.max())

            # Extract window data
            window_data = data.loc[start_date:end_date]

            event = FloodEvent(
                start_date=start_date,
                peak_date=peak_idx,
                end_date=end_date,
                obs_flow=window_data['obs'].values / 1000,  # Convert ML/d to GL/d
                mod_flow=window_data['mod'].values / 1000,
                rainfall=window_data['rain'].values if 'rain' in window_data else np.zeros(len(window_data)),
                dates=window_data.index,
                peak_value=peak_value,
                rank=rank
            )
            self.flood_events.append(event)

            # Mask out this event's window for finding next peak
            mask_start = peak_idx - timedelta(days=window_days)
            mask_end = peak_idx + timedelta(days=window_days)
            obs.loc[mask_start:mask_end] = np.nan

        return self.flood_events

    def _assign_water_year(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add a 'water_year' column to a DataFrame with DatetimeIndex.

        Uses bulum's shared water-year convention (:func:`bulum.utils.get_wy`),
        where the water year is labelled by the year of its start month.
        """
        df = df.copy()
        df['water_year'] = utils.get_wy(df.index, self.config.water_year_start_month)
        return df

    def compute_water_year_totals(self) -> WaterYearData:
        """
        Compute annual totals using water years.

        Water year start month is configurable (default July for Australian convention).
        Water year is labelled by the year of the start month.

        Returns totals only for years with complete data, plus separate arrays
        including incomplete years (for dashed line plotting).
        """
        if self.aligned_data is None:
            self.align_data()

        data = self._assign_water_year(self.aligned_data)
        start_month = self.config.water_year_start_month

        # Group by water year
        grouped = data.groupby('water_year')

        # Calculate totals (ML -> GL, so divide by 1000)
        obs_totals = grouped['obs'].sum() / 1000
        mod_totals = grouped['mod'].sum() / 1000

        # Count valid days per water year
        obs_days = grouped['obs'].count()
        mod_days = grouped['mod'].count()

        # Expected days per water year (accounting for leap years)
        def water_year_days(wy):
            # Determine which calendar year contains February
            # If start_month > 2, Feb is in wy+1; otherwise Feb is in wy
            feb_year = wy + 1 if start_month > 2 else wy
            is_leap = (feb_year % 4 == 0 and feb_year % 100 != 0) or (feb_year % 400 == 0)
            return 366 if is_leap else 365

        years = obs_totals.index.values
        expected_days = np.array([water_year_days(wy) for wy in years])

        # Complete years: both obs and mod have all days
        complete_mask = (obs_days.values == expected_days) & (mod_days.values == expected_days)

        # Prepare totals with gaps (set incomplete years to NaN for complete-only series)
        obs_complete = obs_totals.values.copy()
        mod_complete = mod_totals.values.copy()
        obs_complete[~complete_mask] = np.nan
        mod_complete[~complete_mask] = np.nan

        self.water_year = WaterYearData(
            years=years,
            obs_totals=obs_complete,
            mod_totals=mod_complete,
            complete_years=complete_mask,
            obs_totals_with_gaps=obs_totals.values,
            mod_totals_with_gaps=mod_totals.values
        )
        return self.water_year

    def compute_residual_mass(self) -> ResidualMassData:
        """
        Compute residual mass series.

        Residual mass = cumulative sum - cumulative mean
        This highlights long-term trends and bias between series.

        Only uses days where both obs and mod are available.
        """
        paired = self.get_paired_data()

        # Convert to GL for plotting
        obs_gl = paired['obs'].values / 1000
        mod_gl = paired['mod'].values / 1000

        # Cumulative sums
        obs_cumsum = np.cumsum(obs_gl)
        mod_cumsum = np.cumsum(mod_gl)

        # Residual mass = cumsum - (mean * n) where mean is overall mean
        n = np.arange(1, len(obs_gl) + 1)
        obs_mean = np.mean(obs_gl)
        mod_mean = np.mean(mod_gl)

        obs_residual = obs_cumsum - obs_mean * n
        mod_residual = mod_cumsum - mod_mean * n

        self.residual_mass = ResidualMassData(
            dates=paired.index,
            obs_residual=obs_residual,
            mod_residual=mod_residual
        )
        return self.residual_mass

    def compute_statistics(self) -> Statistics:
        """
        Compute all statistics for the report card table.

        Uses paired data (days where both obs and mod available) for most stats.
        """
        if self.aligned_data is None:
            self.align_data()

        paired = self.get_paired_data()
        obs = paired['obs'].values
        mod = paired['mod'].values
        n = len(obs)

        # Flow thresholds based on exceedance probabilities
        if self.exceedance is None:
            self.compute_exceedance()

        p_high = self.config.p_high  # e.g., 0.1
        p_low = self.config.p_low    # e.g., 0.79

        # Calculate volume totals (ML)
        total_obs = np.sum(obs)
        total_mod = np.sum(mod)

        # Flow volume by exceedance band
        # Each series is sorted independently, then summed within each band.
        # High flow: top p_high fraction (0 to p_high exceedance probability)
        # Medium flow: middle fraction (p_high to p_low)
        # Low flow: bottom fraction (p_low to 1)
        obs_sorted = np.sort(obs)[::-1]  # descending
        mod_sorted = np.sort(mod)[::-1]  # descending

        high_idx = int(p_high * n)
        low_idx = int(p_low * n)

        high_obs = np.sum(obs_sorted[:high_idx])
        high_mod = np.sum(mod_sorted[:high_idx])

        med_obs = np.sum(obs_sorted[high_idx:low_idx])
        med_mod = np.sum(mod_sorted[high_idx:low_idx])

        low_obs = np.sum(obs_sorted[low_idx:])
        low_mod = np.sum(mod_sorted[low_idx:])

        # Bias calculations
        def calc_bias(obs_val, mod_val):
            if obs_val == 0:
                return 0.0
            return ((mod_val - obs_val) / obs_val) * 100

        # Mean flow
        mean_obs = np.mean(obs)
        mean_mod = np.mean(mod)

        # Standard deviation
        std_obs = np.std(obs, ddof=1)
        std_mod = np.std(mod, ddof=1)

        # Driest 3-year mean
        driest_3yr_obs, driest_3yr_mod = self._compute_driest_3yr_mean(paired)

        # Zero flow days
        zero_obs = np.sum(obs < self.zero_threshold) / n * 100
        zero_mod = np.sum(mod < self.zero_threshold) / n * 100

        # Nash-Sutcliffe Efficiency
        nse = 1 - np.sum((obs - mod)**2) / np.sum((obs - np.mean(obs))**2)

        # Non-matching zero flow days
        # Counts days where model predicts zero flow but observed is not zero
        obs_zero = obs < self.zero_threshold
        mod_zero = mod < self.zero_threshold
        non_matching = np.sum(mod_zero & ~obs_zero) / n * 100

        # Metadata
        total_days = len(self.aligned_data)
        valid_days = n
        percent_available = (valid_days / total_days) * 100

        self.statistics = Statistics(
            total_flow_obs=total_obs,
            total_flow_mod=total_mod,
            total_flow_bias=calc_bias(total_obs, total_mod),

            low_flow_obs=low_obs,
            low_flow_mod=low_mod,
            low_flow_bias=calc_bias(low_obs, low_mod),

            med_flow_obs=med_obs,
            med_flow_mod=med_mod,
            med_flow_bias=calc_bias(med_obs, med_mod),

            high_flow_obs=high_obs,
            high_flow_mod=high_mod,
            high_flow_bias=calc_bias(high_obs, high_mod),

            mean_flow_obs=mean_obs,
            mean_flow_mod=mean_mod,
            mean_flow_bias=calc_bias(mean_obs, mean_mod),

            driest_3yr_obs=driest_3yr_obs,
            driest_3yr_mod=driest_3yr_mod,
            driest_3yr_bias=calc_bias(driest_3yr_obs, driest_3yr_mod),

            zero_flow_days_obs=zero_obs,
            zero_flow_days_mod=zero_mod,
            zero_flow_days_diff=abs(zero_mod - zero_obs),

            std_dev_obs=std_obs,
            std_dev_mod=std_mod,
            std_dev_bias=calc_bias(std_obs, std_mod),

            nse=nse,
            non_matching_zero_days=non_matching,

            analysis_start=self.start_date,
            analysis_end=self.end_date,
            total_days=total_days,
            valid_days=valid_days,
            percent_available=percent_available
        )
        return self.statistics

    def _compute_driest_3yr_mean(self, paired: pd.DataFrame) -> Tuple[float, float]:
        """
        Compute the mean daily flow for the driest consecutive 3 water-year period.

        Water year start month is configurable (default July for Australian convention).
        Finds the 3 consecutive water years with lowest total observed flow,
        then returns the mean daily flow for both observed and modelled.
        """
        data = self._assign_water_year(paired)

        # Group by water year
        wy_totals = data.groupby('water_year').agg({
            'obs': ['sum', 'count'],
            'mod': ['sum', 'count']
        })
        wy_totals.columns = ['obs_sum', 'obs_count', 'mod_sum', 'mod_count']
        years = wy_totals.index.values

        if len(years) < 3:
            # Not enough data for 3-year period
            return np.mean(paired['obs']), np.mean(paired['mod'])

        # Find 3 consecutive water years with lowest total observed flow
        min_3yr_sum = float('inf')
        min_start_wy = None

        for i in range(len(years) - 2):
            # Check if these are consecutive years
            if years[i+1] == years[i] + 1 and years[i+2] == years[i] + 2:
                sum_3yr = wy_totals.loc[years[i]:years[i+2], 'obs_sum'].sum()
                if sum_3yr < min_3yr_sum:
                    min_3yr_sum = sum_3yr
                    min_start_wy = years[i]

        if min_start_wy is None:
            # No 3 consecutive years found
            return np.mean(paired['obs']), np.mean(paired['mod'])

        # Calculate mean daily flow for the driest 3 water years
        driest_3wy = wy_totals.loc[min_start_wy:min_start_wy+2]
        total_days = driest_3wy['obs_count'].sum()
        obs_mean = driest_3wy['obs_sum'].sum() / total_days
        mod_mean = driest_3wy['mod_sum'].sum() / total_days

        return obs_mean, mod_mean

    def process_all(self) -> None:
        """Run all processing steps."""
        self.load_data()
        self.align_data()
        self.compute_exceedance()
        self.compute_flood_events()
        self.compute_water_year_totals()
        self.compute_residual_mass()
        self.compute_statistics()

    def get_rating(self, bias_percent: float, is_absolute: bool = False) -> str:
        """
        Convert bias percentage to star rating.

        Rating thresholds (approximate, adjust as needed):
        5 stars: < 2% bias
        4 stars: < 5% bias
        3 stars: < 10% bias
        2 stars: < 20% bias
        1 star: >= 20% bias
        """
        bias = abs(bias_percent) if not is_absolute else bias_percent

        if bias < 2:
            return "\u2605\u2605\u2605\u2605\u2605"  # 5 filled stars
        elif bias < 5:
            return "\u2605\u2605\u2605\u2605\u2606"  # 4 filled, 1 empty
        elif bias < 10:
            return "\u2605\u2605\u2605\u2606\u2606"  # 3 filled, 2 empty
        elif bias < 20:
            return "\u2605\u2605\u2606\u2606\u2606"  # 2 filled, 3 empty
        else:
            return "\u2605\u2606\u2606\u2606\u2606"  # 1 filled, 4 empty

    def get_nse_rating(self, nse: float) -> str:
        """
        Convert NSE to star rating.

        NSE thresholds:
        5 stars: > 0.9
        4 stars: > 0.8
        3 stars: > 0.65
        2 stars: > 0.5
        1 star: <= 0.5
        """
        if nse > 0.9:
            return "\u2605\u2605\u2605\u2605\u2605"
        elif nse > 0.8:
            return "\u2605\u2605\u2605\u2605\u2606"
        elif nse > 0.65:
            return "\u2605\u2605\u2605\u2606\u2606"
        elif nse > 0.5:
            return "\u2605\u2605\u2606\u2606\u2606"
        else:
            return "\u2605\u2606\u2606\u2606\u2606"


def main():
    """Example usage."""
    import sys

    if len(sys.argv) < 2:
        print("Usage: python reportcard_data.py <input_file.in>")
        print("\nRunning with default qa_input.in...")
        input_file = "qa_input.in"
    else:
        input_file = sys.argv[1]

    # Parse config
    config = parse_input_file(input_file)
    print(f"Loaded config: {config.heading}")
    print(f"  Observed: {config.obs_file}")
    print(f"  Modelled: {config.mod_file}")
    print(f"  Rainfall: {config.rain_file}")

    # Process data
    processor = ReportCardProcessor(config)
    processor.process_all()

    # Print summary
    stats = processor.statistics
    print(f"\nAnalysis period: {stats.analysis_start:%d/%m/%Y} to {stats.analysis_end:%d/%m/%Y}")
    print(f"Data availability: {stats.percent_available:.1f}% ({stats.valid_days}/{stats.total_days} days)")

    print(f"\nTotal Flow: {stats.total_flow_obs:,.0f} ML (bias: {stats.total_flow_bias:.1f}%)")
    print(f"NSE: {stats.nse:.2f}")
    print(f"Non-matching zero days: {stats.non_matching_zero_days:.1f}%")

    print(f"\nFlood events found: {len(processor.flood_events)}")
    for event in processor.flood_events:
        print(f"  #{event.rank}: {event.peak_date:%d/%m/%Y} - peak {event.peak_value:.0f} ML/d")


if __name__ == "__main__":
    main()
