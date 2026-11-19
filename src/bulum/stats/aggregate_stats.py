"""
Aggregate annual statistics for timeseries dataframes.

Each function sums daily values into water year totals, then summarises the
totals for each column.
"""

import numpy as np
import pandas as pd

from bulum import utils


def _fill_missing_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Returns `df` with a daily date index from its first to its last date.
    Dates absent from the index are added with NaN values."""
    if len(df) == 0:
        return df
    index = df.index
    # A sorted, unique index with one row per day between its first and last
    # dates is complete, so only start and end dates need parsing.
    days = (pd.Timestamp(index[-1]) - pd.Timestamp(index[0])).days + 1
    if days == len(index) and index.is_monotonic_increasing and index.is_unique:
        return df
    dates = pd.DatetimeIndex(pd.to_datetime(index), name=index.name)
    all_dates = pd.date_range(dates.min(), dates.max(), freq="D", name=index.name)
    return df.set_axis(dates).reindex(all_dates)


def _annual_totals(
        df: pd.DataFrame, wy_month: int, allow_part_years: bool) -> pd.DataFrame | None:
    """Returns the water year totals of `df`, or None if `allow_part_years` is
    False and `df` does not contain a complete water year.

    Dates absent from the index count as missing values. With
    `allow_part_years=False`, a water year containing any missing values has a
    NaN total. With `allow_part_years=True`, missing values are skipped, and a
    water year with no values has a NaN total.
    """
    df = _fill_missing_dates(df)
    if allow_part_years:
        return df.groupby(utils.get_wy(df.index, wy_month)).sum(min_count=1)
    cropped_df = utils.crop_to_wy(df, wy_month)
    if len(cropped_df) == 0:
        return None
    return cropped_df.groupby(utils.get_wy(cropped_df.index, wy_month)).sum(skipna=False)


def annual_sum(df: pd.DataFrame, wy_month=7, allow_part_years=False):
    """Returns the water year totals for a daily timeseries dataframe.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe with a daily date index. Dates absent from the index count
        as missing values.
    wy_month : int, optional
        Water year start month. Defaults to 7 (i.e. July).
    allow_part_years : bool, optional
        Allow part water years or only complete water years. Defaults to False.
        When False, a water year containing any missing (NaN) values has a
        NaN total.
        When True, missing values are skipped, and a water year with no values
        has a NaN total.

    Returns
    -------
    DataFrame
        The total of each column for each water year, indexed by water year.
        A Series if `df` is a Series.
    numpy.nan
        If `allow_part_years==False` and there is not at least one complete
        water year.
    """
    totals = _annual_totals(df, wy_month, allow_part_years)
    return np.nan if totals is None else totals


def annual_max(df: pd.DataFrame, wy_month=7, allow_part_years=False):
    """Returns the maximum water year total for a daily timeseries dataframe.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe with a daily date index. Dates absent from the index count
        as missing values.
    wy_month : int, optional
        Water year start month. Defaults to 7 (i.e. July).
    allow_part_years : bool, optional
        Allow part water years or only complete water years. Defaults to False.
        When False, a water year containing any missing (NaN) values is
        excluded.
        When True, missing values are skipped, and a water year with no values
        is excluded.

    Returns
    -------
    Series
        The maximum water year total of each column. A float if `df` is a
        Series.
    numpy.nan
        If `allow_part_years==False` and there is not at least one complete
        water year.
    """
    totals = _annual_totals(df, wy_month, allow_part_years)
    return np.nan if totals is None else totals.max()


def annual_min(df: pd.DataFrame, wy_month=7, allow_part_years=False):
    """Returns the minimum water year total for a daily timeseries dataframe.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe with a daily date index. Dates absent from the index count
        as missing values.
    wy_month : int, optional
        Water year start month. Defaults to 7 (i.e. July).
    allow_part_years : bool, optional
        Allow part water years or only complete water years. Defaults to False.
        When False, a water year containing any missing (NaN) values is
        excluded.
        When True, missing values are skipped, and a water year with no values
        is excluded.

    Returns
    -------
    Series
        The minimum water year total of each column. A float if `df` is a
        Series.
    numpy.nan
        If `allow_part_years==False` and there is not at least one complete
        water year.
    """
    totals = _annual_totals(df, wy_month, allow_part_years)
    return np.nan if totals is None else totals.min()


def annual_mean(df: pd.DataFrame, wy_month=7, allow_part_years=False):
    """Returns the mean water year total for a daily timeseries dataframe.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe with a daily date index. Dates absent from the index count
        as missing values.
    wy_month : int, optional
        Water year start month. Defaults to 7 (i.e. July).
    allow_part_years : bool, optional
        Allow part water years or only complete water years. Defaults to False.
        When False, a water year containing any missing (NaN) values is
        excluded.
        When True, missing values are skipped, and a water year with no values
        is excluded.

    Returns
    -------
    Series
        The mean water year total of each column. A float if `df` is a
        Series.
    numpy.nan
        If `allow_part_years==False` and there is not at least one complete
        water year.
    """
    totals = _annual_totals(df, wy_month, allow_part_years)
    return np.nan if totals is None else totals.mean()


def annual_median(df: pd.DataFrame, wy_month=7, allow_part_years=False):
    """Returns the median water year total for a daily timeseries dataframe.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe with a daily date index. Dates absent from the index count
        as missing values.
    wy_month : int, optional
        Water year start month. Defaults to 7 (i.e. July).
    allow_part_years : bool, optional
        Allow part water years or only complete water years. Defaults to False.
        When False, a water year containing any missing (NaN) values is
        excluded.
        When True, missing values are skipped, and a water year with no values
        is excluded.

    Returns
    -------
    Series
        The median water year total of each column. A float if `df` is a
        Series.
    numpy.nan
        If `allow_part_years==False` and there is not at least one complete
        water year.
    """
    totals = _annual_totals(df, wy_month, allow_part_years)
    return np.nan if totals is None else totals.median()


def annual_percentile(df: pd.DataFrame, q, wy_month=7, allow_part_years=False):
    """Returns percentiles of the water year totals for a daily timeseries
    dataframe.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe with a daily date index. Dates absent from the index count
        as missing values.
    q : float or sequence of float
        Percentile or percentiles to compute, between 0 and 100.
    wy_month : int, optional
        Water year start month. Defaults to 7 (i.e. July).
    allow_part_years : bool, optional
        Allow part water years or only complete water years. Defaults to False.
        When False, a water year containing any missing (NaN) values is
        excluded.
        When True, missing values are skipped, and a water year with no values
        is excluded.

    Returns
    -------
    DataFrame
        The percentiles of the water year totals of each column, indexed by
        `q`. A Series if `df` is a Series.
    numpy.nan
        If `allow_part_years==False` and there is not at least one complete
        water year.

    Raises
    ------
    ValueError
        If `q` is not numeric, has more than one dimension, or has a value
        outside 0 to 100.
    """
    q = np.atleast_1d(q)
    if q.ndim != 1:
        raise ValueError(f"q must be a number or a 1-D sequence, got shape {q.shape}")
    try:
        in_range = (q >= 0) & (q <= 100)
    except TypeError as err:
        raise ValueError(f"q must be numeric, got {q.tolist()}") from err
    if not in_range.all():
        raise ValueError(f"q must be between 0 and 100, got {q.tolist()}")
    q = q.tolist()
    totals = _annual_totals(df, wy_month, allow_part_years)
    if totals is None:
        return np.nan
    result = totals.quantile([x / 100 for x in q])
    result.index = q
    return result
