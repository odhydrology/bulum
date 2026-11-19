"""
Aggregate annual statistics for timeseries dataframes.

Each function sums daily values into water year totals, then summarises the
totals for each column.
"""

import numpy as np
import pandas as pd

from bulum import utils


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
        When False, a water year containing any missing (`NaN`) values has a
        `NaN` total.
        When True, missing values are skipped, and a water year with no values
        has a `NaN` total.

    Returns
    -------
    DataFrame
        The total of each column for each water year, indexed by water year.
        A Series if `df` is a Series.
    numpy.nan
        If `allow_part_years==False` and there is not at least one complete
        water year.

    Raises
    ------
    ValueError
        If the date index has duplicate dates.
    """
    totals = utils.annual_totals(df, wy_month, allow_part_years)
    return np.nan if len(totals) == 0 else totals


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
        When False, a water year containing any missing (`NaN`) values is
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

    Raises
    ------
    ValueError
        If the date index has duplicate dates.
    """
    totals = utils.annual_totals(df, wy_month, allow_part_years)
    return np.nan if len(totals) == 0 else totals.max()


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
        When False, a water year containing any missing (`NaN`) values is
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

    Raises
    ------
    ValueError
        If the date index has duplicate dates.
    """
    totals = utils.annual_totals(df, wy_month, allow_part_years)
    return np.nan if len(totals) == 0 else totals.min()


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
        When False, a water year containing any missing (`NaN`) values is
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

    Raises
    ------
    ValueError
        If the date index has duplicate dates.
    """
    totals = utils.annual_totals(df, wy_month, allow_part_years)
    return np.nan if len(totals) == 0 else totals.mean()


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
        When False, a water year containing any missing (`NaN`) values is
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

    Raises
    ------
    ValueError
        If the date index has duplicate dates.
    """
    totals = utils.annual_totals(df, wy_month, allow_part_years)
    return np.nan if len(totals) == 0 else totals.median()


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
        When False, a water year containing any missing (`NaN`) values is
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
        outside 0 to 100, or if the date index has duplicate dates.
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
    totals = utils.annual_totals(df, wy_month, allow_part_years)
    if len(totals) == 0:
        return np.nan
    result = totals.quantile([x / 100 for x in q])
    result.index = q
    return result
