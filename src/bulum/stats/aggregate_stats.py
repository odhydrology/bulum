""" 
Aggregate annual statistics for timeseries dataframes.
"""
# TODO refactor this module to reduce repeated code.

import numpy as np
import pandas as pd

from bulum import utils


def annual_max(df: pd.DataFrame, wy_month=7, allow_part_years=False):
    """Returns the maximum annual for a daily timeseries dataframe.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe with date as index
    wy_month : int, optional
        Water year start month. Defaults to 7 (i.e. July).
    allow_part_years : bool, optional
        Allow part water years or only complete water years. Defaults to False.

    Returns
    -------
    DataFrame
        A dataframe with entries the maximum measurement of each year.
    """
    if (allow_part_years):
        return df.groupby(utils.get_wy(df.index, wy_month)).sum().max()
    else:
        cropped_df = utils.crop_to_wy(df, wy_month)
        if (len(cropped_df) > 0):
            return cropped_df.groupby(utils.get_wy(cropped_df.index, wy_month)).sum().max()
        else:
            return np.nan

def annual_min(df: pd.DataFrame, wy_month=7, allow_part_years=False):
    """Returns the minimum annual for a daily timeseries dataframe.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe with date as index
    wy_month : int, optional
        Water year start month. Defaults to 7 (i.e. July).
    allow_part_years : bool, optional
        Allow part water years or only complete water years. Defaults to False.

    Returns
    -------
    DataFrame
        A dataframe with entries the maximum measurement of each year.
    """
    if (allow_part_years):
        return df.groupby(utils.get_wy(df.index, wy_month)).sum().min()
    else:
        cropped_df = utils.crop_to_wy(df, wy_month)
        if (len(cropped_df) > 0):
            return cropped_df.groupby(utils.get_wy(cropped_df.index, wy_month)).sum().min()
        else:
            return np.nan

def annual_mean(df: pd.DataFrame, wy_month=7, allow_part_years=False):
    """Returns the mean annual for a daily timeseries dataframe.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe with date as index
    wy_month : int, optional
        Water year start month. Defaults to 7 (i.e. July).
    allow_part_years : bool, optional
        Allow part water years or only complete water years. Defaults to False.

    Returns
    -------
    DataFrame
        A dataframe with entries the mean measurement of each year.
    """
    if (allow_part_years):
        return df.groupby(utils.get_wy(df.index, wy_month)).sum().mean()
    else:
        cropped_df = utils.crop_to_wy(df, wy_month)
        if (len(cropped_df) > 0):
            return cropped_df.groupby(utils.get_wy(cropped_df.index, wy_month)).sum().mean()
        else:
            return np.nan

def annual_median(df: pd.DataFrame, wy_month=7, allow_part_years=False):
    """Returns the median annual for a daily timeseries dataframe.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe with date as index
    wy_month : int, optional
        Water year start month. Defaults to 7 (i.e. July).
    allow_part_years : bool, optional
        Allow part water years or only complete water years. Defaults to False.

    Returns
    -------
    DataFrame
        A dataframe with entries the median measurement of each year.
    """
    if (allow_part_years):
        return df.groupby(utils.get_wy(df.index, wy_month)).sum().median()
    else:
        cropped_df = utils.crop_to_wy(df, wy_month)
        if (len(cropped_df) > 0):
            return cropped_df.groupby(utils.get_wy(cropped_df.index, wy_month)).sum().median()
        else:
            return np.nan
        
def annual_percentile(df: pd.DataFrame, q, wy_month=7, allow_part_years=False):
    """Returns the annual percentile (q) for a daily timeseries dataframe.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe with date as index
    wy_month : int, optional
        Water year start month. Defaults to 7 (i.e. July).
    allow_part_years : bool, optional
        Allow part water years or only complete water years. Defaults to False.

    Returns
    -------
    DataFrame
        A dataframe with entries the percentile `q` of each year.
    """
    if not isinstance(q,list):
        q=[q]

    if (allow_part_years):
        temp=df.groupby(utils.get_wy(df.index, wy_month)).sum().apply(lambda x: np.percentile(x,q))#.reindex(q)
        temp.index=q
        return temp
    else:
        cropped_df = utils.crop_to_wy(df, wy_month)
        if (len(cropped_df) > 0):
            temp=cropped_df.groupby(utils.get_wy(cropped_df.index, wy_month)).sum().apply(lambda x: np.percentile(x,q))
            temp.index=q
            return temp
        else:
            return np.nan
