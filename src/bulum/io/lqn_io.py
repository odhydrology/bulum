"""
IO functions for IQQM listquan output files.

This module provides functionality to read IQQM listquan output files,
which are space-separated text files containing time series data.
"""
import os
from typing import Union

import numpy as np
import pandas as pd

from bulum import utils


def read_iqqm_lqn_output(filename: str | os.PathLike, col_name: str | None = None,
                         df: pd.DataFrame | None = None, *, data_start_row: int = 7,
                         dropna: bool = False) -> utils.TimeseriesDataframe:
    """
    Reads the output of IQQM listquan. This is a space-separated format with
    two columns (date, value) and data starting on line 7.

    Parameters
    ----------
    filename : str or :class:`os.PathLike`
        Path to the file to be read.
    col_name : str, optional
        If supplied, sets the name for the resulting output column, otherwise
        uses the filename. Default is None.
    df : :class:`pandas.DataFrame`, optional
        If supplied, joins the output to `df`. (Does not modify `df`.)
        Default is None.
    data_start_row : int, optional
        Optionally specify the start row; may be useful for reading other IQQM
        TEXT outputs. Default is 7.
    dropna : bool, optional
        Remove NA values from dataframe before reading. Default is False.

    Returns
    -------
    :class:`~bulum.utils.TimeseriesDataframe`
        A time series dataframe containing the parsed data with standardized
        date index and the specified column name.
    """
    # If no df was supplied, instantiate a new one
    if df is None:
        df = pd.DataFrame()
    # If no column name was specified, we use the base name of the file
    if col_name is None:
        col_name = os.path.basename(filename)
    # Read the data
    temp = pd.read_csv(filename, skiprows=(data_start_row-2),
                       sep=r'\s+', names=["Date", col_name], header=None)
    # temp = utils.set_index_dt(temp, format='%d/%m/%Y')
    if dropna:
        temp = temp.dropna()
    temp.set_index(temp.columns[0], inplace=True)
    temp.index = utils.standardize_datestring_format(temp.index)  # type: ignore
    temp.index.name = "Date"  # type: ignore
    temp = temp.replace(r'^\s*$', np.nan, regex=True)
    df = df.join(temp, how="outer").sort_index()

    # TODO: THERE IS NO GUARANTEE THAT THE DATES OVERLAP, THEREFORE WE MAY END UP WITH A DATAFRAME WITH INCOMPLETE DATES
    # TODO: I SHOULD MAKE DEFAULT BEHAVIOUR AUTO-DETECT FORMAT DEPENDING ON *TYPE* AND *LOCATION* OF DELIMIT CHARS
    # TODO: In the meantime we use the below to assert that the format of the resulting df meets our minimum standards.
    utils.assert_df_format_standards(df)
    return utils.TimeseriesDataframe.from_dataframe(df)
