import numpy as np
import pandas as pd
from bulum import utils
na_values = ['', ' ', 'null', 'NULL', 'NAN', 'NaN', 'nan', 'NA', 'na', 'N/A' 'n/a', '#N/A', '#NA', '-NaN', '-nan']


def read_ts_csv(filename, date_format=None, df=None, colprefix=None, allow_nonnumeric=False, assert_date=True, **kwargs) -> utils.TimeseriesDataframe:
    """Reads a daily timeseries csv into a DataFrame, and sets the index to string dates in the "%Y-%m-%d" format. 
    The method assumes the first column are dates.

    Args:
        filename (_type_): _description_
        date_format (str, optional): defaults to "%d/%m/%Y" as per Fors. Other common formats include "%Y-%m-%d", "%Y/%m/%d".
        df (pd.DataFrame, optional): If provided, the reader will append columns to this dataframe. Defaults to None.
        colprefix (str, optional): If provided, the reader will append this prefix to the start of each column name. Defaults to None.
        allow_nonnumeric (bool, optional): If false, the method will assert that all columns are numerical. Defaults to False.
        assert_date (bool, optional): If true, the method will assert that date index meets "%Y-%m-%d" format. Defaults to True.         

    Returns:
        pd.DataFrame: Dataframe containing the data from the csv file.
    """
    new_df = pd.read_csv(filename, na_values=na_values, **kwargs)
    # Date index
    new_df.set_index(new_df.columns[0], inplace=True)
    if assert_date:
        new_df.index = utils.standardize_datestring_format(new_df.index)
    new_df.index.name = "Date"
    # df = df.replace(r'^\s*$', np.nan, regex=True)
    # Check values
    if not allow_nonnumeric:
        for col in new_df.columns:
            if not np.issubdtype(new_df[col].dtype, np.number):
                raise Exception(f"ERROR: Column '{col}' is not numeric!")
    # Rename columns if required
    if colprefix is not None:
        for c in new_df.columns:
            new_df.rename(columns = {c:f"{colprefix}{c}"}, inplace = True)
    # Join to existing dataframe if required
    if df is None:
        df = new_df
    else:
        if len(df) > 0:
            # Check that the dates overlap
            newdf_ends_before_df_starts = new_df.index[0] < df.index[-1]
            df_ends_before_newdf_starts = df.index[-1] < new_df.index[0]
            if newdf_ends_before_df_starts or df_ends_before_newdf_starts:
                raise Exception("ERROR: The dates in the new dataframe do not overlap with the existing dataframe!")
        df = df.join(new_df, how="outer")
    return utils.TimeseriesDataframe.from_dataframe(df)


def write_ts_csv(df: pd.DataFrame, filename: str):
    """_summary_

    Args:
        df (pd.DataFrame): _description_
        filename (str): _description_
    """
    df.to_csv(filename)
