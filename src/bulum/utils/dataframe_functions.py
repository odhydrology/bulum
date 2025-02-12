import pandas as pd
from bulum import utils
from pandas.api.types import is_datetime64_any_dtype as is_datetime
from datetime import datetime


def find_col(df: pd.DataFrame, string_pattern: str, unique_match=True):
    """
    Args:
        df (pd.DataFrame): _description_
        string_pattern (str): _description_
        unique_match (bool, optional): _description_. Defaults to True.
    """
    cols = [col for col in df.columns if string_pattern in col]
    if unique_match:        
        if len(cols) < 1:
            raise Exception(f'No column name matched "{string_pattern}".')
        if len(cols) > 1:
            raise Exception(f'More than one column name matched "{string_pattern}".')
        return df[cols[0]]
    return df[cols]


def assert_df_has_one_column(df: pd.DataFrame):
    """
    Args:
        df (pd.DataFrame): _description_
    """
    n_cols = len(df.columns)
    if not n_cols == 1:
        raise Exception(f"The dataframe must have exactly 1 column, but {n_cols} were found.") 
    

def assert_df_format_standards(df: pd.DataFrame):
    """
    Args:
        df (pd.DataFrame): _description_
    """
    violations = check_df_format_standards(df)
    if len(violations) > 0:
        raise Exception(f"Dataframe does not meet bulum format standards.\n {violations[0]}")
    

def crop_to_wy(df: pd.DataFrame, wy_month=7):
        start_date=utils.get_wy_start_date(df, wy_month)
        end_date=utils.get_wy_end_date(df, wy_month)
        if isinstance(start_date, datetime): #handle datetime inputs
            start_date = start_date.strftime(r"%Y-%m-%d")
            end_date = end_date.strftime(r"%Y-%m-%d")
        return df.loc[(df.index >= start_date) & (df.index <= end_date)]
        

def check_df_format_standards(df: pd.DataFrame):
    """
    Checks if a given dataframe meets standards generally requried by 
    bulum functions. These standards include:
    - Dataframe is not none
    - Dateframe is not empty
    - Dataframe index name is "Date"
    - Dataframe index values are daily sequential strings with the format "%Y-%m-%d"
    - Data columns all have datatype of double
    - Missing values are nan (not na, not -nan)

    Args:
        df (_type_): _description_

    Returns:
        bool: _description_
    """
    # - Dataframe is not none
    if df is None:
        return ["Dataframe is None"]
    # - Dataframe index name is "Date"
    if df.index.name != "Date":
        return ["Dataframe index name is not 'Date'"]
    # - Dataframe index values are daily sequential strings with the format "%Y-%m-%d"
    if len(df) > 0:
        # Check index values are strings
        if not isinstance(df.index[0], str):
            return [f"Index values are not strings: {type(df.index[0])}"]
        # Call function to determine string format
        str_format = utils.get_date_format(df.index[0])
        if str_format != r"%Y-%m-%d":
            return [f"Date string format should be '%Y-%m-%d' but is '{str_format}'"]
        # Check for sequential dates
        start_datetime = datetime.strptime(df.index[0], str_format)
        expected_date_strings = utils.get_dates(start_date=start_datetime, days=len(df), str_format=r"%Y-%m-%d")
        for i in range(len(df)):
            if df.index.values[i] != expected_date_strings[i]:
                return [f"Expected date string '{expected_date_strings[i]}' but found '{df.index.values[i]}' at index {i}"]
    # - Data columns all have datatype of double
    for c in df.columns:
        data_type = df[c].dtypes
        if (data_type != 'int64') and (df[c].dtypes != 'float64'):
            return [f"Column '{c}' is not int64 or float64: {data_type}"]
    # - Missing values are nan (not na, not -nan)
    return []


def set_index_dt(df: pd.DataFrame, dt_values=None, start_dt=None, **kwargs):
    """
    Returns a dataframe with datetime index. Useful for converting bulum dataframes to datetime
    as needed. WARNING: THE RETURNED DATAFRAME WILL BE INCONSISTENT WITH bulum STANDARD WHICH USES STRING DATES.
    
    If no optional arguments are provided, the function will look for a column named "date" (not 
    case-sensitive) within the input dataframe. Otherwise dt_values or start_dt (assumes daily)
    may be provided. 

    Args:
        df (pd.DataFrame): _description_
        dt_values (_type_, optional): _description_. Defaults to None.
        start_dt (_type_, optional): _description_. Defaults to None.
    """
    if (df is None) or (len(df.columns) == 0):
        raise Exception("The dataframe is None, or has no columns.")
    answer = None
    n_days = len(df)

    # If start_dt or dt_values was provided, then we go ahead and use those. This could 
    # potentially override an existing "Date" column, but I assume thats what the user 
    # wants if used in that way.
    if start_dt != None:
        df["Date"] = utils.get_dates(start_dt, days=n_days)
    elif dt_values != None:
        if len(dt_values) < n_days:
            raise Exception("dt_values is shorter than the dataframe.") 
        df["Date"] = dt_values[:n_days]

    # Now if the index is datetimes (whether it already was, or was created above) we just 
    # need to make sure it is named "Date" and return the df.
    if is_datetime(df.index):
        df.index.name = "Date"
        return df

    # If the index is named "Date" and is strings, we try to convert it to datetimes.
    if (df.index.name is not None) and (df.index.name.upper() == "DATE"):
        df.index.name = "Date"
        df.index = pd.to_datetime(df.index, **kwargs)
        return df

    # If there's no column named exactly "Date" (case sensitive) then we look for something 
    # ignoring whitespaces and capitalization and rename it exactly to "Date".
    if not "Date" in df.columns:
        for c in df.columns:
            if c.lower().strip() == "date":
                df = df.rename(columns={c: "Date"})
    
    # If there's still no "Date" column let's use the first column instead.
    if not "Date" in df.columns:
        df = df.rename(columns={df.columns[0]: "Date"})
    
    # Now, after all that, if there's a column called exactly "Date", we try to convert 
    # values to datetimes. 
    df["Date"] = pd.to_datetime(df["Date"], **kwargs)
    answer = df.set_index("Date")
    return answer


def datetimes_to_strings(v, str_format: str=r"%Y-%m-%d", **kwargs):
    """Converts a list of datetimes to strings using the given format.

    Args:
        v (_type_): _description_
        str_format (str, optional): _description_. Defaults to r"%Y-%m-%d".

    Returns:
        _type_: _description_
    """
    answer = [d.strftime(str_format) for d in v]
    return answer


def strings_to_datetimes(v, **kwargs):
    """Converts a list of strings to datetimes.

    Args:
        v (_type_): _description_

    Returns:
        _type_: _description_
    """
    answer = pd.to_datetime(v, **kwargs)
    return answer


def convert_index_to_datetime(df: pd.DataFrame, **kwargs):
    """
    Converts the index to pandas datetime. Accepts a dataframe with a index as datetime or strings.
    
    Args:
        df (pd.DataFrame): _description_ 
        **kwargs: _description_       
    """
    if (df is None) or (len(df.columns) == 0):
        raise Exception("The dataframe is None, or has no columns.")
    n_days = len(df)
    
    # Force the index name to be "Date"
    df.index.name = "Date"    
    
    if (n_days == 0) or (is_datetime(df.index)):
        # Nothing needed to be done
        pass
    elif (type(df.index[0]) is str):
        # Try to convert strings to datetimes.
        #df.index = pd.to_datetime(df.index, **kwargs)
        df.index = strings_to_datetimes(df.index, **kwargs)
    else:
        raise Exception("The index is not strings or datetimes.")
    return df


def convert_index_to_string(df: pd.DataFrame, str_format: str=r"%Y-%m-%d", **kwargs):
    """
    Converts the index to strings. Accepts a dataframe with a index as datetime or strings.
    
    Args:
        df (pd.DataFrame): _description_ 
        str_format (str, optional): _description_. Defaults to r"%Y-%m-%d".
        **kwargs: _description_       
    """    
    if (df is None) or (len(df.columns) == 0):
        raise Exception("The dataframe is None, or has no columns.")
    n_days = len(df)
    
    if (n_days == 0) or (type(df.index[0]) is str):
        # Nothing needed to be done
        pass
    elif is_datetime(df.index):
        # Try to convert datetimes to strings.
        new_index_values = [d.strftime(str_format) for d in df.index]
        df.index = new_index_values
    else:
        raise Exception("The index is not strings or datetimes.")

    # Force the index name to be "Date"
    df.index.name = "Date"    
    return df


def check_data_equivalence(df1, df2, check_col_order=True, threshold=1e-6, details:dict=None):
    """Checks if two numeric dataframes are the same. It checks the names & order of the columns, 
    the values of the index, and summary stats on all the data columns.

    Args:
        df1 (_type_): First dataframe.
        df2 (_type_): Second dataframe.
        check_col_order (bool, optional): Specifies whether column order is important or not. Defaults to True.
        threshold (float, optional): Numerical threshold for checking if stats are the same. Defaults to 1e-6.
        details (dict, optional): This is a dicitonary for returning detailed results to the user. Defaults to None.

    Returns:
        bool: _description_
    """
    if details is None:
        details = {}
    # Check if columns are the same
    c1 = df1.columns
    c2 = df2.columns
    differences = set(c1).difference(set(df2))
    if len(differences) > 0:
        temp = differences.intersection(set(c1))
        details[len(details)] = f"{len(temp)} columns are unique to df1: {temp}"
        temp = differences.intersection(set(c2))
        details[len(details)] = f"{len(temp)} columns are unique to df2: {temp}"
        return False
    # Check column order
    if check_col_order:
        for i in range(len(c1)):
            if c1[i] != c2[i]:
                details[len(details)] = f"Column order difference at col {i}: '{c1[i]}' != '{c2[i]}'"
                return False
    # Check row count
    if len(df1) != len(df2):
        details[len(details)] = f"Row count difference: '{len(df1)}' != '{len(df2)}'"
        return False
    # Check values of indices
    for i in range(len(df1)):
        if df1.index[i] != df2.index[i]:
            details[len(details)] = f"Indices not matching at row {i}: {df1.index[i]} != {df2.index[i]}"
            return False
    # Check the stats
    stats1 = df1.describe()
    stats2 = df2.describe()
    for c in c1:
        for stat_name in stats1.index:
            val1 = stats1.loc[stat_name][c]
            val2 = stats2.loc[stat_name][c]
            if abs(val1 - val2) > threshold:
                details[len(details)] = f"Column '{c}' has a different {stat_name}: {val1} != {val2}"
                return False            
    # All checks passed
    return True
