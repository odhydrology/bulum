import pandas as pd
from datetime import datetime, timedelta
from bulum import utils


def join_on_dates(df1, df2, assert_format_standards=True):
    """_summary_
    Assumes df1 and df2 both have datetime indexes with values that are daily,
    and sequentially ascending.

    Args:
        df1 (_type_): _description_
        df2 (_type_): _description_

    Returns:
        _type_: _description_
    """
    if assert_format_standards:
        utils.assert_df_format_standards(df1)
        utils.assert_df_format_standards(df2)
    min_datetime = datetime.strptime(min(min(df1.index),min(df2.index)), r"%Y-%m-%d")
    max_datetime = datetime.strptime(max(max(df1.index),max(df2.index)), r"%Y-%m-%d")
    ans_df = pd.DataFrame()
    ans_df["Date"] = utils.get_dates(min_datetime, max_datetime, str_format=r"%Y-%m-%d")
    ans_df.set_index("Date", inplace=True)
    ans_df = ans_df.join(df1, how='left')
    ans_df = ans_df.join(df2, how='left', lsuffix='_left', rsuffix='_right')
    if assert_format_standards:
        utils.assert_df_format_standards(ans_df)
    return ans_df



def get_exceedence(obs_df, mod_df, plotting_position="cunnane") -> pd.DataFrame:
    """_summary_

    Args:
        obs_df (_type_): _description_
        mod_df (_type_): _description_
        plotting_position (str, optional): _description_. Defaults to "cunnane". Other supported values: "weibull", "gringorten". See https://glossary.ametsoc.org/wiki/Plotting_position

    Raises:
        Exception: _description_

    Returns:
        _type_: _description_
    """
    df = obs_df.join(mod_df, how='inner')
    df = df.dropna()
    df.columns = ['x', 'y']
    df.x = df.x.sort_values(ascending=False).values
    df.y = df.y.sort_values(ascending=False).values
    n = len(df)
    df.index = get_exceedence_plotting_position(n, plotting_position)
    return df



def get_exceedence_plotting_position(length, plotting_position="cunnane"):
    """Get plotting position values for a given length.

    Args:
        plotting_position (str, optional): _description_. Defaults to "cunnane". Other supported values: "weibull", "gringorten". See https://glossary.ametsoc.org/wiki/Plotting_position

    Raises:
        Exception: If plotting position is not supported.

    Returns:
        _type_: _description_
    """
    
    n = length

    index_starting_at_one = [i + 1 for i in range(n)]
    if plotting_position == "cunnane":
        plotting_points = [100 * (r - 0.4)/(n + 0.2) for r in index_starting_at_one]
    elif plotting_position == "weibull":
        plotting_points = [100 * (r/(n + 1)) for r in index_starting_at_one]
    elif plotting_position == "gringorten":
        plotting_points = [100 * (r - 0.44)/(n + 0.12) for r in index_starting_at_one]
    else:
        raise Exception(f"Plotting position not supported: {plotting_position}")
    return plotting_points