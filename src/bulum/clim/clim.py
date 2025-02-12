import numpy as np
import pandas as pd


def derive_transformation_curves(original_ts: pd.Series, augmented_ts: pd.Series, season_start_months=[1,2,3,4,5,6,7,8,9,10,11,12], epsilon=1e-3) -> dict:
    """Returns a dictionary of exceedence-based transformation curves - one for each season 
    with the season's start month as the key. These are tables that map from exceedance 
    (cunnane plotting position as a fraction) to a scaling factor. These are intended to 
    be used to
    effectively summarise climate-change adjustments, and allow them to be transported from
    one timeseries to another.

    Args:
        original_ts (pd.Series): _description_
        augmented_ts (pd.Series): _description_
        season_start_months (list, optional): _description_. Defaults to [1,2,3,4,5,6,7,8,9,10,11,12].

    Returns:
        dict: _description_
    """
    df = pd.DataFrame()
    df["x"] = original_ts
    df["y"] = augmented_ts
    df = df.dropna() # Force common period
    answer = {}
    for i in range(len(season_start_months)):
        # Get a list of the months in this season
        start_month = season_start_months[i]
        season_len = (season_start_months + [m + 12 for m in season_start_months])[i + 1] - start_month
        months_in_this_season = [1,2,3,4,5,6,7,8,9,10,11,12,1,2,3,4,5,6,7,8,9,10,11,12][start_month - 1: start_month - 1 + season_len]
        # Find the data for this season
        df_m = df[[int(d[5:7]) in months_in_this_season for d in df.index]] #d[5:7] is the month part of the date string
        x = np.sort(df_m.x.values)
        y = np.sort(df_m.y.values)
        # The transformation factor is y/x except when the original value x is zero (<epsilon) in which case we default to 1.0
        f = np.where(x < epsilon, 1.0, y / x)
        n = len(x)
        ii = [i + 1 for i in range(n)] #index starting at 1
        p = [(i - 0.4)/(n + 0.2) for i in ii]
        answer[start_month] = [p,f]
    return answer     

    
def apply_transformation_curves(tranformation_curves: dict, series: pd.Series) -> pd.Series:
    """Applies seasonal transformation curves to an input series.
    Refer to the function 'derive_transformation_curves(...)'.

    Args:
        tranformation_curves (dict): _description_
        series (pd.Series): _description_

    Returns:
        pd.Series: _description_
    """
    dates = series.index
    answer = series.copy()    
    # Apply each transformation curves to the whole series. Splice the appropriate 
    # parts (seasons) into the 'answer' series as we go. 
    season_start_months = sorted(tranformation_curves.keys())
    for i in range(len(season_start_months)):
        # Identify the transform curve for this season
        start_month = season_start_months[i]
        t = tranformation_curves[start_month]
        xp = t[0] 
        fp = t[1]
        # Get a list of the months in this season
        season_len = (season_start_months + [m + 12 for m in season_start_months])[i + 1] - start_month
        months_in_this_season = [1,2,3,4,5,6,7,8,9,10,11,12,1,2,3,4,5,6,7,8,9,10,11,12][start_month - 1: start_month - 1 + season_len]
        # Find the data for this season
        m = len(series)
        season_dates = pd.Series([d for d in dates if int(d[5:7]) in months_in_this_season]) #d[5:7] is the month part of the date string
        values = answer[season_dates]
        # And get their ranks and plotting positions
        rank_starting_at_one = values.rank(ascending=True) # This function is nice because equal values are assigned the same (averaged) rank.
        n = len(values)
        p = [(r - 0.4)/(n + 0.2) for r in rank_starting_at_one] # plotting position
        f = np.interp(p, xp, fp) # interpolated scaling factors
        # Calcualte new values and update the answer
        new_values = pd.Series([values.iloc[i] * f[i] for i in range(n)], index=season_dates)
        answer.update(new_values)
    # Return a pd.Series so user can easily join it back into a dataframe
    return pd.Series(answer, index=dates, name=series.name)     
    
    
def derive_transformation_factors(original_ts: pd.Series, augmented_ts: pd.Series, season_start_months=[1,2,3,4,5,6,7,8,9,10,11,12], epsilon=1e-3) -> dict:
    """Returns a dictionary of transformation factors - one for each season 
    with the season's start month as the key. These scaling factors are intended to 
    be used to effectively summarise climate-change adjustments, and allow them to be 
    transported from one timeseries to another.

    Args:
        original_ts (pd.Series): _description_
        augmented_ts (pd.Series): _description_
        season_start_months (list, optional): _description_. Defaults to [1,2,3,4,5,6,7,8,9,10,11,12].
        epsilon: Threshold below which values are treated as zero, and the associated factor defaults to 1.

    Returns:
        dict: _description_
    """
    # Create a map of month -> season_start_month (for all months)
    month_to_season_map = {}
    key = max(season_start_months)
    for m in [1,2,3,4,5,6,7,8,9,10,11,12]:
        if m in season_start_months:
            key = m
        month_to_season_map[m] = key
    # Put the data in a dataframe, and groupby season start month
    df = pd.DataFrame()
    df["x"] = original_ts
    df["y"] = augmented_ts
    df = df.dropna() # Force common period
    df['m'] = df.index.month
    df['s'] = df['m'].map(month_to_season_map)
    df2 = df.groupby('s').agg('sum')
    df2['f'] = np.where(df2.x < epsilon, 1.0, df2.y / df2.x)
    return df2['f'].to_dict()

    
def apply_transformation_factors(tranformation_factors: dict, series: pd.Series) -> pd.Series:
    """Applies seasonal transformation factors to an input series.
    Refer to the function 'derive_transformation_factors(...)'.

    Args:
        tranformation_curves (dict): _description_
        series (pd.Series): _description_

    Returns:
        pd.Series: _description_
    """
    # Create a map of month -> factor (containing all months)
    season_start_months = sorted(tranformation_factors.keys())
    month_to_factor_map = {}
    key = max(season_start_months)
    for m in [1,2,3,4,5,6,7,8,9,10,11,12]:
        if m in season_start_months:
            key = m
        month_to_factor_map[m] = tranformation_factors[key]
    # Apply transformation factors to the whole series. Splice the appropriate 
    df = pd.DataFrame()
    df['x'] = series
    df['m'] = df.index.month
    df['f'] = df['m'].map(month_to_factor_map)
    df['y'] = df['x'] * df['f']
    answer = df['y']
    answer.name = series.name
    return answer

    
    
    