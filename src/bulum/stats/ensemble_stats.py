import pandas as pd
import numpy as np
from bulum import utils
from datetime import datetime, timedelta

def cumulative_risk(input: utils.DataframeEnsemble, variable: str, parameters: list):
    """Returns a timeseries of cumulative risk for each parameter i.e. probability (as a proportion of ensemble DataFrames) of variable having been below a certain parameter at least once by a given date in the timeseries.

    Args:
        input (utils.DataframeEnsemble): A (likely) filtered ODH DataframeEnsemble.
        variable (str): Timeseries column of interest from the ensembled DataFrames e.g. 'Storage Volume'.
        parameters (list): List of parameters to test.

    Returns:
        dict: Dictionary with keys = 'parameters' and values = cumulative risk timeseries output.
    """    
    if not isinstance(parameters, list):
        parameters=[parameters]     
    cumu_dict={}
    temp=pd.concat([x[variable] for x in input],axis=1)
    no_repl=len(temp.columns)
    for v in parameters:
        cumulative=temp.copy(deep=True)
        for i in range(len(temp.columns)):
            if min(cumulative.iloc[:,i])<v:                                              #An event occurred at least once this replicate
                cumu_ind=cumulative.index[cumulative.iloc[:,i]<v][0]                         #Find the index of the first occurrence
                cumu_ind_int=list(cumulative.index).index(cumu_ind)                          #Convert index to an integer
                cumulative.iloc[:,i]=0                                                       #Initally set all days to '0' i.e. did not occur
                cumulative.iloc[cumu_ind_int:,i]=1                                           #Update all days including and post first occurrence index to '1' i.e. has occurred
            else:                                                                        #An event did not occur this replicate
                cumulative.iloc[:,i]=0                                                       #Set all days to '0' i.e. did not occur

        cumu_dict[f"{str(v)}"]=cumulative.apply(lambda x: sum(x)/no_repl*100,axis=1).rename(variable) #Set 
    return cumu_dict

def percentile_envelope(input: utils.DataframeEnsemble, variable: str, parameters: list):
    """Returns a timeseries of percentile outcomes for a given variable across ensemble DataFrames for a given date in the timeseries.

    Args:
        input (utils.DataframeEnsemble): A (likely) filtered ODH DataframeEnsemble.
        variable (str): Timeseries column of interest from the ensembled DataFrames e.g. 'Storage Volume'.
        parameters (list): List of percentiles (0 - 100) to test.

    Returns:
        dict: Dictionary with keys = 'parameters' and values = percentile envelope timeseries output.
    """   
    if not isinstance(parameters, list):
        parameters=[parameters]     
    env_dict={}
    temp=pd.concat([x[variable] for x in input],axis=1)
    for v in parameters:
        env_dict[f"{str(v)}"]=temp.apply(lambda x: np.percentile(x,v), axis=1).rename(variable)
    return env_dict

def incremental_risk(input: utils.DataframeEnsemble, variable: str, parameters: list):
    """Returns a timeseries of incremental risk for each parameter i.e. probability (as a proportion of ensemble DataFrames) of variable having been below a certain parameter for a given date in the timeseries.

    Args:
        input (utils.DataframeEnsemble): A (likely) filtered ODH DataframeEnsemble.
        variable (str): Timeseries column of interest from the ensembled DataFrames e.g. 'Storage Volume'.
        parameters (list): List of parameters to test.

    Returns:
        dict: Dictionary with keys = 'parameters' and values = incremental risk timeseries output.
    """   
    if not isinstance(parameters, list):
        parameters=[parameters]    
    inc_dict={}
    temp=pd.concat([x[variable] for x in input],axis=1)
    no_repl=len(temp.columns)
    for v in parameters:
        inc_dict[f"{str(v)}"]=temp.apply(lambda x: sum(np.where(x<v,1,0))/no_repl*100,axis=1).rename(variable)
    return inc_dict

def annual_incremental_risk(input: utils.DataframeEnsemble, variable: str, parameters: list, min_count=7):
    """Returns an annual timeseries of incremental risk for each parameter i.e. probability (as a proportion of ensemble DataFrames) of variable having been below a certain parameter for a given water year in the timeseries, with a minimum event count threshold.

    Args:
        input (utils.DataframeEnsemble): A (likely) filtered ODH DataframeEnsemble.
        variable (str): Timeseries column of interest from the ensembled DataFrames e.g. 'Storage Volume'.
        parameters (list): List of parameters to test.
        min_count (int, optional): Minimum number of days in year to count as event. Defaults to 7.

    Returns:
        dict: Dictionary with keys = 'parameters' and values = annual incremental risk timeseries output.
    """   
    if not isinstance(parameters, list):
        parameters=[parameters] 
    ann_inc_dict={}
    temp=pd.concat([x[variable] for x in input],axis=1)
    no_repl=len(temp.columns)
    for v in parameters:
        ann_inc_dict[f"{str(v)}"]=temp.groupby(utils.get_wy(temp.index)).aggregate(lambda x: sum(np.where(x<v,1,0))).apply(lambda x: sum(np.where(x>=min_count,1,0))/no_repl*100,axis=1).rename(variable).rename_axis("Date")
    return ann_inc_dict

