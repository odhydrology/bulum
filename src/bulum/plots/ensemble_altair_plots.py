import pandas as pd
import altair as alt
from bulum import utils

def ensemble_probability_plot(input: utils.DataframeEnsemble | list , variable: str, parameters: list, stat_function, labels=None, width=800, height=400):
    """Returns a plot of typical probabilistic outputs from DataframeEnsemble inputs. Available statistics: [stats.ensemble_stats.cumulative_risk, stats.ensemble_stats.incremental_risk, stats.ensemble_stats.annual_incremental_risk, stats.ensemble_stats.percentile_envelope]

    Args
    --------
        input (utils.DataframeEnsemble | list): A (likely) filtered ODH DataframeEnsemble (or list of DataframeEnsembles).
        variable (str): Timeseries column of interest from the ensembled DataFrames e.g. 'Storage Volume'.
        parameters (list): List of parameters to test (relevant to function type).
        stat_function (function): Function from ODH ensemble stats.
        labels (_type_, optional): Optional list of label strings. Defaults to None.
        width (int, optional): Optional plot width. Defaults to 800.
        height (int, optional): Optional plot height. Defaults to 400.

    Returns
    --------
        altair.Chart: Returns an Altair chart object.

    Examples
    --------
    Constructing cumulative risk plot, looking at storage volumes [100000,50000,20000].

    >>> ensemble_probability_plot(input=[DataframeEnsemble.filter_tag("Scen1"), DataframeEnsemble.filter_tag("Scen2")], variable="Dam Storage Volume", parameters=[100000,50000,20000], stat_function=stats.cumulative_risk)

    Constructing incremental risk plot, looking at storage volumes [100000,50000,20000].

    >>> ensemble_probability_plot(input=[DataframeEnsemble.filter_tag("Scen1"), DataframeEnsemble.filter_tag("Scen2")], variable="Dam Storage Volume", parameters=[100000,50000,20000], stat_function=stats.incremental_risk)

    Constructing annual incremental risk plot, looking at storage volumes [100000,50000,20000]. Custom labels.

    >>> ensemble_probability_plot(input=[DataframeEnsemble.filter_tag("Scen1"), DataframeEnsemble.filter_tag("Scen2")], variable="Dam Storage Volume", parameters=[100000,50000,20000], stat_function=stats.annual_incremental_risk, labels=["Existing Dam", "Raised Dam"])

    Constructing percentile envelope plot (single dataset), looking at percentiles [1,10,90].

    >>> ensemble_probability_plot(input=DataframeEnsemble.filter_tag("Scen1"), variable="Dam Storage Volume", parameters=[1,10,90], stat_function=stats.percentile_envelope)
    """
    if not isinstance(input, list):
        input=[input]
    
    if not isinstance(parameters, list):
        parameters=[parameters]

    no_series=len(input)
    if labels==None:
        labels=['Series '+ str(i+1) for i in range(no_series)]
    if len(labels)!=no_series:
        print("Warning: Length of labels should equal number of input DataframeEnsembles. Reverting labels to default.")
        labels=['Series '+ str(i+1) for i in range(no_series)]

    input_array=[]
    for i in range(len(input)):
        input_dict=stat_function(input[i], variable, parameters)
        #temp_df=dict_to_df(input_dict)
        temp_df=pd.concat([values.rename(keys) for keys,values in input_dict.items()],axis=1)
        temp_df["Series"]=labels[i]
        input_array.append(temp_df)
    input_concat=pd.concat(input_array,axis=0)
       
    label=[str(x) for x in parameters]

    if stat_function.__name__ in ["cumulative_risk", "incremental_risk", "annual_incremental_risk"]:
        scale_domain=[0,100]
        y_units="Proportion (%)"
    else:
        scale_domain="unaggregated"
        y_units="Units"

    chart = alt.Chart(input_concat.reset_index()).mark_line(
    ).transform_fold(
        label,
        as_=['column','value']
    ).transform_calculate(
        value='round(datum.value * 10)/10'
    ).encode(
            x=alt.X('Date:T'),
            y=alt.Y('value:Q', scale=alt.Scale(domain=scale_domain), title=y_units),
            color=alt.Color('column:N', legend=alt.Legend(title="Parameter"), sort=None),
            strokeDash=alt.StrokeDash('Series:N', sort=None)
    ).properties(
        width=width,
        height=height
    )

    return chart
