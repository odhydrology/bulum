import matplotlib.pyplot as plt
import plotly.express as px
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype as is_datetime



def _get_clone_with_datetimes(df) -> pd.DataFrame:
    if (len(df) == 0):
        raise Exception("Dateframe is empty.")
    elif (is_datetime(df.index)):
        pass # nothing to do
    elif (type(df.index[0]) is str):
        #Take a copy of the dataframe and convert the index to datetime
        df = df.copy(deep=True)
        df.index = df.index = pd.to_datetime(df.index)
    else:
        raise Exception("Dateframe index is not datetimes or strings.")
    return df


def plot_flowx(df, labels=None):
    """_summary_
    Ref: https://www.youtube.com/watch?v=6AurbMHGqBY

    Args:
        df (_type_): _description_
        labels (_type_, optional): _description_. Defaults to None.

    Returns:
        _type_: _description_
    """
    fig = px.line(df)
    fig.update_layout(
        yaxis_title_text = "Flow [ML/d]",
        yaxis_type = "log",
    )
    return fig


def plot_flow(df, labels=None):
    #plt.rcParams['figure.figsize'] = [12, 8]
    #Handle datetime and string indices
    df = _get_clone_with_datetimes(df)
    #Plot the data
    fig, ax = plt.subplots()
    for i in range(len(df.columns)):
        col = df.columns[i]
        if labels is not None:
            lab = labels[i]
        else:
            lab = col
        ax.plot(df[col], label=lab)
    ax.legend()
    ax.set_ylabel("Flow [ML/d]")
    ax.set_yscale('log')
    ax.grid(True)
    ax.set_ylim(1, None)
    #date_range = [datetime.date(2014, 1, 1), datetime.date(2022, 1, 1)]
    #ax.set_xlim(date_range)
    return fig, ax


def plot_exceedence(df, labels=None):
    #Handle datetime and string indices
    df = _get_clone_with_datetimes(df)
    #Plot the data
    fig, ax = plt.subplots()
    df_exceedence = df.dropna()
    nn = len(df_exceedence)
    index_starting_at_one = [i + 1 for i in range(nn)]
    df_exceedence["Exceedence"] = [100 * (r - 0.4)/(nn + 0.2) for r in index_starting_at_one]
    df_exceedence.set_index("Exceedence", inplace=True)
    for i in range(len(df_exceedence.columns)):
        col = df_exceedence.columns[i]
        if labels is not None:
            lab = labels[i]
        else:
            lab = col
        df_exceedence[col] = df_exceedence[col].sort_values(ascending=False).values
        ax.plot(df_exceedence[col], label=lab)
    ax.legend()
    ax.set_ylabel("Flow [ML/d]")
    ax.set_xlabel("Exceedence probability [%]")
    ax.set_yscale('log')
    ax.grid(True)
    ax.grid(True, which='minor')
    ax.set_ylim(1, None)
    ax.set_xlim(-2,102)
    return fig, ax
