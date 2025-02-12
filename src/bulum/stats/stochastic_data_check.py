import pandas as pd
import numpy as np
from bulum import utils
from bulum import trans
import statistics
import altair as alt
alt.data_transformers.enable("vegafusion")

class StochasticDataComparison:
    """
    Set of outputs for comparison of baseline dataset with additional dataset(s).

    Internal attributes provide raw outputs and charts for comparison of timeseries cross-correlations, distribution and general summary statistics. 
    Can be applied to stochastically generated data e.g. some subset (mean, percentile) of climate data replicates or climate factor adjusted datasets.

    Parameters
    ----------
    dfs_dict : dict of pd.DataFrames
        Dict containing name (key) and reference to (value) of input dataframe(s).
        Outputs are calculated for each of the columns in the first DataFrame (first entry in dict).
        Assumes that subsequent DataFrames contain at least the subset of columns from first DataFrame. Column headers must be identical.

    wy_month : int, default 7
        Water Year start month for annual aggregation.
        
    allow_part_years : bool, default False
        Allow part water years or only complete water years.

    Attributes
    --------
    StochasticDataComparison.Correlations : {outputs, chts, heatmaps}
        outputs: Multi-index df with Lag-0 and Lag-1 cross correlations grouped by - Period (annual vs. months), Lag-type, Timeseries, Dataset
        chts: Multi-level dictionary with charts stored by - Period (annual vs. months), Lag-type, Timeseries
        heatmaps: Multi-level dictionary with charts stored by - Period (annual vs. months), Lag-type, Dataset
        
    StochasticDataComparison.Distributions : {outputs, chts}
        outputs: Multi-index df with distribution of each timeseries grouped by - Period (annual vs. months), Timeseries, Dataset
        chts: Multi-level dictionary with charts stored by - Period (annual vs. months), Timeseries
        
    StochasticDataComparison.Stats : {outputs, chts}
        outputs: Multi-index df with stats grouped by - Period (annual vs. months), Statistic, Dataset, Timeseries
        chts: Multi-level dictionary with charts stored by - Period (annual vs. months), Statistic

    Examples
    --------
    Constructing StochasticDataComparison.

    >>> Comparison = StochasticDataComparison(dfs_dict = {'Dataset1': df_1, 'Dataset2': df_2})

    Output annual distribution all timeseries and datasets.

    >>> Comparison.Distributions["outputs"]["annual"]

    Output July (month 7) distribution comparison for given timeseries ("col1").
    
    >>> Comparison.Distributions["outputs"]["07"]["col1"]

    Output July (month 7) distribution chart for given timeseries ("col1").
    
    >>> Comparison.Distributions["chts"]["07"]["col1"]
    
    Output annual Lag-0 and Lag-1 cross correlations for all timeseries and datasets.

    >>> Comparison.Correlations["outputs"]["annual"]

    Output annual Lag-0 cross correlation chart comparison for given timeseries ("col1")

    >>> Comparison.Correlations["chts"]["annual"]["lag0"]["col1"]

    Output annual Lag-0 cross correlation heatmap for given dataset ("Dataset1")

    >>> Comparison.Correlations["chts"]["annual"]["lag0"]["Dataset1"]

    Output annual statistic summary for all timeseries and datasets.

    >>> Comparison.Stats["outputs"]["annual"]

    Output July mean total comparison for all timeseries and datasets.

    >>> Comparison.Stats["outputs"]["07"]["mean"]

    Output July mean total chart for all timeseries and datasets.

    >>> Comparison.Stats["chts"]["07"]["mean"]    

    Output July (month 7) distribution chart for all timeseries.
    
    >>> alt.vconcat(*Comparison.Distributions["chts"]["07"].values())

    Output July (month 7) distribution chart for all timeseries and adjust properties.
    
    >>> alt.vconcat(*[x.properties(width=800).interactive() for x in Comparison.Distributions["chts"]["07"].values()])    

    Output annual distribution chart for given timeseries ("col1"), convert to log-scale.
    
    >>> Comparison.Distributions["chts"]["annual"]["col1"].layer[0].encoding.y.scale = {'type': 'log'}    

    """

    def __init__(self, dfs_dict: dict, wy_month=7, allow_part_years=False) -> None:
                   
        self.wy_month = wy_month
        self.allow_part_years = allow_part_years

        self.dfs = dfs_dict.copy()
        self.datasets=[]
        colnames_check=[]
        for keys,values in self.dfs.items():
            colnames_check.append(values.columns)
            self.datasets.append(keys)

        #self.colnames=self.dfs[self.datasets[0]].columns
        self.colnames=colnames_check[0]

        for i,x in enumerate(colnames_check[1:]):
            if set(self.colnames).issubset(x)==False:
                raise Exception(f"ERROR: Dataset '{self.datasets[i+1]}' does not contain all columns from '{self.datasets[0]}'.")

        # Calculate whether to include full WYs only
        if not allow_part_years:
            for keys,values in self.dfs.items():
                self.dfs[keys] = utils.crop_to_wy(values, wy_month)  

        self.cropped_df_ann={keys: values.groupby(utils.get_wy(values.index)).sum() for keys, values in self.dfs.items()}
        self.cropped_df_mon={keys: values.groupby(utils.get_year_and_month(values.index)).sum() for keys, values in self.dfs.items()}

        self.Stats={}
        self.Distributions={}
        self.Correlations={}

        self.Distributions["outputs"]=self.__CalcDistributions()
        self.Distributions["chts"]=self.__DistributionCharts()

        self.Stats["outputs"]=self.__CalcSummaryStats()
        self.Stats["chts"]=self.__StatsCharts()

        self.Correlations["outputs"]=self.__CalcCorrelations()
        self.Correlations["chts"]=self.__CorrelationCharts()
        self.Correlations["heatmaps"]=self.__HeatmapCharts()
        
    def __CalcDistributions(self):
        """Internal function to calculate timeseries distribution.

        Returns:
            pd.DataFrame: Multi-index df with distribution of each timeseries grouped by - Period (annual vs. months), Timeseries, Dataset 
        """        
        distr={}

        months=['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

        for ds in self.datasets:
            distr[ds]={}


            distr[ds]['annual']={}

            subset=self.cropped_df_ann[ds]

            #Annual Calcs
            p=trans.get_exceedence_plotting_position(len(subset))
            p=[x/100 for x in p]
            z=[statistics.NormalDist().inv_cdf(x) for x in p]

            distr[ds]['annual']=subset.transform(np.sort)
            distr[ds]['annual'].index=z

            #Monthly Calcs
            for i in range(12):
                distr[ds][months[i]]={}
                subset=self.cropped_df_mon[ds][[x[5:]==str(f'{i+1:02}') for x in self.cropped_df_mon[ds].index]]
                
                p=trans.get_exceedence_plotting_position(len(subset))
                p=[x/100 for x in p]
                z=[statistics.NormalDist().inv_cdf(x) for x in p]

                distr[ds][months[i]]=subset.transform(np.sort)
                distr[ds][months[i]].index=z

        iterables = [['annual', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'], self.colnames, self.datasets]
        mindex=pd.MultiIndex.from_product(iterables, names=["season", "timeseries", "dataset"])

        distributions=pd.DataFrame({x: distr[x[2]][x[0]][x[1]] for x in mindex})

        return distributions
    
    def __DistributionCharts(self):
        """Internal function to generate Altair charts from timeseries distributions.

        Returns:
            dict: Multi-level dictionary with charts stored by - Period (annual vs. months), Timeseries
        """        

        charts={}

        for y in ['annual', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']:

            charts[y]={}

            for i in self.colnames:
                chart_list=[]
                count=0
                subset=self.Distributions["outputs"][y][i]
                for j in subset:
                    if count==0:
                        chart_list.append(alt.Chart(subset.reset_index()).mark_point(
                        ).transform_fold(
                            [j]
                        ).transform_filter('isValid(datum.value)'
                        ).encode(
                            alt.X('index', title='Z'),
                            alt.Y('value:Q', title=y),
                            color=alt.Color('key:N', title='Dataset'))
                        )
                    else:
                        chart_list.append(alt.Chart(subset.reset_index()).mark_line(
                        ).transform_fold(
                            [j]
                        ).transform_filter('isValid(datum.value)'
                        ).encode(
                            alt.X('index', title='Z'),
                            alt.Y('value:Q', title=y),
                            color=alt.Color('key:N', title='Dataset'))
                        )
                    count=count+1
                
                charts[y][i]=alt.layer(*chart_list[::-1]).properties(title=i)
        
        return charts
    
    def __CalcCorrelations(self):
        """Internal function to calculate Lag-0 and Lag-1 cross correlation for each timeseries.

        Returns:
            pd.DataFrame: Multi-index df with correlations grouped by - Period (annual vs. months), Lag-type, Timeseries, Dataset
        """        
                    
        corrs={}

        months=['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

        for ds in self.datasets:
            corrs[ds]={}

            #Annual Calcs
            corrs[ds]['annual']={}
            subset=self.cropped_df_ann[ds]

            corrs[ds]['annual']['lag0']=pd.DataFrame.from_dict({x: subset.corrwith(subset[x].shift(0)) for x in subset.columns})
            corrs[ds]['annual']['lag1']=pd.DataFrame.from_dict({x: subset.corrwith(subset[x].shift(1)) for x in subset.columns})
            
            #Monthly Calcs
            for i in range(12):
                corrs[ds][months[i]]={}
                subset=self.cropped_df_mon[ds][[x[5:]==str(f'{i+1:02}') for x in self.cropped_df_mon[ds].index]]

                corrs[ds][months[i]]['lag0']=pd.DataFrame.from_dict({x: subset.shift(-0).corrwith(subset[x]) for x in subset.columns})
                corrs[ds][months[i]]['lag1']=pd.DataFrame.from_dict({x: subset.shift(-1).corrwith(subset[x]) for x in subset.columns})

        iterables = [['annual', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'], ["lag0", "lag1"], self.colnames, self.datasets]
        mindex=pd.MultiIndex.from_product(iterables, names=["season", "stat", "timeseries", "dataset"])

        correlations=pd.DataFrame({x: corrs[x[3]][x[0]][x[1]][x[2]] for x in mindex})

        return correlations 
    
    def __CorrelationCharts(self):
        """Internal function to generate Altair charts from Lag-0 and Lag-1 cross correlations. Compares each timeseries individually against all other timeseries.

        Returns:
            dict: Multi-level dictionary with charts stored by - Period (annual vs. months), Lag-type, Timeseries
        """        

        charts={}  
        rule=alt.Chart().mark_rule(color='blue').encode(y=alt.datum(-1),y2=alt.datum(1),x=alt.datum(-1),x2=alt.datum(1))
        rule2=alt.Chart().mark_rule(strokeDash=[10,10]).encode(y=alt.datum(0))
        rule3=alt.Chart().mark_rule(strokeDash=[10,10]).encode(x=alt.datum(0))
        for y in ['annual', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']:
            charts[y]={}
            for x in ["lag0", "lag1"]:
                charts[y][x]={}
                for i in self.colnames:

                    subset=self.Correlations["outputs"][y][x][i]

                    # actual chart
                    
                    base=alt.Chart(subset.reset_index()).mark_point(tooltip=True)\
                    .transform_fold(
                        self.datasets[1:]
                    ).encode(
                        x=alt.X(f'{self.datasets[0]}:Q', sort=None, scale=alt.Scale(domain=[-1,1]), title=f'{self.datasets[0]} {y} correlation ({x})'), 
                        y=alt.Y('value:Q', sort=None, title=f'Comparative {y} correlation ({x})', scale=alt.Scale(domain=[-1,1])),
                        color=alt.Color('key:N', title="Comparative dataset"),
                        shape=alt.Shape('index:N', title="Timeseries", legend=None)
                    )
                    
                    charts[y][x][i]=(base+rule+rule2+rule3).properties(title=i)
        
        return charts  

    def __HeatmapCharts(self):
        """Internal function to generate Altair heatmap charts from Lag-0 and Lag-1 cross correlations. Compares each dataset individually.

        Returns:
            dict: Multi-level dictionary with charts stored by - Period (annual vs. months), Lag-type, Dataset
        """        

        charts={}  
        for y in ['annual', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']:
            charts[y]={}
            for x in ["lag0", "lag1"]:
                charts[y][x]={}
                for ds in self.datasets:

                    subset=self.Correlations["outputs"][y][x][:][ds]

                    # data preparation
                    pivot_cols = list(subset.index)
                    subset=subset.set_axis(subset.index, axis=1)
                    subset['timeseries'] = subset.index

                    # actual chart
                    charts[y][x][ds]=alt.Chart(subset).mark_rect(tooltip=True)\
                    .transform_fold(pivot_cols,
                                    as_=["lagged_timeseries","value"])\
                    .encode(
                        x=alt.X("timeseries:N", sort=None), 
                        y=alt.Y('lagged_timeseries:N', sort=None), 
                        color=alt.Color("value:Q", scale=alt.Scale(scheme="spectral", domain=[-1,1]), title=f"{y} correlation ({x})")
                    ).properties(title=ds)
        
        return charts  

    def __CalcSummaryStats(self):
        """Internal function to calculate summary stats for each timeseries. Functions

        Returns:
            pd.DataFrame: Multi-index df with stats grouped by - Period (annual vs. months), Statistic, Dataset, Timeseries
        """        

        stats={}

        months=['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

        for ds in self.datasets:
            stats[ds]={}

            #Annual Calcs
            stats[ds]['annual']={}
            subset=self.cropped_df_ann[ds]
            
            stats[ds]['annual']['mean']=subset.mean()
            stats[ds]['annual']['stddev']=subset.apply(lambda x: np.std(x))
            stats[ds]['annual']['skew']=subset.skew()
            stats[ds]['annual']['coeffofvar']=subset.apply(lambda x: np.std(x)/np.mean(x))
            stats[ds]['annual']['min']=subset.min()
            stats[ds]['annual']['max']=subset.max()
            stats[ds]['annual']['25thP']=subset.apply(lambda x: np.percentile(x,25))
            stats[ds]['annual']['median']=subset.apply(lambda x: np.percentile(x,50))
            stats[ds]['annual']['75thP']=subset.apply(lambda x: np.percentile(x,75))

            #Monthly Calcs
            for i in range(12):
                stats[ds][months[i]]={}
                subset=self.cropped_df_mon[ds][[x[5:]==str(f'{i+1:02}') for x in self.cropped_df_mon[ds].index]]

                stats[ds][months[i]]['mean']=subset.mean()
                stats[ds][months[i]]['stddev']=subset.apply(lambda x: np.std(x))
                stats[ds][months[i]]['skew']=subset.skew()
                stats[ds][months[i]]['coeffofvar']=subset.apply(lambda x: np.std(x)/np.mean(x))
                stats[ds][months[i]]['min']=subset.min()
                stats[ds][months[i]]['max']=subset.max()
                stats[ds][months[i]]['25thP']=subset.apply(lambda x: np.percentile(x,25))
                stats[ds][months[i]]['median']=subset.apply(lambda x: np.percentile(x,50))
                stats[ds][months[i]]['75thP']=subset.apply(lambda x: np.percentile(x,75))

        iterables = [['annual', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'], ["mean", "stddev", "skew", "coeffofvar", "min", "max", "25thP", "median", "75thP"], self.datasets]
        mindex=pd.MultiIndex.from_product(iterables, names=["season", "stat", "dataset"])

        summary=pd.DataFrame({x: stats[x[2]][x[0]][x[1]] for x in mindex})
        
        return summary
    
    def __StatsCharts(self):
        """Internal function to generate Altair charts from summary stats. Compares each statistic individually.

        Returns:
            dict: Multi-level dictionary with charts stored by - Period (annual vs. months), Statistic
        """        

        charts={}

        for y in ['annual', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']:

            charts[y]={}

            for i in ["mean", "stddev", "skew", "coeffofvar", "min", "max", "25thP", "median", "75thP"]:

                charts[y][i]=alt.Chart(self.Stats["outputs"][y][i].reset_index()).mark_point(
                    tooltip=True
                ).transform_fold(
                    self.datasets
                ).encode(
                    alt.X('index', title=None, sort=None),
                    alt.Y('value:Q', title=f'{i} ({y})'),
                    color=alt.Color('key:N', title='Dataset')
                ).properties(title=f'{y} {i}')
                            
        return charts 
