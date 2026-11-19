import random
import unittest
from datetime import datetime

import altair as alt
import matplotlib
matplotlib.use('Agg')  # Set non-interactive backend for testing
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pytest

from bulum import io, plots, stats, utils


class Tests(unittest.TestCase):
    
    def test_flow_plot(self):
        df = pd.DataFrame()
        df["a"] = [random.normalvariate(40,5) for _ in range(200)]
        df["b"] = [random.normalvariate(50,5) for _ in range(200)]
        df["c"] = [random.normalvariate(60,5) for _ in range(200)]
        df = utils.set_index_dt(df, start_dt=datetime(2000, 1, 1))
        plots.plot_flow(df)
        #plt.show()       

    def test_flow_plotx(self):
        df = pd.DataFrame()
        df["a"] = [random.normalvariate(40,5) for _ in range(200)]
        df["b"] = [random.normalvariate(50,5) for _ in range(200)]
        df["c"] = [random.normalvariate(60,5) for _ in range(200)]
        df = utils.set_index_dt(df, start_dt=datetime(2000, 1, 1))
        fig = plots.plot_flowx(df)
        #fig.write_html("c:/temp/blarg.html")
        #fig.show()

    def test_exceedence_plot(self):
        start_dt=datetime(1889, 1, 1)
        df = pd.DataFrame()
        df["a"] = [random.normalvariate(40,5) for _ in range(3650)]
        df["b"] = [random.normalvariate(50,5) for _ in range(3650)]
        df["c"] = [random.normalvariate(60,5) for _ in range(3650)]
        df.index = utils.get_dates(start_dt, days=3650,str_format=r'%Y-%m-%d')
        df.index.name = "Date"
        plots.exceedance_plot(df)

    def test_daily_plot(self):
        start_dt=datetime(1889, 1, 1)
        df = pd.DataFrame()
        df["a"] = [random.normalvariate(40,5) for _ in range(3650)]
        df["b"] = [random.normalvariate(50,5) for _ in range(3650)]
        df["c"] = [random.normalvariate(60,5) for _ in range(3650)]
        df.index = utils.get_dates(start_dt, days=3650,str_format=r'%Y-%m-%d')
        df.index.name = "Date"
        plots.daily_plot(df)

    def test_annual_plot(self):
        start_dt=datetime(1889, 1, 1)
        df = pd.DataFrame()
        df["a"] = [random.normalvariate(40,5) for _ in range(3650)]
        df["b"] = [random.normalvariate(50,5) for _ in range(3650)]
        df["c"] = [random.normalvariate(60,5) for _ in range(3650)]
        df.index = utils.get_dates(start_dt, days=3650,str_format=r'%Y-%m-%d')
        df.index.name = "Date"
        plots.annual_plot(df)

    @staticmethod
    def _missing_data_df():
        """Daily values of 1.0 for water years 2001 to 2003, where WY2003 is
        a leap year. Column "gap" misses 10 days of WY2002, "all_missing"
        misses all of WY2002, and "leap_gap" misses one day of WY2003."""
        idx = pd.date_range("2001-07-01", "2004-06-30", freq="D", name="Date")
        df = pd.DataFrame({"full": 1.0, "gap": 1.0, "all_missing": 1.0, "leap_gap": 1.0}, index=idx)
        df.loc["2003-01-01":"2003-01-10", "gap"] = np.nan
        df.loc["2002-07-01":"2003-06-30", "all_missing"] = np.nan
        df.loc["2004-01-15", "leap_gap"] = np.nan
        return df

    def test_annual_plot_missing_data(self):
        nan = np.nan
        df = self._missing_data_df()
        cases = [
            # (name, df, {column: [WY2001, WY2002, WY2003]})
            ("missing values", df,
             {"full": [365, 365, 366], "gap": [365, nan, 366],
              "all_missing": [365, nan, 366], "leap_gap": [365, 365, nan]}),
            ("missing dates", df.drop(pd.date_range("2003-01-01", "2003-01-10")),
             {"full": [365, nan, 366]}),
            ("string dates", df.set_axis(df.index.strftime("%Y-%m-%d")),
             {"gap": [365, nan, 366]}),
        ]
        for name, data, expected in cases:
            with self.subTest(case=name):
                chart = plots.annual_plot(data)
                totals = chart.data.pivot(index="Water year", columns="Data set", values="Flow (ML/a)")
                for col, values in expected.items():
                    np.testing.assert_array_equal(totals[col].to_numpy(), values)

    def test_annual_plot_stochastic_dates(self):
        # String dates in years outside the nanosecond timestamp range.
        dates = np.arange(np.datetime64("0001-07-01"), np.datetime64("0003-07-01"))
        df = pd.DataFrame({"a": 1.0}, index=[str(date) for date in dates])
        chart = plots.annual_plot(df.drop(df.index[400]))
        np.testing.assert_array_equal(chart.data["Flow (ML/a)"].to_numpy(), [365, np.nan])

    def test_annual_plot_no_complete_year(self):
        df = self._missing_data_df().iloc[:30]
        self.assertEqual(len(plots.annual_plot(df).data), 0)

    def test_annual_demand_supply_plot_missing_data(self):
        nan = np.nan
        df = self._missing_data_df()
        expected = {"full": [365, 365, 366], "gap": [365, 355, 366],
                    "all_missing": [365, nan, 366], "leap_gap": [365, 365, 365]}
        for col, values in expected.items():
            with self.subTest(column=col):
                chart = plots.annual_demand_supply_plot(df[col].rename("demand"), df["full"].rename("supply"))
                totals = chart.layer[0].data.set_index("index")
                np.testing.assert_array_equal(totals["demand"].to_numpy(), values)

    def test_residual_mass_curve(self):
        start_dt=datetime(1889, 1, 1)
        df = pd.DataFrame()
        df["a"] = [random.normalvariate(40,5) for _ in range(3650)]
        df["b"] = [random.normalvariate(50,5) for _ in range(3650)]
        df["c"] = [random.normalvariate(60,5) for _ in range(3650)]
        df.index = utils.get_dates(start_dt, days=3650,str_format=r'%Y-%m-%d')
        df.index.name = "Date"
        plots.residual_mass_curve(df)

    def test_storage_plot(self):
        start_dt=datetime(1889, 1, 1)
        df = pd.DataFrame()
        df["a"] = [random.normalvariate(40,5) for _ in range(3650)]
        df["b"] = [random.normalvariate(50,5) for _ in range(3650)]
        df["c"] = [random.normalvariate(60,5) for _ in range(3650)]
        triggers={"L1": 40, "L2": 50, "L3": 60}
        df.index = utils.get_dates(start_dt, days=3650,str_format=r'%Y-%m-%d')
        df.index.name = "Date"
        plots.storage_plot(df,triggers)

    def test_annual_demand_supply_plot(self):
        start_dt=datetime(1889, 1, 1)
        df = pd.DataFrame()
        df["a"] = [random.normalvariate(40,5) for _ in range(3650)]
        df["b"] = [random.normalvariate(50,5) for _ in range(3650)]
        df.index = utils.get_dates(start_dt, days=3650,str_format=r'%Y-%m-%d')
        df.index.name = "Date"
        plots.annual_demand_supply_plot(df["b"],df["a"])
    
    def test_pyblo(self):
        pathlist=['./src/bulum/plots/tests/919001c.csv','./src/bulum/plots/tests/919001b.csv','./src/bulum/plots/tests/919001a.csv','./src/bulum/plots/tests/919003a.csv','./src/bulum/plots/tests/919009b.csv','./src/bulum/plots/tests/919009a.csv']
        df_list=[]
        for i in range(len(pathlist)):
            df_list.append(io.read_ts_csv(pathlist[i])["Total"])
        sites=["919001C","919001B","919001A","919003A","919009B","919009A"]
        series=["Gauge data","Gauge data","Gauge data","Gauge data","Gauge data","Gauge data"]
        plots.pyblo(df_list,sites,series)

    def test_pyblo_vegafusion(self):
        # Issue #8: VegaFusion parsed a one-element list subtitle as an
        # expression and failed on the leading zeros in its dates (the
        # issue's date range started "01/01/1889").
        pathlist=['./src/bulum/plots/tests/919001c.csv','./src/bulum/plots/tests/919003a.csv']
        df_list=[io.read_ts_csv(path)["Total"] for path in pathlist]
        sites=["919001C","919003A"]
        series=["Gauge data","Gauge data"]
        charts = {subtitle: plots.pyblo(df_list,sites,series,subtitle=subtitle)
                  for subtitle in [True, False]}
        # The subtitle text must be a plain string, not a one-element list.
        with alt.data_transformers.enable("default"):
            title_text = charts[True].to_dict()["title"]["text"]
        self.assertIsInstance(title_text, str)
        self.assertTrue(title_text.startswith("Annual (January to December)"))
        pytest.importorskip("vegafusion")
        with alt.data_transformers.enable("vegafusion"):
            for subtitle, chart in charts.items():
                with self.subTest(subtitle=subtitle):
                    chart.to_dict(format="vega")

    def test_wy_event_heatmap(self):
        start_dt=datetime(1889, 1, 1)
        df = pd.DataFrame()
        df["a"] = np.random.binomial(n=1, p=0.001, size=3650)
        df["b"] = np.random.binomial(n=1, p=0.001, size=3650)
        df["c"] = np.random.binomial(n=1, p=0.001, size=3650)
        df.index = utils.get_dates(start_dt, days=3650,str_format=r'%Y-%m-%d')
        df.index.name = "Date"
        df_annual=df.groupby(utils.get_wy(df.index)).max()
        plots.wy_event_heatmap(df_annual)

    def test_plot_flow(self):
        df = io.read_ts_csv("./src/bulum/io/tests/test_data2.csv")
        plots.plot_flow(df)

    def test_ensemble_plot_cumulative(self):
        ensemble = utils.DataframeEnsemble()
        for filename in ["./src/bulum/stats/tests/scenario_replicates/test_scen1_repl1.csv",
                         "./src/bulum/stats/tests/scenario_replicates/test_scen1_repl2.csv"]:
            ensemble.add_dataframe(io.read(filename), tag="scen1")
            #ensemble.add_dataframe_from_file(filename, tag="scen1") //TODO: I have replaced this with above until we can unpick the cicrular import issue
        for filename in ["./src/bulum/stats/tests/scenario_replicates/test_scen2_repl1.csv",
                         "./src/bulum/stats/tests/scenario_replicates/test_scen2_repl2.csv"]:
            ensemble.add_dataframe(io.read(filename), tag="scen2")
            #ensemble.add_dataframe_from_file(filename, tag="scen2") //TODO: I have replaced this with above until we can unpick the cicrular import issue
        plots.ensemble_probability_plot([ensemble.filter_tag("scen1"),ensemble.filter_tag("scen2")],
                                        variable="Storage",
                                        parameters=[100000,64000],
                                        stat_function=stats.cumulative_risk,
                                        labels=["Scenario 1", "Scenario 2"])
        plots.ensemble_probability_plot([ensemble.filter_tag("scen1"),ensemble.filter_tag("scen2")],
                                        variable="Storage",
                                        parameters=[64000],
                                        stat_function=stats.cumulative_risk)
        plots.ensemble_probability_plot([ensemble.filter_tag("scen1"),ensemble.filter_tag("scen2")],
                                        variable="Storage",
                                        parameters=64000,
                                        stat_function=stats.cumulative_risk,
                                        width=1200,
                                        height=600)

    def test_ensemble_plot_incremental(self):
        ensemble = utils.DataframeEnsemble()
        for filename in ["./src/bulum/stats/tests/scenario_replicates/test_scen1_repl1.csv",
                         "./src/bulum/stats/tests/scenario_replicates/test_scen1_repl2.csv"]:
            ensemble.add_dataframe(io.read(filename), tag="scen1")
            #ensemble.add_dataframe_from_file(filename, tag="scen1") //TODO: I have replaced this with above until we can unpick the cicrular import issue
        for filename in ["./src/bulum/stats/tests/scenario_replicates/test_scen2_repl1.csv",
                         "./src/bulum/stats/tests/scenario_replicates/test_scen2_repl2.csv"]:
            ensemble.add_dataframe(io.read(filename), tag="scen2")
            #ensemble.add_dataframe_from_file(filename, tag="scen2") //TODO: I have replaced this with above until we can unpick the cicrular import issue
        plots.ensemble_probability_plot([ensemble.filter_tag("scen1"),ensemble.filter_tag("scen2")],
                                        variable="Storage",
                                        parameters=[100000,64000],
                                        stat_function=stats.incremental_risk,
                                        labels=["Scenario 1", "Scenario 2"])
        plots.ensemble_probability_plot([ensemble.filter_tag("scen1"),ensemble.filter_tag("scen2")],
                                        variable="Storage",
                                        parameters=[64000],
                                        stat_function=stats.incremental_risk)
        plots.ensemble_probability_plot([ensemble.filter_tag("scen1"),ensemble.filter_tag("scen2")],
                                        variable="Storage",
                                        parameters=64000,
                                        stat_function=stats.incremental_risk,
                                        width=1200,
                                        height=600)

    def test_ensemble_plot_annual_incremental(self):
        ensemble = utils.DataframeEnsemble()
        for filename in ["./src/bulum/stats/tests/scenario_replicates/test_scen1_repl1.csv",
                         "./src/bulum/stats/tests/scenario_replicates/test_scen1_repl2.csv"]:
            ensemble.add_dataframe(io.read(filename), tag="scen1")
            #ensemble.add_dataframe_from_file(filename, tag="scen1") //TODO: I have replaced this with above until we can unpick the cicrular import issue
        for filename in ["./src/bulum/stats/tests/scenario_replicates/test_scen2_repl1.csv",
                         "./src/bulum/stats/tests/scenario_replicates/test_scen2_repl2.csv"]:
            ensemble.add_dataframe(io.read(filename), tag="scen2")
            #ensemble.add_dataframe_from_file(filename, tag="scen2") //TODO: I have replaced this with above until we can unpick the cicrular import issue
        plots.ensemble_probability_plot([ensemble.filter_tag("scen1"),ensemble.filter_tag("scen2")],
                                        variable="Storage",
                                        parameters=[100000,64000],
                                        stat_function=stats.annual_incremental_risk,
                                        labels=["Scenario 1", "Scenario 2"])
        plots.ensemble_probability_plot([ensemble.filter_tag("scen1"),ensemble.filter_tag("scen2")],
                                        variable="Storage",
                                        parameters=[64000],
                                        stat_function=stats.annual_incremental_risk)
        plots.ensemble_probability_plot([ensemble.filter_tag("scen1"),ensemble.filter_tag("scen2")],
                                        variable="Storage",
                                        parameters=64000,
                                        stat_function=stats.annual_incremental_risk,
                                        width=1200,
                                        height=600)                   
        
    def test_ensemble_plot_percentile(self):
        ensemble = utils.DataframeEnsemble()
        for filename in ["./src/bulum/stats/tests/scenario_replicates/test_scen1_repl1.csv",
                         "./src/bulum/stats/tests/scenario_replicates/test_scen1_repl2.csv"]:
            ensemble.add_dataframe(io.read(filename), tag="scen1")            
            #ensemble.add_dataframe_from_file(filename, tag="scen1") //TODO: I have replaced this with above until we can unpick the cicrular import issue
        for filename in ["./src/bulum/stats/tests/scenario_replicates/test_scen2_repl1.csv",
                         "./src/bulum/stats/tests/scenario_replicates/test_scen2_repl2.csv"]:
            ensemble.add_dataframe(io.read(filename), tag="scen2")
            #ensemble.add_dataframe_from_file(filename, tag="scen2") //TODO: I have replaced this with above until we can unpick the cicrular import issue
        plots.ensemble_probability_plot([ensemble.filter_tag("scen1"),ensemble.filter_tag("scen2")],
                                        variable="Storage",
                                        parameters=[0,10,25,50],
                                        stat_function=stats.percentile_envelope,
                                        labels=["Scenario 1", "Scenario 2"])
        plots.ensemble_probability_plot([ensemble.filter_tag("scen1"),ensemble.filter_tag("scen2")],
                                        variable="Storage",
                                        parameters=[10],
                                        stat_function=stats.percentile_envelope)
        plots.ensemble_probability_plot([ensemble.filter_tag("scen1"),ensemble.filter_tag("scen2")],
                                        variable="Storage",
                                        parameters=10,
                                        stat_function=stats.percentile_envelope,
                                        width=1200,
                                        height=600)        

