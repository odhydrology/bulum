import unittest

import pandas as pd

import bulum.stats as bsta
from bulum import io, utils
from bulum.stats.reliability_stats_class import Reliability


class Tests(unittest.TestCase):

    def test_annual_max(self):
        df = io.read_ts_csv("./src/bulum/io/tests/test_data.csv")
        answer_non_complete_wy = bsta.annual_max(df, 7, True)["rex_creek_sac_flow"]
        self.assertAlmostEqual(answer_non_complete_wy, 6.68182359932205)

        # Test not working for some reason
        df = io.read_ts_csv("./src/bulum/stats/tests/test_div_data.csv", r"%Y-%m-%d")
        answer_complete_wy = bsta.annual_max(df, 7)["Functions\\Functions\\Functions\\Functions@Results@ODH_RWA@$f_KurandaTWS (ML.day^-1)"]
        self.assertAlmostEqual(answer_complete_wy, 459.819458676998)

    def test_annual_min(self):
        df = io.read_ts_csv("./src/bulum/io/tests/test_data.csv")
        answer_non_complete_wy = bsta.annual_min(df, 7, True)["rex_creek_sac_flow"]
        self.assertAlmostEqual(answer_non_complete_wy, 6.68182359932205)

        # Test not working for some reason
        df = io.read_ts_csv("./src/bulum/stats/tests/test_div_data.csv", r"%Y-%m-%d")
        answer_complete_wy = bsta.annual_min(df, 7)["Functions\\Functions\\Functions\\Functions@Results@ODH_RWA@$f_KurandaTWS (ML.day^-1)"]
        self.assertAlmostEqual(answer_complete_wy, 452.252343499997)

    def test_annual_mean(self):
        df = io.read_ts_csv("./src/bulum/io/tests/test_data.csv")
        answer_non_complete_wy = bsta.annual_mean(df, 7, True)["rex_creek_sac_flow"]
        self.assertAlmostEqual(answer_non_complete_wy, 6.68182359932205)

        # Test not working for some reason
        df = io.read_ts_csv("./src/bulum/stats/tests/test_div_data.csv", r"%Y-%m-%d")
        answer_complete_wy = bsta.annual_mean(df, 7)["Functions\\Functions\\Functions\\Functions@Results@ODH_RWA@$f_KurandaTWS (ML.day^-1)"]
        self.assertAlmostEqual(answer_complete_wy, 457.095643460864)

    def test_annual_median(self):
        df = io.read_ts_csv("./src/bulum/io/tests/test_data.csv")
        answer_non_complete_wy = bsta.annual_median(df, 7, True)["rex_creek_sac_flow"]
        self.assertAlmostEqual(answer_non_complete_wy, 6.68182359932205)

        # Test not working for some reason
        df = io.read_ts_csv("./src/bulum/stats/tests/test_div_data.csv", r"%Y-%m-%d")
        answer_complete_wy = bsta.annual_median(df, 7)["Functions\\Functions\\Functions\\Functions@Results@ODH_RWA@$f_KurandaTWS (ML.day^-1)"]
        self.assertAlmostEqual(answer_complete_wy, 457.233468094998)

    @staticmethod
    def _missing_data_df(start="2000-07-01", wy_month=7):
        """Three complete water years from `start` of 1, 3 and 2 per day. The
        three years from `start` must not include 29 February.

        Column 'gap' has 10 missing days in the second year; column
        'all_missing' has all of the second year missing. The complete-year
        totals are 365 and 730. The second year has the highest values, so a
        gap there changes the maximum (a short gap) and the minimum (a full
        gap)."""
        start = pd.Timestamp(start)
        idx = pd.date_range(start, start + pd.DateOffset(years=3, days=-1), freq="D",
                            name="Date")
        wy = pd.Series(utils.get_wy(idx, wy_month), index=idx)
        first = wy.iloc[0]
        values = wy.map({first: 1.0, first + 1: 3.0, first + 2: 2.0})
        df = pd.DataFrame({"gap": values, "all_missing": values})
        second = start + pd.DateOffset(years=1)
        gap_start = second + pd.DateOffset(months=6)
        df.loc[gap_start:gap_start + pd.Timedelta(days=9), "gap"] = float("nan")
        df.loc[second:second + pd.DateOffset(years=1, days=-1), "all_missing"] = float("nan")
        return df

    def _missing_data_variants(self):
        """(name, df, wy_month) for the missing-data fixture with a string
        index and with other water year start months."""
        string_index = self._missing_data_df()
        string_index.index = string_index.index.strftime("%Y-%m-%d")
        return [
            ("July, date index", self._missing_data_df(), 7),
            ("July, string index", string_index, 7),
            ("January", self._missing_data_df("2001-01-01", 1), 1),
            ("October", self._missing_data_df("2000-10-01", 10), 10),
        ]

    def test_annual_stats_exclude_years_with_missing_data(self):
        # With allow_part_years=False, water years with missing data are
        # excluded. Each statistic has at least one column where counting
        # missing days as zero would give a different result.
        cases = [
            ("annual_max", bsta.annual_max, 730.0),
            ("annual_min", bsta.annual_min, 365.0),
            ("annual_mean", bsta.annual_mean, 547.5),
            ("annual_median", bsta.annual_median, 547.5),
            ("annual_percentile", lambda d, m: bsta.annual_percentile(d, 50, m).iloc[0], 547.5),
        ]
        for variant, df, wy_month in self._missing_data_variants():
            for name, func, expected in cases:
                result = func(df, wy_month)
                for col in df.columns:
                    with self.subTest(variant=variant, func=name, column=col):
                        self.assertAlmostEqual(result[col], expected)
                if name != "annual_percentile":  # Series input is tested separately
                    with self.subTest(variant=variant, func=name, column="gap as Series"):
                        self.assertAlmostEqual(func(df["gap"], wy_month), expected)

    def test_annual_stats_nan_without_complete_year(self):
        df = self._missing_data_df().iloc[:30]
        funcs = [
            ("annual_sum", bsta.annual_sum),
            ("annual_max", bsta.annual_max),
            ("annual_min", bsta.annual_min),
            ("annual_mean", bsta.annual_mean),
            ("annual_median", bsta.annual_median),
            ("annual_percentile", lambda d: bsta.annual_percentile(d, 50)),
        ]
        for name, func in funcs:
            with self.subTest(func=name):
                self.assertTrue(pd.isna(func(df)))

    def test_annual_sum_nan_for_years_with_missing_data(self):
        df = self._missing_data_df()
        result = bsta.annual_sum(df)
        columns = [(col, result[col]) for col in df.columns]
        columns.append(("gap as Series", bsta.annual_sum(df["gap"])))
        for col, totals in columns:
            with self.subTest(column=col):
                self.assertEqual(totals.loc[2000], 365.0)
                self.assertTrue(pd.isna(totals.loc[2001]))
                self.assertEqual(totals.loc[2002], 730.0)

    def test_annual_sum_part_years_still_skips_missing(self):
        # With allow_part_years=True, the available days are summed.
        df = self._missing_data_df()
        result = bsta.annual_sum(df, allow_part_years=True)
        self.assertEqual(result["gap"].loc[2001], 3.0 * 355)
        self.assertEqual(result["all_missing"].loc[2001], 0.0)

    def test_monthly_reliability(self):
        # Test timeseries demand input
        df = io.read_ts_csv("./src/bulum/stats/tests/test_dem_sup_data_trunc.csv")
        temp = bsta.Reliability(df["Functions\\Functions\\Functions\\Functions@ODH@$f_Irrigator_180day_Unrestricted (ML.day^-1)"],
                                df["Water User\\Irrigation_Demand(ODH)\\Demand Model\\Demand Model@Ordered Water Supplied (ML)"], quiet=True)

        answer_non_complete_months = temp.MonthlyReliability(.99, True)
        self.assertAlmostEqual(answer_non_complete_months, 0.863319386)

        answer_complete_months = temp.MonthlyReliability(.99)
        self.assertAlmostEqual(answer_complete_months, 0.863128492)

        # Test monthly list of daily demands
        df = io.read_ts_csv("./src/bulum/stats/tests/test_dem_sup_data_trunc.csv")
        temp = bsta.Reliability([8.5, 8.5, 8.5, 8.5, 0, 0, 0, 0, 0, 0, 8.5, 8.5],
                                df["Water User\\Irrigation_Demand(ODH)\\Demand Model\\Demand Model@Ordered Water Supplied (ML)"], demand_type="daily_constant", quiet=True)

        answer_non_complete_months_list = temp.MonthlyReliability(.99, True)
        self.assertAlmostEqual(answer_non_complete_months_list, 0.863319386)

        answer_complete_months_list = temp.MonthlyReliability(.99)
        self.assertAlmostEqual(answer_complete_months_list, 0.863128492)

        # Test monthly list of total demands
        df = io.read_ts_csv("./src/bulum/stats/tests/test_dem_sup_data_trunc.csv")
        temp_total = bsta.Reliability([263.5, 238, 263.5, 255, 0, 0, 0, 0, 0, 0, 255, 263.5],
                                      df["Water User\\Irrigation_Demand(ODH)\\Demand Model\\Demand Model@Ordered Water Supplied (ML)"], demand_type="total", ignore_leap_years=True, quiet=True)

        answer_non_complete_months_total_list = temp_total.MonthlyReliability(.99, True)
        self.assertAlmostEqual(answer_non_complete_months_total_list, 0.863319386)

        answer_complete_months_total_list = temp_total.MonthlyReliability(.99)
        self.assertAlmostEqual(answer_complete_months_total_list, 0.863128492)

        # Test daily constant demand
        df = io.read_ts_csv("./src/bulum/stats/tests/test_dem_sup_data_trunc.csv")
        temp_day_constant = bsta.Reliability(8.5, df["Random 8.5 Supply"], quiet=True)

        answer_non_complete_months_day_constant = temp_day_constant.MonthlyReliability(0.95, True)
        self.assertAlmostEqual(answer_non_complete_months_day_constant, 0.566945607)

        answer_complete_months_day_constant = temp_day_constant.MonthlyReliability(0.95)
        self.assertAlmostEqual(answer_complete_months_day_constant, 0.567039106)

        # Test annual constant demand
        df = io.read_ts_csv("./src/bulum/stats/tests/test_dem_sup_data_trunc.csv")
        temp_annual_constant = bsta.Reliability(3102.5, df["Random 8.5 Supply"], ignore_leap_years=True, demand_timescale="yearly", demand_type="total", quiet=True)

        answer_non_complete_months_annual_constant = temp_annual_constant.MonthlyReliability(0.95, True)
        self.assertAlmostEqual(answer_non_complete_months_annual_constant, 0.566945607)

        answer_complete_months_annual_constant = temp_annual_constant.MonthlyReliability(0.95)
        self.assertAlmostEqual(answer_complete_months_annual_constant, 0.567039106)

    def test_annual_reliability(self):
        df = io.read_ts_csv("./src/bulum/stats/tests/test_dem_sup_data_trunc.csv")
        temp = bsta.Reliability(df["Functions\\Functions\\Functions\\Functions@ODH@$f_Irrigator_180day_Unrestricted (ML.day^-1)"],
                                df["Water User\\Irrigation_Demand(ODH)\\Demand Model\\Demand Model@Ordered Water Supplied (ML)"], quiet=True)

        answer_non_complete_years = temp.AnnualReliability(.99, 7, True)
        self.assertAlmostEqual(answer_non_complete_years, 0.641666667)

        answer_complete_years = temp.AnnualReliability(.99, 7)
        self.assertAlmostEqual(answer_complete_years, 0.644067797)

        df = io.read_ts_csv("./src/bulum/stats/tests/test_dem_sup_data_trunc.csv")
        temp = bsta.Reliability([8.5, 8.5, 8.5, 8.5, 0, 0, 0, 0, 0, 0, 8.5, 8.5],
                                df["Water User\\Irrigation_Demand(ODH)\\Demand Model\\Demand Model@Ordered Water Supplied (ML)"], demand_type="daily_constant", quiet=True)

        answer_non_complete_years_list = temp.AnnualReliability(.99, 7, True)
        self.assertAlmostEqual(answer_non_complete_years_list, 0.641666667)

        answer_complete_years_list = temp.AnnualReliability(.99, 7)
        self.assertAlmostEqual(answer_complete_years_list, 0.644067797)

        df = io.read_ts_csv("./src/bulum/stats/tests/test_dem_sup_data_trunc.csv")
        temp_total = bsta.Reliability([263.5, 238, 263.5, 255, 0, 0, 0, 0, 0, 0, 255, 263.5],
                                      df["Water User\\Irrigation_Demand(ODH)\\Demand Model\\Demand Model@Ordered Water Supplied (ML)"], demand_type="total", ignore_leap_years=True, quiet=True)

        answer_non_complete_years_total_list = temp_total.AnnualReliability(.99, 7, True)
        self.assertAlmostEqual(answer_non_complete_years_total_list, 0.641666667)

        answer_complete_years_total_list = temp_total.AnnualReliability(.99, 7)
        self.assertAlmostEqual(answer_complete_years_total_list, 0.644067797)

        # Test daily constant demand
        df = io.read_ts_csv("./src/bulum/stats/tests/test_dem_sup_data_trunc.csv")
        temp_day_constant = bsta.Reliability(8.5, df["Random 8.5 Supply"], quiet=True)

        answer_non_complete_years_day_constant = temp_day_constant.AnnualReliability(0.95, 7, True)
        self.assertAlmostEqual(answer_non_complete_years_day_constant, 0.591666667)

        answer_complete_years_day_constant = temp_day_constant.AnnualReliability(0.95, 7)
        self.assertAlmostEqual(answer_complete_years_day_constant, 0.584745763)

        # Test annual constant demand
        df = io.read_ts_csv("./src/bulum/stats/tests/test_dem_sup_data_trunc.csv")
        temp_annual_constant = bsta.Reliability(3102.5, df["Random 8.5 Supply"], ignore_leap_years=True, demand_timescale="yearly", demand_type="total", quiet=True)

        answer_non_complete_years_annual_constant = temp_annual_constant.AnnualReliability(0.95, 7, True)
        self.assertAlmostEqual(answer_non_complete_years_annual_constant, 0.591666667)

        answer_complete_years_annual_constant = temp_annual_constant.AnnualReliability(0.95, 7)
        self.assertAlmostEqual(answer_complete_years_annual_constant, 0.584745763)

    def test_reliability_initialization_validation(self):
        """Test Reliability class initialization with various input validation scenarios."""
        # Test valid initialization
        supply = pd.Series([10, 20, 30], index=['2023-01-01', '2023-01-02', '2023-01-03'])
        demand = pd.Series([5, 15, 25], index=['2023-01-01', '2023-01-02', '2023-01-03'])

        # Valid timeseries input
        rel = Reliability(demand, supply, quiet=True)
        self.assertEqual(rel.state, "ts")

        # Valid list input
        rel_list = Reliability([8.5] * 12, supply, demand_type="daily_constant", quiet=True)
        self.assertEqual(rel_list.state, "monthly_constant_list")

        # Valid float input
        rel_float = Reliability(8.5, supply, quiet=True)
        self.assertEqual(rel_float.state, "daily_constant")

        # Test error cases
        with self.assertRaises(TypeError):
            Reliability("invalid", supply, quiet=True)  # Invalid demand type

        with self.assertRaises(TypeError):
            Reliability(demand, "invalid", quiet=True)  # Invalid supply type

        with self.assertRaises(ValueError):
            Reliability([8.5] * 11, supply, quiet=True)  # Invalid list length

        # Test additional specific error cases
        with self.assertRaises(ValueError):
            Reliability(8.5, supply, demand_type="invalid", quiet=True)  # Invalid demand_type

        with self.assertRaises(ValueError):
            Reliability(8.5, supply, demand_timescale="invalid", quiet=True)  # Invalid demand_timescale

    def test_reliability_ts_method(self):
        """Test ReliabilityTS method with different demand configurations."""
        supply = pd.Series([10, 20, 30, 40], index=['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04'])

        # Test with timeseries demand
        demand = pd.Series([5, 15, 25, 35], index=['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04'])
        rel = Reliability(demand, supply, quiet=True)
        demand_ts = rel.ReliabilityTS(wy_month=7)
        self.assertIsInstance(demand_ts, pd.Series)
        self.assertEqual(len(demand_ts), 4)

        # Test with daily constant
        rel_const = Reliability(8.5, supply, quiet=True)
        demand_ts_const = rel_const.ReliabilityTS(wy_month=7)
        self.assertTrue((demand_ts_const == 8.5).all())

    def test_reliability_edge_cases(self):
        """Test edge cases and boundary conditions."""
        # Create minimal test data
        supply = pd.Series([10, 20], index=['2023-01-01', '2023-01-02'])
        demand = pd.Series([5, 15], index=['2023-01-01', '2023-01-02'])

        # Test with tolerance edge cases
        rel = Reliability(demand, supply, quiet=True)

        # Test with 100% tolerance (tol=1)
        monthly_rel_100 = rel.MonthlyReliability(tol=1.0, allow_part_months=True)
        self.assertIsInstance(monthly_rel_100, (float, int))

        # Test with 0% tolerance (very strict)
        monthly_rel_0 = rel.MonthlyReliability(tol=0.0, allow_part_months=True)
        self.assertIsInstance(monthly_rel_0, (float, int))

        # Test annual reliability with edge cases
        annual_rel = rel.AnnualReliability(tol=1.0, allow_part_years=True)
        self.assertIsInstance(annual_rel, (float, int))

    def test_reliability_return_types(self):
        """Test that methods return correct types."""
        supply = pd.Series([10, 20, 30], index=['2023-01-01', '2023-01-02', '2023-01-03'])
        demand = pd.Series([5, 15, 25], index=['2023-01-01', '2023-01-02', '2023-01-03'])

        rel = Reliability(demand, supply, quiet=True)

        # Test ReliabilityTS returns pd.Series
        demand_ts = rel.ReliabilityTS(wy_month=7)
        self.assertIsInstance(demand_ts, pd.Series)

        # Test MonthlyReliability returns float
        monthly_rel = rel.MonthlyReliability(allow_part_months=True)
        self.assertIsInstance(monthly_rel, (float, int))

        # Test AnnualReliability returns float
        annual_rel = rel.AnnualReliability(allow_part_years=True)
        self.assertIsInstance(annual_rel, (float, int))

    def test_reliability_parameter_combinations(self):
        """Test various parameter combinations for Reliability class."""
        supply = pd.Series([10, 20, 30, 40, 50], index=['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05'])

        # Test different demand_timescale and demand_type combinations
        test_cases = [
            {"demand": 100, "demand_timescale": "yearly", "demand_type": "total"},
            {"demand": 10, "demand_timescale": "monthly", "demand_type": "total"},
            {"demand": 5, "demand_timescale": "daily", "demand_type": "daily_constant"},
        ]

        for case in test_cases:
            rel = Reliability(
                case["demand"],
                supply,
                demand_timescale=case["demand_timescale"],
                demand_type=case["demand_type"],
                quiet=True
            )
            # Verify initialization works
            self.assertIsNotNone(rel.state)

            # Test that methods work
            demand_ts = rel.ReliabilityTS(wy_month=7)
            self.assertIsInstance(demand_ts, pd.Series)


    def test_StochasticDataComparison(self):
        # Checking answers against PPT Historical outputs in Checking_LF_B.xlsm in 10010

        # Define an SDC
        df = io.read("./src/bulum/stats/tests/da_file/nogr306a.idx")
        sdc = bsta.StochasticDataComparison({"Historical": df.loc[:"2011-06-30", :]})

        # Test distributions
        check_dist_ann = sdc.Distributions["outputs"]["annual"]["7>PPT502Ba.q13>GS130502B Brown River at Brown Lake Part"]["Historical"].dropna().reset_index()
        read_dist_ann = pd.read_csv("./src/bulum/stats/tests/test_sdc_distr_ann_answers.csv")
        pd.testing.assert_frame_equal(round(check_dist_ann, 4), round(read_dist_ann, 4), check_dtype=False)

        check_dist_mon = sdc.Distributions["outputs"]["07"]["3>PPT_206A.q13>GS130206A Theresa Creek at Gregory Highw"]["Historical"].dropna().reset_index()
        read_dist_mon = pd.read_csv("./src/bulum/stats/tests/test_sdc_distr_mon_answers.csv")
        pd.testing.assert_frame_equal(round(check_dist_mon, 4), round(read_dist_mon, 4), check_dtype=False)

        # Test correlations
        # Annual
        read_corr_ann = pd.read_csv("./src/bulum/stats/tests/test_sdc_corr_ann_answers.csv")
        check_corr_ann = sdc.Correlations["outputs"]["annual"]["lag0"]["15>PPT_413A.q13>GS130413A Denison Creek at Braeside"]["Historical"]
        pd.testing.assert_series_equal(round(check_corr_ann, 3), read_corr_ann["Lag0"], check_index=False, check_names=False)

        check_corr_ann = sdc.Correlations["outputs"]["annual"]["lag1"]["15>PPT_413A.q13>GS130413A Denison Creek at Braeside"]["Historical"]
        pd.testing.assert_series_equal(round(check_corr_ann, 3), read_corr_ann["Lag1"], check_index=False, check_names=False)
        # Monthly
        read_corr_mon = pd.read_csv("./src/bulum/stats/tests/test_sdc_corr_mon_answers.csv")
        check_corr_mon = sdc.Correlations["outputs"]["02"]["lag0"]["12>PPT_106A.q13>GS130106A Mackenzie River at Bingegang"]["Historical"]
        pd.testing.assert_series_equal(round(check_corr_mon, 3), read_corr_mon["Lag0"], check_index=False, check_names=False)

        check_corr_mon = sdc.Correlations["outputs"]["02"]["lag1"]["12>PPT_106A.q13>GS130106A Mackenzie River at Bingegang"]["Historical"]
        pd.testing.assert_series_equal(round(check_corr_mon, 3), read_corr_mon["Lag1"], check_index=False, check_names=False)

        # Spot testing individual results
        self.assertEqual(round(sdc.Stats["outputs"]["annual"]["mean"]["Historical"]["20>PPT_410A.q13>GS130410A Isaac River at Deverill"], 1), 592.7)
        self.assertEqual(round(sdc.Stats["outputs"]["04"]["skew"]["Historical"]["20>PPT_410A.q13>GS130410A Isaac River at Deverill"], 3), 3.972)

        # Testing charts
        ## Force conversion to JSON so altair checks for errors.
        
        for timescale,vals in sdc.Stats["chts"].items():
            if timescale in ["01","07","annual"]:
                for chart in vals.values(): chart.to_json(format="vega")
            if timescale =="monthly":
                for stat,chart in vals.items():
                    for chart in list(chart.values())[::9]: chart.to_json(format="vega")

        for timescale,vals in sdc.Correlations["chts"].items():
            if timescale in ["02","08","annual","daily"]:
                for lag,col in vals.items():
                    for chart in list(col.values())[::9]: chart.to_json(format="vega")

        for timescale,vals in sdc.Correlations["heatmaps"].items():
            if timescale in ["03","09","annual","daily"]:
                for lag,col in vals.items():
                    for chart in list(col.values())[::9]: chart.to_json(format="vega")

        for timescale,col in sdc.Distributions["chts"].items():
            if timescale in ["04","10","annual","daily"]:
                for chart in list(col.values())[::9]: chart.to_json(format="vega")

    def test_cumulative_risk(self):
        ensemble = utils.DataframeEnsemble()
        for filename in ["./src/bulum/stats/tests/scenario_replicates/test_scen1_repl1.csv",
                         "./src/bulum/stats/tests/scenario_replicates/test_scen1_repl2.csv"]:
            ensemble.add_dataframe(io.read(filename), tag="scen1")
            # ensemble.add_dataframe_from_file(filename, tag="scen1") //TODO: I have replaced this with above until we can unpick the cicrular import issue
        self.assertEqual(bsta.cumulative_risk(input=ensemble.filter_tag("scen1"), variable="Storage", parameters=[100000, 64000]).__len__(), 2)
        self.assertAlmostEqual(bsta.cumulative_risk(input=ensemble.filter_tag("scen1"), variable="Storage", parameters=[100000, 64000])["64000"].mean(), 6.572572150832104)
        self.assertEqual(bsta.cumulative_risk(input=ensemble, variable="Storage", parameters=[64000]).__len__(), 1)
        self.assertEqual(bsta.cumulative_risk(input=ensemble.filter_tag("scen1"), variable="Storage", parameters=64000).__len__(), 1)

    def test_incremental_risk(self):
        ensemble = utils.DataframeEnsemble()
        for filename in ["./src/bulum/stats/tests/scenario_replicates/test_scen1_repl1.csv",
                         "./src/bulum/stats/tests/scenario_replicates/test_scen1_repl2.csv"]:
            ensemble.add_dataframe(io.read(filename), tag="scen1")
            # ensemble.add_dataframe_from_file(filename, tag="scen1") //TODO: I have replaced this with above until we can unpick the cicrular import issue
        self.assertEqual(bsta.incremental_risk(input=ensemble.filter_tag("scen1"), variable="Storage", parameters=[100000, 64000]).__len__(), 2)
        self.assertAlmostEqual(bsta.incremental_risk(input=ensemble.filter_tag("scen1"), variable="Storage", parameters=[100000, 64000])["64000"].mean(), 1.3692858647566883)
        self.assertEqual(bsta.incremental_risk(input=ensemble, variable="Storage", parameters=[64000]).__len__(), 1)
        self.assertEqual(bsta.incremental_risk(input=ensemble.filter_tag("scen1"), variable="Storage", parameters=64000).__len__(), 1)

    def test_annual_incremental_risk(self):
        ensemble = utils.DataframeEnsemble()
        for filename in ["./src/bulum/stats/tests/scenario_replicates/test_scen1_repl1.csv",
                         "./src/bulum/stats/tests/scenario_replicates/test_scen1_repl2.csv"]:
            ensemble.add_dataframe(io.read(filename), tag="scen1")
            # ensemble.add_dataframe_from_file(filename, tag="scen1") //TODO: I have replaced this with above until we can unpick the cicrular import issue
        self.assertEqual(bsta.annual_incremental_risk(input=ensemble.filter_tag("scen1"), variable="Storage", parameters=[100000, 64000]).__len__(), 2)
        self.assertAlmostEqual(bsta.annual_incremental_risk(input=ensemble.filter_tag("scen1"), variable="Storage", parameters=[100000, 64000])["64000"].mean(), 3.8461538461538463)
        self.assertEqual(bsta.annual_incremental_risk(input=ensemble, variable="Storage", parameters=[64000]).__len__(), 1)
        self.assertEqual(bsta.annual_incremental_risk(input=ensemble.filter_tag("scen1"), variable="Storage", parameters=64000).__len__(), 1)

    def test_percentile_envelope(self):
        ensemble = utils.DataframeEnsemble()
        for filename in ["./src/bulum/stats/tests/scenario_replicates/test_scen1_repl1.csv",
                         "./src/bulum/stats/tests/scenario_replicates/test_scen1_repl2.csv"]:
            ensemble.add_dataframe(io.read(filename), tag="scen1")
            # ensemble.add_dataframe_from_file(filename, tag="scen1") //TODO: I have replaced this with above until we can unpick the cicrular import issue
        self.assertEqual(bsta.percentile_envelope(input=ensemble.filter_tag("scen1"), variable="Storage", parameters=[0, 10, 25, 50]).__len__(), 4)
        self.assertAlmostEqual(bsta.percentile_envelope(input=ensemble.filter_tag("scen1"), variable="Storage", parameters=[0, 10, 25, 50])["10"].mean(), 130487.5073260038)
        self.assertEqual(bsta.percentile_envelope(input=ensemble, variable="Storage", parameters=[10]).__len__(), 1)
        self.assertEqual(bsta.percentile_envelope(input=ensemble.filter_tag("scen1"), variable="Storage", parameters=10).__len__(), 1)


if __name__ == '__main__':
    unittest.main()
