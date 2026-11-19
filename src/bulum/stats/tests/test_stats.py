import unittest

import numpy as np
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

    def test_annual_percentile_inputs(self):
        df = self._missing_data_df()
        cases = [
            ("scalar q", df, 50, [50]),
            ("list q", df, [10, 90], [10, 90]),
            ("tuple q", df, (10, 90), [10, 90]),
            ("array q", df, np.array([10, 90]), [10, 90]),
            ("series input", df["gap"], [10, 90], [10, 90]),
            ("lower bound", df, 0, [0]),
            ("upper bound", df, 100, [100]),
            ("both bounds", df, [0, 100], [0, 100]),
        ]
        for name, data, q, expected_index in cases:
            with self.subTest(case=name):
                result = bsta.annual_percentile(data, q)
                self.assertEqual(list(result.index), expected_index)
                self.assertAlmostEqual(np.ravel(result.loc[expected_index[-1]])[0],
                                       np.percentile([365.0, 730.0], expected_index[-1]))
        invalid = [
            (101, r"between 0 and 100, got \[101\]"),
            (-1, r"between 0 and 100, got \[-1\]"),
            ([50, 150.5], r"between 0 and 100, got \[50.0, 150.5\]"),
            (float("nan"), "between 0 and 100"),
            ("50", "must be numeric"),
            (None, "must be numeric"),
            (np.array([[5, 95]]), "1-D sequence"),
        ]
        for q, message in invalid:
            with self.subTest(case=f"invalid q={q!r}"):
                with self.assertRaisesRegex(ValueError, message):
                    bsta.annual_percentile(df, q)

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

    def test_annual_stats_part_years_with_missing_data(self):
        # With allow_part_years=True, the available days are summed, and a
        # water year with no values is excluded. A year of zeros still
        # totals zero.
        df = self._missing_data_df()
        df["zero_year"] = df["gap"].fillna(3.0)
        df.loc["2001-07-01":"2002-06-30", "zero_year"] = 0.0
        cases = [
            # (name, func, {column: expected})
            ("annual_max", bsta.annual_max,
             {"gap": 1065.0, "all_missing": 730.0, "zero_year": 730.0}),
            ("annual_min", bsta.annual_min,
             {"gap": 365.0, "all_missing": 365.0, "zero_year": 0.0}),
            ("annual_mean", bsta.annual_mean,
             {"gap": 720.0, "all_missing": 547.5, "zero_year": 365.0}),
            ("annual_median", bsta.annual_median,
             {"gap": 730.0, "all_missing": 547.5, "zero_year": 365.0}),
            ("annual_percentile",
             lambda d, allow_part_years: bsta.annual_percentile(
                 d, 50, allow_part_years=allow_part_years).iloc[0],
             {"gap": 730.0, "all_missing": 547.5, "zero_year": 365.0}),
        ]
        for name, func, expected in cases:
            result = func(df, allow_part_years=True)
            for col, value in expected.items():
                with self.subTest(func=name, column=col):
                    self.assertAlmostEqual(result[col], value)
        totals = bsta.annual_sum(df, allow_part_years=True)
        self.assertEqual(totals["gap"].loc[2001], 3.0 * 355)
        self.assertTrue(pd.isna(totals["all_missing"].loc[2001]))
        self.assertEqual(totals["zero_year"].loc[2001], 0.0)

    def test_annual_stats_part_years_with_missing_data_by_variant(self):
        # The water year start month and the index format also apply when
        # part years are allowed.
        for variant, df, wy_month in self._missing_data_variants():
            with self.subTest(variant=variant):
                result = bsta.annual_mean(df, wy_month, allow_part_years=True)
                self.assertAlmostEqual(result["gap"], (365.0 + 3.0 * 355 + 730.0) / 3)
                self.assertAlmostEqual(result["all_missing"], 547.5)

    def test_annual_stats_dates_missing_from_index(self):
        # Dates absent from the index count as missing values, so dropping
        # rows gives the same results as setting them to NaN.
        funcs = [
            ("annual_sum", bsta.annual_sum),
            ("annual_max", bsta.annual_max),
            ("annual_min", bsta.annual_min),
            ("annual_mean", bsta.annual_mean),
            ("annual_median", bsta.annual_median),
            ("annual_percentile", lambda d, m, allow_part_years: bsta.annual_percentile(
                d, [10, 90], m, allow_part_years)),
        ]
        for variant, df, wy_month in self._missing_data_variants():
            for col in df.columns:
                with_nan = df[[col]]
                dropped = with_nan.dropna()
                self.assertLess(len(dropped), len(with_nan))
                for allow_part_years in (False, True):
                    for name, func in funcs:
                        with self.subTest(variant=variant, column=col, func=name,
                                          allow_part_years=allow_part_years):
                            expected = func(with_nan, wy_month, allow_part_years)
                            result = func(dropped, wy_month, allow_part_years)
                            if isinstance(expected, pd.DataFrame):
                                pd.testing.assert_frame_equal(result, expected)
                            else:
                                pd.testing.assert_series_equal(result, expected)

    def test_annual_stats_unsorted_index(self):
        # An unsorted index has the right number of rows but is not treated
        # as complete without checking; it gives the same results as sorted.
        df = self._missing_data_df()
        string_index = df.set_axis(df.index.strftime("%Y-%m-%d"))
        shuffled_order = np.random.default_rng(0).permutation(len(df))
        for variant, data in (("date index", df), ("string index", string_index)):
            for order_name, unsorted in (("reversed", data.iloc[::-1]),
                                         ("shuffled", data.iloc[shuffled_order])):
                for allow_part_years in (False, True):
                    with self.subTest(variant=variant, order=order_name,
                                      allow_part_years=allow_part_years):
                        pd.testing.assert_frame_equal(
                            bsta.annual_sum(unsorted, allow_part_years=allow_part_years),
                            bsta.annual_sum(data, allow_part_years=allow_part_years))

    def test_annual_stats_index_that_looks_complete(self):
        # Indexes with one row per day between their end dates that still
        # have a date missing.
        df = self._missing_data_df()[["gap"]].fillna(1.0)
        missing = "2002-01-20"
        moved = "2003-07-01"  # outside the end dates, placed mid-series
        extra = df.loc[["2003-06-30"]].set_axis(pd.DatetimeIndex([moved], name="Date"))
        position = df.index.get_loc(pd.Timestamp(missing))
        out_of_order = pd.concat([df.iloc[:position], extra, df.iloc[position + 1:]])
        expected = bsta.annual_sum(out_of_order.sort_index())
        with self.subTest(case="date missing, extra date out of order"):
            self.assertEqual(len(out_of_order), len(df))
            pd.testing.assert_frame_equal(bsta.annual_sum(out_of_order), expected)
            self.assertTrue(pd.isna(bsta.annual_sum(out_of_order)["gap"].loc[2001]))
        duplicated = df.drop(pd.Timestamp(missing))
        duplicated = pd.concat([duplicated, duplicated.loc[["2002-01-21"]]]).sort_index()
        with self.subTest(case="date missing, another date duplicated"):
            self.assertEqual(len(duplicated), len(df))
            with self.assertRaises(ValueError):
                bsta.annual_sum(duplicated)

    def test_annual_sum_water_year_totals(self):
        nan = float("nan")
        full = self._missing_data_df()
        part = full.loc["2000-09-01":]  # WY2000 starts in September: 303 days
        cases = [
            # (name, df, allow_part_years, {water year: (gap, all_missing)})
            ("complete years", full, False,
             {2000: (365.0, 365.0), 2001: (nan, nan), 2002: (730.0, 730.0)}),
            ("complete years, part allowed", full, True,
             {2000: (365.0, 365.0), 2001: (1065.0, nan), 2002: (730.0, 730.0)}),
            ("part year cropped", part, False,
             {2001: (nan, nan), 2002: (730.0, 730.0)}),
            ("part year kept", part, True,
             {2000: (303.0, 303.0), 2001: (1065.0, nan), 2002: (730.0, 730.0)}),
        ]
        for name, df, allow_part_years, expected in cases:
            with self.subTest(case=name):
                result = bsta.annual_sum(df, 7, allow_part_years)
                expected_df = pd.DataFrame.from_dict(
                    expected, orient="index", columns=["gap", "all_missing"])
                pd.testing.assert_frame_equal(result, expected_df, check_index_type=False)

    def test_annual_sum_part_year_without_complete_year(self):
        df = self._missing_data_df().iloc[:30]
        self.assertEqual(len(bsta.annual_sum(df, allow_part_years=True)), 1)

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
