import unittest
import warnings
from datetime import datetime

import numpy as np
import pandas as pd

import bulum.io as bio
from bulum import utils


class Tests(unittest.TestCase):

    def test_meets_ts_standards_1(self):
        df = pd.read_csv("./src/bulum/utils/tests/test_data.csv")
        violations = utils.check_df_format_standards(df)
        # There should be one violation because the index are still integers
        self.assertEqual(violations, ["Dataframe index name is not 'Date'"])

    def test_meets_ts_standards_2(self):
        df = pd.read_csv("./src/bulum/utils/tests/test_data.csv")
        df["Date"] = pd.to_datetime(df["Date"])
        df.set_index("Date", inplace=True)
        df.index.name = "date"  # Change to lowercase to cause violation
        violations = utils.check_df_format_standards(df)
        self.assertEqual(violations, ["Dataframe index name is not 'Date'"])

    def test_meets_ts_standards_3(self):
        df = pd.read_csv("./src/bulum/utils/tests/test_data.csv")
        df["Date"] = utils.standardize_datestring_format(df["Date"].values)
        df.set_index("Date", inplace=True)
        # There should be nothing wrong
        violations = utils.check_df_format_standards(df)
        self.assertEqual(violations, [])

    def test_meets_ts_standards_4(self):
        df = pd.read_csv("./src/bulum/utils/tests/test_data_missing.csv")
        df["Date"] = utils.standardize_datestring_format(df["Date"].values)
        df.set_index("Date", inplace=True)
        # The missing value will be read as a string, causing the column to have type "object"
        violations = utils.check_df_format_standards(df)
        self.assertEqual(violations, [f"Column 'col_1' is not int64 or float64: object"])

    def test_generate_dates(self):
        dates = utils.get_dates(datetime(2000, 1, 1), datetime(2020, 1, 4))
        n = 7308
        self.assertEqual(len(dates), n)
        self.assertEqual(dates[0], datetime(2000, 1, 1))
        self.assertEqual(dates[n-1], datetime(2020, 1, 3))

    def test_generate_string_dates(self):
        date_strings = utils.get_dates('2000-01-01', '2020-01-04')  # it should automatically determine the string format. str_format=r"%Y-%m-%d"
        n = 7308
        self.assertEqual(len(date_strings), n)
        self.assertEqual(date_strings[0], '2000-01-01')
        self.assertEqual(date_strings[n-1], '2020-01-03')

    def test_wy(self):
        dates = utils.get_dates(datetime(2000, 1, 1), datetime(2020, 1, 4), str_format=r"%Y-%m-%d")
        wy = utils.get_wy(dates)  # default conventions are wy_month=7 and using_end_year=False
        self.assertEqual(len(dates), len(wy))
        self.assertEqual(wy[0], 1999)  # with a default wy_month (=7), the WY on the first date should be 1999
        self.assertEqual(wy[len(wy) - 1], 2019)  # with a default wy_month (=7), the WY on the last date should be 2019
        wy = utils.get_wy(dates, wy_month=1)
        self.assertEqual(wy[len(wy) - 1], 2020)  # with the above custom wy_month=1, the WY on the last date should be 2020
        wy = utils.get_wy(dates, wy_month=1, using_end_year=True)
        self.assertEqual(wy[len(wy) - 1], 2021)  # with the above custom wy_month=1, and the fiscal conventions (using_end_year=True), the WY on the last date should be 2021

    def test_stochastic_get_wy(self):
        wy1 = utils.get_wy(["0001-01-01"])
        self.assertEqual(wy1[0], 0)

        wy2 = utils.get_wy(["0001-07-01"])
        self.assertEqual(wy2[0], 1)

        wy3 = utils.get_wy(["9999-12-30"])
        self.assertEqual(wy3[0], 9999)

        # This one is most important as there is potential edge case errors.
        wy4 = utils.get_wy(["9999-12-31"])
        self.assertEqual(wy4[0], 9999)

    def test_set_index_dt(self):
        df = pd.DataFrame()
        df["Date"] = utils.get_dates(datetime(2000, 1, 1), datetime(2000, 1, 8))
        df["y1"] = 1
        df["y2"] = 2
        ans = utils.set_index_dt(df)
        self.assertEqual(len(ans), 7)
        self.assertEqual(len(ans.columns), 2)  # "Date" is converted to the index thus we only have 2 columns left
        self.assertFalse("Date" in ans.columns)

    def test_set_index_dt_lowercase_whitespace(self):
        df = pd.DataFrame()
        df["date "] = utils.get_dates(datetime(2000, 1, 1), datetime(2000, 1, 8))
        df["y1"] = 1
        df["y2"] = 2
        ans = utils.set_index_dt(df)
        self.assertEqual(len(ans), 7)
        self.assertEqual(len(ans.columns), 2)  # "date " is converted to the index "Date" thus we only have 2 columns left
        self.assertFalse("Date" in ans.columns)

    # TODO: this is a test for future functionality. I want to automatically attempt to parse
    #       the first column as dates, if no column can be found by name.
    #
    # def test_set_index_dt_first_colum_are_dates_but_name_is_weird(self):
    #     df = pd.DataFrame()
    #     df["year-month-day"] = utils.get_dates(datetime(2000,1,1), datetime(2000,1,8))
    #     df["y1"] = 1
    #     df["y2"] = 2
    #     ans = utils.set_index_dt(df)
    #     self.assertEqual(len(ans), 7)
    #     self.assertEqual(len(ans.columns), 3) #"date " is converted to the index "Date" thus we only have 2 columns left
    #     self.assertFalse("Date" in ans.columns)

    def test_set_index_dt_ignored(self):
        df = pd.DataFrame()
        df["Date"] = utils.get_dates(datetime(2004, 1, 1), datetime(2004, 1, 8))
        df["date"] = utils.get_dates(datetime(2001, 1, 1), datetime(2001, 1, 8))
        df["y1"] = 1
        df["y2"] = 2
        ans = utils.set_index_dt(df)
        self.assertEqual(len(ans.columns), 3)
        self.assertTrue("date" in ans.columns)  # "date" should still be a column
        self.assertFalse("Date" in ans.columns)  # but "Date" (the index) should not be a column
        self.assertTrue(min(ans.index).year == 2004)  # Make sure the index dates are in 2004, not 2001
        ans2 = utils.set_index_dt(ans)
        self.assertTrue(set(ans.columns) == set(ans2.columns))

    def test_set_index_dt_start_dt(self):
        df = pd.DataFrame()
        df["y1"] = [i for i in range(9)]
        df["y2"] = 2
        ans = utils.set_index_dt(df, start_dt=datetime(2000, 1, 1))
        self.assertEqual(len(ans.columns), 2)
        self.assertFalse("Date" in ans.columns)
        self.assertEqual(max(ans.index), datetime(2000, 1, 9))

    def test_set_index_dt_values(self):
        df = pd.DataFrame()
        df["y1"] = [i for i in range(9)]
        df["y2"] = 2
        ans = utils.set_index_dt(df, dt_values=utils.get_dates(datetime(2000, 1, 1), days=999))
        self.assertEqual(len(ans.columns), 2)
        self.assertFalse("Date" in ans.columns)
        self.assertEqual(max(ans.index), datetime(2000, 1, 9))

    def test_check_data_equivalence_true(self):
        df = pd.read_csv("./src/bulum/utils/tests/test_data.csv")
        df["Date"] = pd.to_datetime(df["Date"], format=r"%d/%m/%Y")
        df.set_index("Date", inplace=True)
        df2 = df.copy(deep=True)
        self.assertTrue(utils.check_data_equivalence(df, df2))
        self.assertFalse(utils.check_data_equivalence(df, df2 * 1.00001))

    def test_wy_start_date(self):
        df = bio.read_ts_csv("./src/bulum/stats/tests/test_div_data.csv", r"%Y-%m-%d")
        start_jul = utils.get_wy_start_date(df)
        start_jan = utils.get_wy_start_date(df, 1)
        self.assertEqual(start_jul, datetime(1889, 7, 1))
        self.assertEqual(start_jan, datetime(1890, 1, 1))

    def test_wy_end_date(self):
        df = bio.read_ts_csv("./src/bulum/stats/tests/test_div_data.csv", r"%Y-%m-%d")
        start_jul = utils.get_wy_end_date(df)
        start_mar = utils.get_wy_end_date(df, 3)
        self.assertEqual(start_jul, datetime(1919, 6, 30))
        self.assertEqual(start_mar, datetime(1920, 2, 29))

    # ==== NEW EDGE CASE TESTS ====

    def test_get_wy_leap_years(self):
        """Test get_wy with leap year dates"""
        # Test leap year February 29th
        leap_dates = ["2020-02-28", "2020-02-29", "2020-03-01"]
        wy_results = utils.get_wy(leap_dates, wy_month=3)  # March start
        self.assertEqual(wy_results[0], 2019)  # Feb 28 is in WY 2019
        self.assertEqual(wy_results[1], 2019)  # Feb 29 is in WY 2019
        self.assertEqual(wy_results[2], 2020)  # Mar 1 starts WY 2020

        # Test non-leap year boundary
        non_leap_dates = ["2021-02-28", "2021-03-01"]
        wy_results = utils.get_wy(non_leap_dates, wy_month=3)
        self.assertEqual(wy_results[0], 2020)  # Feb 28 is in WY 2020
        self.assertEqual(wy_results[1], 2021)  # Mar 1 starts WY 2021

    def test_get_wy_boundary_dates(self):
        """Test get_wy with water year boundary dates"""
        # Test individual boundary dates (get_wy doesn't require consecutive dates for individual calls)
        wy_june30_2023 = utils.get_wy(["2023-06-30"], wy_month=7)
        self.assertEqual(wy_june30_2023[0], 2022)  # June 30 is end of WY 2022

        wy_july1_2023 = utils.get_wy(["2023-07-01"], wy_month=7)
        self.assertEqual(wy_july1_2023[0], 2023)  # July 1 starts WY 2023

        wy_june30_2024 = utils.get_wy(["2024-06-30"], wy_month=7)
        self.assertEqual(wy_june30_2024[0], 2023)  # June 30 is end of WY 2023

        wy_july1_2024 = utils.get_wy(["2024-07-01"], wy_month=7)
        self.assertEqual(wy_july1_2024[0], 2024)  # July 1 starts WY 2024

    def test_get_wy_using_end_year_parameter(self):
        """Test get_wy with using_end_year=True (fiscal convention)"""
        # Test individual dates instead of non-consecutive sequence

        # Standard convention (using_end_year=False)
        wy_std_june = utils.get_wy(["2023-06-30"], wy_month=7, using_end_year=False)
        self.assertEqual(wy_std_june[0], 2022)

        wy_std_july = utils.get_wy(["2023-07-01"], wy_month=7, using_end_year=False)
        self.assertEqual(wy_std_july[0], 2023)

        # Fiscal convention (using_end_year=True)
        wy_fiscal_june = utils.get_wy(["2023-06-30"], wy_month=7, using_end_year=True)
        self.assertEqual(wy_fiscal_june[0], 2023)

        wy_fiscal_july = utils.get_wy(["2023-07-01"], wy_month=7, using_end_year=True)
        self.assertEqual(wy_fiscal_july[0], 2024)

    def test_get_wy_different_water_year_months(self):
        """Test get_wy with various water year start months"""
        test_date = ["2023-06-15"]

        # Test different water year start months
        for wy_month in range(1, 13):
            wy_result = utils.get_wy(test_date, wy_month=wy_month)
            self.assertIsInstance(wy_result[0], (int, np.integer))
            self.assertTrue(1 <= wy_result[0] <= 9999)

    def test_parse_date_components_valid_inputs(self):
        """Test _parse_date_components with valid inputs"""
        # Test string input
        year, month, day = utils.datetime_functions._parse_date_components("2023-12-25")
        self.assertEqual((year, month, day), (2023, 12, 25))

        # Test datetime input
        dt = datetime(2023, 12, 25, 10, 30, 45)
        year, month, day = utils.datetime_functions._parse_date_components(dt)
        self.assertEqual((year, month, day), (2023, 12, 25))

    def test_parse_date_components_invalid_inputs(self):
        """Test _parse_date_components with invalid inputs"""
        # Test invalid string format (too short)
        with self.assertRaises(ValueError) as cm:
            utils.datetime_functions._parse_date_components("2023-13")
        self.assertIn("must be in YYYY-MM-DD format", str(cm.exception))

        # Test malformed date string (non-numeric parts)
        with self.assertRaises(ValueError) as cm:
            utils.datetime_functions._parse_date_components("2023-ab-01")
        self.assertIn("Invalid date string format", str(cm.exception))

        # Test invalid type (integer) - should raise TypeError
        with self.assertRaises(TypeError) as cm:
            utils.datetime_functions._parse_date_components(20231225)
        self.assertIn("Expected str or datetime, got int", str(cm.exception))

        # Test invalid type (None) - should raise TypeError
        with self.assertRaises(TypeError) as cm:
            utils.datetime_functions._parse_date_components(None)
        self.assertIn("Expected str or datetime, got NoneType", str(cm.exception))

        with self.assertRaises(TypeError) as cm:
            utils.datetime_functions._parse_date_components(["2023-01-01"])

        # Test invalid type (float) - should raise TypeError
        with self.assertRaises(TypeError) as cm:
            utils.datetime_functions._parse_date_components(2023.0)
        self.assertIn("Expected str or datetime, got float", str(cm.exception))

    def test_get_date_format_edge_cases(self):
        """Test get_date_format with edge cases and error handling"""
        # Test valid formats
        self.assertEqual(utils.get_date_format("2023-12-25"), r'%Y-%m-%d')
        self.assertEqual(utils.get_date_format("25/12/2023"), r'%d/%m/%Y')

        # Test invalid format
        with self.assertRaises(ValueError) as cm:
            utils.get_date_format("invalid_date")
        self.assertIn("Invalid date format", str(cm.exception))
        self.assertIn("Supported formats:", str(cm.exception))

    def test_standardize_datestring_format_edge_cases(self):
        """Test standardize_datestring_format with various edge cases"""
        # Test mixed format detection (should use first date's format)
        dates = ["25/12/2023", "26/12/2023", "27/12/2023"]
        standardized = utils.standardize_datestring_format(dates)
        self.assertEqual(standardized, ["2023-12-25", "2023-12-26", "2023-12-27"])

        # Test single date
        single_date = utils.standardize_datestring_format(["01/01/2023"])
        self.assertEqual(single_date, ["2023-01-01"])

    def test_to_np_datetimes64d_edge_cases(self):
        """Test to_np_datetimes64d with edge cases"""
        # Test end date boundary (9999-12-31)
        boundary_dates = ["9999-12-30", "9999-12-31"]
        np_dates = utils.to_np_datetimes64d(boundary_dates)
        self.assertEqual(len(np_dates), 2)
        # numpy datetime64 includes time component when converted to string
        self.assertTrue(str(np_dates[-1]).startswith("9999-12-31"))

        # Test non-consecutive dates should give warning (not error)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            np_dates = utils.to_np_datetimes64d(["2023-01-01", "2023-01-03"])  # Missing 2023-01-02
            # Should have warned
            self.assertEqual(len(w), 1)
            self.assertIn("Date sequence validation", str(w[0].message))
            # But still returns all 3 dates (fills in missing dates)
            self.assertEqual(len(np_dates), 3)

    def test_get_dates_error_handling(self):
        """Test get_dates with invalid parameters"""
        # Test invalid years parameter
        with self.assertRaises(ValueError) as cm:
            utils.get_dates(datetime(2023, 1, 1), years=0)
        self.assertIn("Invalid years parameter", str(cm.exception))

        with self.assertRaises(ValueError) as cm:
            utils.get_dates(datetime(2023, 1, 1), years=-1)
        self.assertIn("Use end_date parameter", str(cm.exception))

    def test_get_dates_with_end_date_and_include_flag(self):
        """Test get_dates with end_date and include_end_date flag"""
        start = datetime(2023, 1, 1)
        end = datetime(2023, 1, 3)

        # Without including end date
        dates_excl = utils.get_dates(start, end_date=end, include_end_date=False)
        self.assertEqual(len(dates_excl), 2)  # Jan 1, Jan 2
        self.assertEqual(dates_excl[-1], datetime(2023, 1, 2))

        # Including end date
        dates_incl = utils.get_dates(start, end_date=end, include_end_date=True)
        self.assertEqual(len(dates_incl), 3)  # Jan 1, Jan 2, Jan 3
        self.assertEqual(dates_incl[-1], datetime(2023, 1, 3))

    def test_performance_large_dataset(self):
        """Test performance with large datasets"""
        # Generate a large date range (2 years = ~730 days for faster testing)
        start_date = datetime(2010, 1, 1)
        large_dates = utils.get_dates(start_date, years=2, str_format=r"%Y-%m-%d")

        # Test get_wy performance
        wy_results = utils.get_wy(large_dates)
        self.assertEqual(len(wy_results), len(large_dates))
        self.assertTrue(all(isinstance(wy, (int, np.integer)) for wy in wy_results))

        # Test standardize_datestring_format performance
        standardized = utils.standardize_datestring_format(large_dates)
        self.assertEqual(len(standardized), len(large_dates))

        # Verify first and last dates
        self.assertEqual(standardized[0], "2010-01-01")
        self.assertEqual(standardized[-1], "2011-12-31")

    def test_get_month_function(self):
        """Test get_month function with various inputs"""
        # Note: get_month requires consecutive dates due to to_np_datetimes64d
        dates = ["2023-01-15", "2023-01-16", "2023-01-17"]  # Consecutive dates
        months = utils.get_month(dates)
        self.assertEqual(months, [1, 1, 1])

        # Test with single date
        single_month = utils.get_month(["2023-06-15"])
        self.assertEqual(single_month, [6])

    def test_get_year_and_month_edge_cases(self):
        """Test get_year_and_month with edge cases"""
        # Test with datetime objects
        dt_list = [datetime(2023, 1, 15), datetime(2023, 12, 31)]
        year_months = utils.get_year_and_month(dt_list)
        self.assertEqual(year_months, ["2023-01", "2023-12"])

        # Test with empty list
        empty_result = utils.get_year_and_month([])
        self.assertEqual(empty_result, [])

    def test_standardize_datestring_format_stochastic_early_dates(self):
        """Test standardize_datestring_format with dates before 1677 (outside numpy datetime64 range).

        Stochastic model outputs can have dates from year 0001 to 9999.
        Numpy datetime64 only supports approximately 1677-2262.
        This test ensures the function handles early dates by keeping them as strings.
        """
        # Test very early dates
        early_dates = ["01/01/0001", "02/01/0001", "03/01/0001"]
        standardized = utils.standardize_datestring_format(early_dates)
        self.assertEqual(standardized, ["0001-01-01", "0001-01-02", "0001-01-03"])
        # Verify all are strings
        self.assertTrue(all(isinstance(d, str) for d in standardized))

    def test_standardize_datestring_format_stochastic_late_dates(self):
        """Test standardize_datestring_format with dates after 2262 (outside numpy datetime64 range).

        This ensures dates in year 9999 are handled correctly as strings.
        """
        # Test very late dates (year 9999)
        late_dates = ["29/12/9999", "30/12/9999", "31/12/9999"]
        standardized = utils.standardize_datestring_format(late_dates)
        self.assertEqual(standardized, ["9999-12-29", "9999-12-30", "9999-12-31"])
        # Verify all are strings
        self.assertTrue(all(isinstance(d, str) for d in standardized))

    def test_standardize_datestring_format_stochastic_full_range(self):
        """Test standardize_datestring_format with dates spanning from year 0001 to 9999.

        Note: Function returns all dates between first and last date, so non-consecutive
        input dates will result in a full date range with a warning.
        """
        # Test dates at boundaries of stochastic range (non-consecutive)
        boundary_dates = ["01/01/0001", "15/06/5000", "31/12/9999"]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")  # Suppress expected warning
            standardized = utils.standardize_datestring_format(boundary_dates)
        # Function returns all dates from first to last
        self.assertEqual(standardized[0], "0001-01-01")
        self.assertEqual(standardized[-1], "9999-12-31")
        # Should have all dates in the range (3,651,694 days)
        self.assertEqual(len(standardized), 3652059)
        # Verify all are strings
        self.assertTrue(all(isinstance(d, str) for d in standardized))

    def test_to_np_datetimes64d_with_pandas_series(self):
        """Test to_np_datetimes64d handles pandas Series input correctly.

        Pandas Series use label-based indexing, not position-based indexing.
        This test ensures the function converts Series to list before processing.
        """
        # Create a pandas Series with date strings
        dates_series = pd.Series(["2023-01-01", "2023-01-02", "2023-01-03"])
        np_dates = utils.to_np_datetimes64d(dates_series)
        self.assertEqual(len(np_dates), 3)
        self.assertTrue(str(np_dates[0]).startswith("2023-01-01"))
        self.assertTrue(str(np_dates[-1]).startswith("2023-01-03"))

    def test_standardize_datestring_format_no_timestamps(self):
        """Test that standardize_datestring_format returns clean date strings without timestamps.

        Ensures output format is exactly YYYY-MM-DD without any time component
        like "2000-01-01T00:00:00.000000".
        """
        # Test single date
        result = utils.standardize_datestring_format(['01/01/2000'])
        self.assertEqual(result, ['2000-01-01'])
        self.assertNotIn('T', result[0])
        self.assertEqual(len(result[0]), 10)

        # Test multiple dates
        result = utils.standardize_datestring_format(['25/12/2023', '26/12/2023', '27/12/2023'])
        self.assertEqual(result, ['2023-12-25', '2023-12-26', '2023-12-27'])
        for date_str in result:
            self.assertNotIn('T', date_str)
            self.assertEqual(len(date_str), 10)
            self.assertIsInstance(date_str, str)

        # Test dates in different year ranges
        result = utils.standardize_datestring_format(['15/06/1995'])
        self.assertEqual(result, ['1995-06-15'])
        self.assertNotIn('T', result[0])

    def test_standardize_datestring_format_already_standardized(self):
        """Test that standardize_datestring_format handles already standardized dates."""
        # Test dates already in YYYY-MM-DD format
        result = utils.standardize_datestring_format(['2023-01-01', '2023-01-02'])
        self.assertEqual(result, ['2023-01-01', '2023-01-02'])
        for date_str in result:
            self.assertNotIn('T', date_str)
            self.assertEqual(len(date_str), 10)
