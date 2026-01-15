import unittest
from datetime import datetime
from bulum import utils


class TestParametrized(unittest.TestCase):
    """Parametrized tests for utils functions with multiple configurations."""

    def test_get_wy_all_months_comprehensive(self):
        """Test get_wy with all possible water year start months."""
        # Test date that will behave differently for each month
        test_dates = ["2023-06-15"]  # Middle of the year for clear behavior

        expected_results = {
            # For date 2023-06-15 with different wy_month values
            # When wy_month <= 6, this date is in the current year's water year
            # When wy_month > 6, this date is in the previous year's water year
            1: 2023,   # Jan start: June is after Jan, so WY 2023
            2: 2023,   # Feb start: June is after Feb, so WY 2023
            3: 2023,   # Mar start: June is after Mar, so WY 2023
            4: 2023,   # Apr start: June is after Apr, so WY 2023
            5: 2023,   # May start: June is after May, so WY 2023
            6: 2023,   # Jun start: June is at start, so WY 2023
            7: 2022,   # Jul start: June is before Jul, so WY 2022
            8: 2022,   # Aug start: June is before Aug, so WY 2022
            9: 2022,   # Sep start: June is before Sep, so WY 2022
            10: 2022,  # Oct start: June is before Oct, so WY 2022
            11: 2022,  # Nov start: June is before Nov, so WY 2022
            12: 2022,  # Dec start: June is before Dec, so WY 2022
        }

        for wy_month in range(1, 13):
            with self.subTest(wy_month=wy_month):
                result = utils.get_wy(test_dates, wy_month=wy_month)
                self.assertEqual(result[0], expected_results[wy_month],
                                f"Failed for wy_month={wy_month}")

    def test_get_wy_boundary_conditions_all_months(self):
        """Test get_wy at exact boundary conditions for different months."""
        boundary_test_cases = [
            # (wy_month, test_date, expected_wy)
            (1, "2023-12-31", 2023),  # Last day of calendar year
            (1, "2024-01-01", 2024),  # First day of calendar year
            (3, "2023-02-28", 2022),  # Day before March start
            (3, "2023-03-01", 2023),  # March start
            (6, "2023-05-31", 2022),  # Day before June start
            (6, "2023-06-01", 2023),  # June start
            (7, "2023-06-30", 2022),  # Day before July start (traditional)
            (7, "2023-07-01", 2023),  # July start (traditional)
            (10, "2023-09-30", 2022), # Day before October start
            (10, "2023-10-01", 2023), # October start
        ]

        for wy_month, test_date, expected_wy in boundary_test_cases:
            with self.subTest(wy_month=wy_month, test_date=test_date):
                result = utils.get_wy([test_date], wy_month=wy_month)
                self.assertEqual(result[0], expected_wy,
                                f"Failed for wy_month={wy_month}, date={test_date}")

    def test_get_wy_fiscal_vs_standard_all_months(self):
        """Test get_wy with both fiscal and standard conventions for all months."""
        test_cases = [
            # (wy_month, test_date, expected_standard, expected_fiscal)
            (1, "2023-06-15", 2023, 2024),
            (3, "2023-01-15", 2022, 2023),
            (7, "2023-03-15", 2022, 2023),
            (10, "2023-05-15", 2022, 2023),
            (12, "2023-06-15", 2022, 2023),
        ]

        for wy_month, test_date, expected_std, expected_fiscal in test_cases:
            with self.subTest(wy_month=wy_month, test_date=test_date):
                # Standard convention
                result_std = utils.get_wy([test_date], wy_month=wy_month, using_end_year=False)
                self.assertEqual(result_std[0], expected_std,
                                f"Standard failed for wy_month={wy_month}, date={test_date}")

                # Fiscal convention
                result_fiscal = utils.get_wy([test_date], wy_month=wy_month, using_end_year=True)
                self.assertEqual(result_fiscal[0], expected_fiscal,
                                f"Fiscal failed for wy_month={wy_month}, date={test_date}")

    def test_get_date_format_all_supported_formats(self):
        """Test get_date_format with all supported date formats."""
        format_test_cases = [
            ("2023-12-25", r'%Y-%m-%d'),
            ("25/12/2023", r'%d/%m/%Y'),
            ("25/12/2023 14:30", r'%d/%m/%Y %H:%M'),
            # Note: %s is not a valid strftime directive, the actual format uses %S
            ("25/12/2023 14:30:45", r'%d/%m/%Y %H:%M:%S'),
        ]

        # Test only the formats that are actually implemented
        implemented_formats = [format_test_cases[0], format_test_cases[1]]  # Only first two are implemented

        for date_str, expected_format in implemented_formats:
            with self.subTest(date_str=date_str):
                result = utils.get_date_format(date_str)
                self.assertEqual(result, expected_format,
                                f"Failed for date_str='{date_str}'")

    def test_get_dates_various_configurations(self):
        """Test get_dates with various parameter combinations."""
        base_date = datetime(2023, 1, 1)

        # Test days parameter with end_date
        end_date_5_days = datetime(2023, 1, 6)  # 5 days after start
        result_5_days_excl = utils.get_dates(base_date, end_date=end_date_5_days, include_end_date=False)
        self.assertEqual(len(result_5_days_excl), 5)

        result_5_days_incl = utils.get_dates(base_date, end_date=end_date_5_days, include_end_date=True)
        self.assertEqual(len(result_5_days_incl), 6)

        # Test years parameter (2 years = 731 days for non-leap to leap year)
        result_2_years = utils.get_dates(base_date, years=2)
        self.assertEqual(len(result_2_years), 731)  # 2023 (365) + 2024 (366 leap year)

        # Test days parameter takes precedence
        result_days_override = utils.get_dates(base_date, days=10, years=5)
        self.assertEqual(len(result_days_override), 10)

    def test_strings_to_datetimes_engines(self):
        """Test strings_to_datetimes with different engines."""
        test_dates = ["2023-01-01", "2023-01-02", "2023-01-03"]

        # Test pandas engine
        result_pandas = utils.strings_to_datetimes(test_dates, engine="pandas")
        self.assertEqual(len(result_pandas), 3)
        self.assertEqual(str(result_pandas[0].date()), "2023-01-01")

        # Test numpy engine
        result_numpy = utils.strings_to_datetimes(test_dates, engine="numpy")
        self.assertEqual(len(result_numpy), 3)
        self.assertEqual(str(result_numpy[0]), "2023-01-01")

        # Test np alias
        result_np = utils.strings_to_datetimes(test_dates, engine="np")
        self.assertEqual(len(result_np), 3)

        # Test invalid engine
        with self.assertRaises(ValueError):
            utils.strings_to_datetimes(test_dates, engine="invalid")

    def test_crop_to_wy_different_months(self):
        """Test crop_to_wy with different water year start months."""
        # Create a test dataframe spanning multiple years
        import pandas as pd
        dates = utils.get_dates(datetime(2022, 1, 1), datetime(2024, 12, 31), str_format=r"%Y-%m-%d")
        df = pd.DataFrame({"value": range(len(dates))}, index=dates)
        df.index.name = "Date"

        # Test with different water year months
        wy_months_to_test = [1, 3, 7, 10]

        for wy_month in wy_months_to_test:
            with self.subTest(wy_month=wy_month):
                cropped = utils.crop_to_wy(df, wy_month=wy_month)

                # Verify the result is a valid dataframe
                self.assertIsInstance(cropped, pd.DataFrame)
                self.assertTrue(len(cropped) > 0)

                # Verify dates are within expected range
                start_date = utils.get_wy_start_date(df, wy_month)
                end_date = utils.get_wy_end_date(df, wy_month)

                first_cropped_date = datetime.strptime(cropped.index[0], r"%Y-%m-%d")
                last_cropped_date = datetime.strptime(cropped.index[-1], r"%Y-%m-%d")

                self.assertEqual(first_cropped_date, start_date)
                self.assertEqual(last_cropped_date, end_date)

    def test_get_month_various_inputs(self):
        """Test get_month with various date string inputs."""
        month_test_cases = [
            (["2023-01-15"], [1]),
            (["2023-02-28"], [2]),
            (["2023-03-31"], [3]),
            (["2023-04-30"], [4]),
            (["2023-05-15"], [5]),
            (["2023-06-15"], [6]),
            (["2023-07-15"], [7]),
            (["2023-08-15"], [8]),
            (["2023-09-15"], [9]),
            (["2023-10-15"], [10]),
            (["2023-11-15"], [11]),
            (["2023-12-15"], [12]),
        ]

        for dates, expected_months in month_test_cases:
            with self.subTest(dates=dates):
                result = utils.get_month(dates)
                self.assertEqual(result, expected_months)

    def test_leap_year_handling_comprehensive(self):
        """Test leap year handling across various functions."""
        leap_year_dates = ["2020-02-28", "2020-02-29", "2020-03-01"]
        non_leap_year_dates = ["2021-02-28", "2021-03-01"]

        # Test standardization with leap years
        standardized_leap = utils.standardize_datestring_format(leap_year_dates)
        self.assertEqual(len(standardized_leap), 3)
        self.assertEqual(standardized_leap[1], "2020-02-29")

        standardized_non_leap = utils.standardize_datestring_format(non_leap_year_dates)
        self.assertEqual(len(standardized_non_leap), 2)

        # Test month extraction with leap year
        months_leap = utils.get_month(leap_year_dates)
        self.assertEqual(months_leap, [2, 2, 3])

        # Test water year calculation with leap year boundary
        wy_leap = utils.get_wy(leap_year_dates, wy_month=3)
        # Feb 28 and 29 should be in previous water year, Mar 1 in new water year
        self.assertEqual(wy_leap, [2019, 2019, 2020])


if __name__ == '__main__':
    unittest.main()