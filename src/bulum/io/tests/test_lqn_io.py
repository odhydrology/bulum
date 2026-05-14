import re
import unittest

import bulum.io as bio


class Tests(unittest.TestCase):

    def test_read_iqqm_lqn_output(self):
        df = bio.read_iqqm_lqn_output("./src/bulum/io/tests/M_L1#030.01d")
        df = bio.read_iqqm_lqn_output("./src/bulum/io/tests/M_L1#065.01d", df=df)
        df = bio.read_iqqm_lqn_output("./src/bulum/io/tests/M_L1#030.01d", df=df, col_name="three")
        self.assertAlmostEqual(df["M_L1#030.01d"].sum(), 19922893.66192)
        self.assertAlmostEqual(df["M_L1#065.01d"].sum(), 53179857.30745)
        self.assertAlmostEqual(df["three"].sum(), 19922893.66192)

    def test_read_iqqm_lqn_output_stochastic_dates(self):
        """Test reading IQQM lqn output with stochastic dates outside numpy datetime64 range.

        Stochastic model outputs can have dates from year 0001 to 9999, which is outside
        the range that numpy datetime64 can handle (approximately 1677-2262).
        This test ensures dates are kept as strings to support the full range.
        """
        df = bio.read_iqqm_lqn_output("./src/bulum/io/tests/stochastic_test.lqn")
        self.assertEqual(len(df), 10)
        self.assertIsInstance(df.index[0], str)
        self.assertEqual(len(df.index[0]), 10)  # YYYY-MM-DD format
        self.assertEqual(df.index[0], "0001-01-01")
        self.assertEqual(df.index[-1], "0001-01-10")

    def test_read_iqqm_lqn_output_date_format_regression(self):
        """Regression test: Ensure lqn files produce clean YYYY-MM-DD format without timestamps.

        This test prevents regression where numpy datetime64 string conversion
        produced timestamps like "2000-01-01T00:00:00.000000" instead of "2000-01-01".
        """
        df = bio.read_iqqm_lqn_output("./src/bulum/io/tests/M_L1#030.01d")
        self.assertIsInstance(df.index[0], str)
        for date_str in df.index[:100]:  # Check first 100 dates
            self.assertNotIn('T', date_str, f"Date {date_str} contains timestamp component")
            self.assertEqual(len(date_str), 10, f"Date {date_str} is not in YYYY-MM-DD format")
        date_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}$')
        for date_str in df.index[:100]:
            self.assertRegex(date_str, date_pattern, f"Date {date_str} doesn't match YYYY-MM-DD pattern")


if __name__ == "__main__":
    unittest.main()
