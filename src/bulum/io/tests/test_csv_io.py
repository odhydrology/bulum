import os
import unittest

import bulum.io as bio
import bulum.utils as out


class Tests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        os.makedirs("./src/bulum/io/tests/test_outputs", exist_ok=True)
        return super().setUpClass()

    def test_read_ts_csv(self):
        df = bio.read_ts_csv("./src/bulum/io/tests/test_data.csv")
        self.assertEqual(len(df), 10)
        df_min_df = min(df.index)
        df_max_df = max(df.index)
        self.assertEqual(df_min_df[:4], "1889")
        self.assertEqual(df_max_df, "1889-01-10")

    def test_read_ts_csv2(self):
        df = bio.read_ts_csv("./src/bulum/io/tests/test_data2.csv")
        self.assertEqual(len(df), 10)
        df_min_df = min(df.index)
        df_max_df = max(df.index)
        self.assertEqual(df_min_df[:4], "1889")
        self.assertEqual(df_max_df, "1889-01-10")

    def test_ts_csv_roundtrip(self):
        """Test that writing and reading back preserves data size for ts_csv."""
        test_output_filename = "./src/bulum/io/tests/test_outputs/test_ts_roundtrip.csv"
        if os.path.isfile(test_output_filename):
            os.remove(test_output_filename)

        df_original = bio.read_ts_csv("./src/bulum/io/tests/test_data.csv")
        bio.write_ts_csv(df_original, test_output_filename)
        df_roundtrip = bio.read_ts_csv(test_output_filename)

        self.assertEqual(len(df_original), len(df_roundtrip),
                         "Row count changed after round-trip")
        self.assertEqual(len(df_original.columns), len(df_roundtrip.columns),
                         "Column count changed after round-trip")
        self.assertEqual(df_original.shape, df_roundtrip.shape,
                         "DataFrame shape changed after round-trip")

    def test_meets_ts_standards_5(self):
        df = bio.read_ts_csv("./src/bulum/utils/tests/test_data_missing.csv")
        violations = out.check_df_format_standards(df)
        self.assertEqual(violations, [])


if __name__ == "__main__":
    unittest.main()
