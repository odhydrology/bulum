import tempfile
import unittest
from pathlib import Path

import bulum.io as bio
import bulum.utils as out


class Tests(unittest.TestCase):

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp_dir = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

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
        tmp_path = self.tmp_dir / "test_ts_roundtrip.csv"
        df_original = bio.read_ts_csv("./src/bulum/io/tests/test_data.csv")
        bio.write_ts_csv(df_original, tmp_path)
        df_roundtrip = bio.read_ts_csv(tmp_path)
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
