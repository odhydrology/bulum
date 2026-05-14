import tempfile
import unittest
from pathlib import Path

import bulum.io as bio


class Tests(unittest.TestCase):

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp_dir = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def test_read_res_csv(self):
        df = bio.read_res_csv("./src/bulum/io/tests/res_csv_files/simple_model.res.csv")
        df_min_df = min(df.index)
        df_max_df = max(df.index)
        self.assertListEqual(df.columns.to_list(), ['1>Gauge 2>Downstream Flow', '2>Inflow 1>Downstream Flow'])
        self.assertEqual(len(df), 49155)
        self.assertEqual(df_min_df[:4], "1889")
        self.assertEqual(df_max_df, "2023-08-01")

    def test_read_res_csv_with_missing_values(self):
        df = bio.read_res_csv("./src/bulum/io/tests/res_csv_files/file_with_missing_vals.res.csv")
        self.assertEqual(df.isnull().sum().sum(), 5)

    def test_read_res_csv_with_missing_values2(self):
        df = bio.read_res_csv("./src/bulum/io/tests/res_csv_files/file_with_missing_vals.res.csv", custom_na_values=['100.00000000000001'])
        self.assertEqual(df.isnull().sum().sum(), 17)

    def test_read_res_csv_date_handling(self):
        """Timeseries saved by excel."""
        bio.read_res_csv("./src/bulum/io/tests/res_csv_files/file_with_bad_dates.res.csv")

    def test_write_res_csv(self):
        tmp_path = self.tmp_dir / "test_out.res.csv"
        df = bio.read_res_csv("./src/bulum/io/tests/res_csv_files/simple_model.res.csv")
        bio.write_res_csv(df, tmp_path)
        self.assertTrue(tmp_path.is_file())
        self.assertGreater(tmp_path.stat().st_size, 0)

    def test_res_csv_roundtrip(self):
        """Test that writing and reading back preserves data size for res_csv."""
        tmp_path = self.tmp_dir / "test_res_roundtrip.res.csv"
        df_original = bio.read_res_csv("./src/bulum/io/tests/res_csv_files/simple_model.res.csv")
        bio.write_res_csv(df_original, tmp_path)
        df_roundtrip = bio.read_res_csv(tmp_path)
        self.assertEqual(len(df_original), len(df_roundtrip),
                         "Row count changed after round-trip")
        self.assertEqual(len(df_original.columns), len(df_roundtrip.columns),
                         "Column count changed after round-trip")
        self.assertEqual(df_original.shape, df_roundtrip.shape,
                         "DataFrame shape changed after round-trip")


if __name__ == "__main__":
    unittest.main()
