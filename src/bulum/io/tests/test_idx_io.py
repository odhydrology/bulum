import shutil
import tempfile
import unittest
from pathlib import Path

import pandas as pd

import bulum.io as bio


class Tests(unittest.TestCase):

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp_dir = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def test_read_idx(self):
        df = bio.read_idx("./src/bulum/io/tests/da_file/BUR_FLWX.IDX")
        self.assertEqual(len(df), 41819)
        self.assertEqual(len(df.columns), 53)

    def test_read_idx_file_not_found(self):
        with self.assertRaises(FileNotFoundError):
            bio.read_idx(self.tmp_dir / "nonexistent.idx")

    def test_write_area_ts_csv(self):
        tmp_path = self.tmp_dir / "test_data.area.csv"
        df = bio.read_ts_csv("./src/bulum/io/tests/test_data.csv")
        bio.write_area_ts_csv(df, tmp_path)
        self.assertTrue(tmp_path.is_file())
        self.assertGreater(tmp_path.stat().st_size, 0)

    def test_write_area_ts_csv_column_clash(self):
        """Columns that are identical when truncated to 12 chars should raise ValueError."""
        dates = pd.Index(["2000-01-01", "2000-01-02", "2000-01-03"], name="Date")
        df = pd.DataFrame({"AAAAAAAAAAAAX": [1.0, 2.0, 3.0],
                           "AAAAAAAAAAAAY": [4.0, 5.0, 6.0]}, index=dates)
        with self.assertRaises(ValueError):
            bio.write_area_ts_csv(df, self.tmp_dir / "clash.csv")

    def test_write_idx(self):
        tmp_path = self.tmp_dir / "test_data.idx"
        df = bio.read_ts_csv("./src/bulum/io/tests/test_data.csv")
        bio.write_idx(df, tmp_path)
        self.assertTrue(tmp_path.is_file())
        self.assertGreater(tmp_path.stat().st_size, 0)

    def test_write_idx_exist_ok_false(self):
        """write_idx with exist_ok=False raises FileExistsError if file already exists."""
        existing = self.tmp_dir / "existing.idx"
        existing.touch()
        df = bio.read_ts_csv("./src/bulum/io/tests/test_data.csv")
        with self.assertRaises(FileExistsError):
            bio.write_idx(df, existing, exist_ok=False)

    def test_write_idx_native(self):
        df = bio.read_ts_csv("./src/bulum/io/tests/test_data.csv")
        for input_type, path in [
            ("str", str(self.tmp_dir / "test_str.idx")),
            ("Path", self.tmp_dir / "test_path.idx"),
        ]:
            with self.subTest(input_type=input_type):
                bio.write_idx_native(df, path)
                self.assertTrue(Path(path).is_file())
                self.assertGreater(Path(path).stat().st_size, 0)

    def test_idx_native_roundtrip_from_csv(self):
        """Test that writing and reading back preserves data size (from CSV source)."""
        tmp_path = self.tmp_dir / "test_roundtrip.idx"
        df_original = bio.read_ts_csv("./src/bulum/io/tests/test_data.csv")
        bio.write_idx_native(df_original, tmp_path)
        df_roundtrip = bio.read_idx(tmp_path)
        self.assertEqual(len(df_original), len(df_roundtrip),
                         "Row count changed after round-trip")
        self.assertEqual(len(df_original.columns), len(df_roundtrip.columns),
                         "Column count changed after round-trip")
        self.assertEqual(df_original.shape, df_roundtrip.shape,
                         "DataFrame shape changed after round-trip")

    def test_idx_native_roundtrip_from_idx(self):
        """Test that writing and reading back preserves data size (from IDX source)."""
        tmp_path = self.tmp_dir / "test_roundtrip2.idx"
        df_original = bio.read_idx("./src/bulum/io/tests/da_file/BUR_FLWX.IDX")
        bio.write_idx_native(df_original, tmp_path)
        df_roundtrip = bio.read_idx(tmp_path)
        self.assertEqual(len(df_original), len(df_roundtrip),
                         "Row count changed after round-trip")
        self.assertEqual(len(df_original.columns), len(df_roundtrip.columns),
                         "Column count changed after round-trip")
        self.assertEqual(df_original.shape, df_roundtrip.shape,
                         "DataFrame shape changed after round-trip")

    @unittest.skipIf(shutil.which('csvidx') is None, "csvidx.exe not found on path")
    def test_idx_csvidx_roundtrip(self):
        """Test that writing and reading back preserves data size for write_idx (csvidx)."""
        tmp_path = self.tmp_dir / "test_csvidx_roundtrip.idx"
        df_original = bio.read_idx("./src/bulum/io/tests/da_file/BUR_FLWX.IDX")
        bio.write_idx(df_original, tmp_path)
        df_roundtrip = bio.read_idx(tmp_path)
        self.assertEqual(len(df_original), len(df_roundtrip),
                         "Row count changed after round-trip")
        self.assertEqual(len(df_original.columns), len(df_roundtrip.columns),
                         "Column count changed after round-trip")
        self.assertEqual(df_original.shape, df_roundtrip.shape,
                         "DataFrame shape changed after round-trip")


if __name__ == "__main__":
    unittest.main()
