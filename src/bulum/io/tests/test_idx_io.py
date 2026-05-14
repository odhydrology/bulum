import shutil
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

    def test_read_idx(self):
        test_idx_filename = "./src/bulum/io/tests/da_file/BUR_FLWX.IDX"
        df = bio.read_idx(test_idx_filename)
        self.assertEqual(len(df), 41819)
        self.assertEqual(len(df.columns), 53)

    def test_write_area_ts_csv(self):
        tmp_path = self.tmp_dir / "test_data.area.csv"
        df = bio.read_ts_csv("./src/bulum/io/tests/test_data.csv")
        bio.write_area_ts_csv(df, tmp_path)
        self.assertTrue(tmp_path.is_file())
        self.assertGreater(tmp_path.stat().st_size, 0)

    def test_write_idx(self):
        tmp_path = self.tmp_dir / "test_data.idx"
        df = bio.read_ts_csv("./src/bulum/io/tests/test_data.csv")
        bio.write_idx(df, tmp_path)
        self.assertTrue(tmp_path.is_file())
        self.assertGreater(tmp_path.stat().st_size, 0)

    def test_write_idx_native(self):
        tmp_path = self.tmp_dir / "test_data.idx"
        df = bio.read_ts_csv("./src/bulum/io/tests/test_data.csv")
        bio.write_idx_native(df, tmp_path)
        self.assertTrue(tmp_path.is_file())
        self.assertGreater(tmp_path.stat().st_size, 0)

    def test_write_idx_native_2(self):
        """Test that write_idx_native accepts a Path object."""
        tmp_path = self.tmp_dir / "test_data.idx"
        df = bio.read_ts_csv("./src/bulum/io/tests/test_data.csv")
        bio.write_idx_native(df, tmp_path)
        self.assertTrue(tmp_path.is_file())
        self.assertGreater(tmp_path.stat().st_size, 0)

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
        test_idx_filename = "./src/bulum/io/tests/da_file/BUR_FLWX.IDX"
        df_original = bio.read_idx(test_idx_filename)
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
        test_idx_filename = "./src/bulum/io/tests/da_file/BUR_FLWX.IDX"
        df_original = bio.read_idx(test_idx_filename)
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
