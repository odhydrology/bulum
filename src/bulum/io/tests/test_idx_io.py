import os
import shutil
import unittest
from pathlib import Path

import bulum.io as bio


class Tests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        os.makedirs("./src/bulum/io/tests/test_outputs", exist_ok=True)
        return super().setUpClass()

    def test_read_idx(self):
        test_idx_filename = "./src/bulum/io/tests/da_file/BUR_FLWX.IDX"
        df = bio.read_idx(test_idx_filename)
        self.assertEqual(len(df), 41819)
        self.assertEqual(len(df.columns), 53)

    def test_write_area_ts_csv(self):
        test_output_filename = "./src/bulum/io/tests/test_outputs/test_data.area.csv"
        if os.path.isfile(test_output_filename):
            os.remove(test_output_filename)
        df = bio.read_ts_csv("./src/bulum/io/tests/test_data.csv")
        bio.write_area_ts_csv(df, test_output_filename)
        self.assertTrue(os.path.isfile(test_output_filename))
        self.assertGreater(os.path.getsize(test_output_filename), 0)

    def test_write_idx(self):
        test_output_filename = "./src/bulum/io/tests/test_outputs/test_data.idx"
        if os.path.isfile(test_output_filename):
            os.remove(test_output_filename)
        df = bio.read_ts_csv("./src/bulum/io/tests/test_data.csv")
        bio.write_idx(df, test_output_filename)
        self.assertTrue(os.path.isfile(test_output_filename))
        self.assertGreater(os.path.getsize(test_output_filename), 0)

    def test_write_idx_native(self):
        test_output_filename = "./src/bulum/io/tests/test_outputs/test_data.idx"
        if os.path.isfile(test_output_filename):
            os.remove(test_output_filename)
        df = bio.read_ts_csv("./src/bulum/io/tests/test_data.csv")
        bio.write_idx_native(df, test_output_filename)
        self.assertTrue(os.path.isfile(test_output_filename))
        self.assertGreater(os.path.getsize(test_output_filename), 0)

    def test_write_idx_native_2(self):
        test_output_filename = Path("./src/bulum/io/tests/test_outputs/test_data.idx")
        test_output_filename.unlink(missing_ok=True)
        df = bio.read_ts_csv("./src/bulum/io/tests/test_data.csv")
        bio.write_idx_native(df, test_output_filename)
        self.assertTrue(os.path.isfile(test_output_filename))
        self.assertGreater(os.path.getsize(test_output_filename), 0)

    def test_idx_native_roundtrip_from_csv(self):
        """Test that writing and reading back preserves data size (from CSV source)."""
        test_output_filename = "./src/bulum/io/tests/test_outputs/test_roundtrip.idx"
        test_output_out = "./src/bulum/io/tests/test_outputs/test_roundtrip.out"
        for f in [test_output_filename, test_output_out]:
            if os.path.isfile(f):
                os.remove(f)

        df_original = bio.read_ts_csv("./src/bulum/io/tests/test_data.csv")
        bio.write_idx_native(df_original, test_output_filename)
        df_roundtrip = bio.read_idx(test_output_filename)

        self.assertEqual(len(df_original), len(df_roundtrip),
                         "Row count changed after round-trip")
        self.assertEqual(len(df_original.columns), len(df_roundtrip.columns),
                         "Column count changed after round-trip")
        self.assertEqual(df_original.shape, df_roundtrip.shape,
                         "DataFrame shape changed after round-trip")

    def test_idx_native_roundtrip_from_idx(self):
        """Test that writing and reading back preserves data size (from IDX source)."""
        test_output_filename = "./src/bulum/io/tests/test_outputs/test_roundtrip2.idx"
        test_output_out = "./src/bulum/io/tests/test_outputs/test_roundtrip2.out"
        for f in [test_output_filename, test_output_out]:
            if os.path.isfile(f):
                os.remove(f)

        test_idx_filename = "./src/bulum/io/tests/da_file/BUR_FLWX.IDX"
        df_original = bio.read_idx(test_idx_filename)
        bio.write_idx_native(df_original, test_output_filename)
        df_roundtrip = bio.read_idx(test_output_filename)

        self.assertEqual(len(df_original), len(df_roundtrip),
                         "Row count changed after round-trip")
        self.assertEqual(len(df_original.columns), len(df_roundtrip.columns),
                         "Column count changed after round-trip")
        self.assertEqual(df_original.shape, df_roundtrip.shape,
                         "DataFrame shape changed after round-trip")

    @unittest.skipIf(shutil.which('csvidx') is None, "csvidx.exe not found on path")
    def test_idx_csvidx_roundtrip(self):
        """Test that writing and reading back preserves data size for write_idx (csvidx)."""
        test_output_filename = "./src/bulum/io/tests/test_outputs/test_csvidx_roundtrip.idx"
        test_output_out = "./src/bulum/io/tests/test_outputs/test_csvidx_roundtrip.out"
        test_temp_csv = "./src/bulum/io/tests/test_outputs/test_csvidx_roundtrip.tempfile.csv"
        for f in [test_output_filename, test_output_out, test_temp_csv]:
            if os.path.isfile(f):
                os.remove(f)

        test_idx_filename = "./src/bulum/io/tests/da_file/BUR_FLWX.IDX"
        df_original = bio.read_idx(test_idx_filename)
        bio.write_idx(df_original, test_output_filename)
        df_roundtrip = bio.read_idx(test_output_filename)

        self.assertEqual(len(df_original), len(df_roundtrip),
                         "Row count changed after round-trip")
        self.assertEqual(len(df_original.columns), len(df_roundtrip.columns),
                         "Column count changed after round-trip")
        self.assertEqual(df_original.shape, df_roundtrip.shape,
                         "DataFrame shape changed after round-trip")


if __name__ == "__main__":
    unittest.main()
