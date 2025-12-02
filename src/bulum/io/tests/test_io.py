import os
import re
import shutil
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

    def test_read_res_csv(self):
        df = bio.read_res_csv("./src/bulum/io/tests/res_csv_files/simple_model.res.csv")
        df_min_df = min(df.index)
        df_max_df = max(df.index)
        self.assertListEqual(df.columns.to_list(), ['1>Gauge 2>Downstream Flow', '2>Inflow 1>Downstream Flow'])
        self.assertEqual(len(df), 49155)
        self.assertEqual(df_min_df[:4], "1889")
        self.assertEqual(df_max_df, "2023-08-01")  # 2023-08-01

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
        test_output_filename = "./src/bulum/io/tests/test_outputs/test_out.res.csv"
        if os.path.isfile(test_output_filename):
            os.remove(test_output_filename)
        # read a dataframe and write to new format
        df = bio.read_res_csv("./src/bulum/io/tests/res_csv_files/simple_model.res.csv")
        bio.write_res_csv(df, test_output_filename)
        self.assertTrue(os.path.isfile(test_output_filename))
        self.assertGreater(os.path.getsize(test_output_filename), 0)

    def test_write_area_ts_csv(self):
        # delete test output if it already exists
        test_output_filename = "./src/bulum/io/tests/test_outputs/test_data.area.csv"
        if os.path.isfile(test_output_filename):
            os.remove(test_output_filename)
        # read a dataframe and write to new format
        df = bio.read_ts_csv("./src/bulum/io/tests/test_data.csv")
        bio.write_area_ts_csv(df, test_output_filename)
        self.assertTrue(os.path.isfile(test_output_filename))
        self.assertGreater(os.path.getsize(test_output_filename), 0)

    def test_read_idx(self):
        # start = timer()
        test_idx_filename = "./src/bulum/io/tests/da_file/BUR_FLWX.IDX"
        df = bio.read_idx(test_idx_filename)
        self.assertEqual(len(df), 41819)
        self.assertEqual(len(df.columns), 53)
        # end = timer()
        # print(f"read time = {(end - start)}") # Time in seconds, e.g. 5.38091952400282

    def test_write_idx(self):
        # start = timer()
        # delete test output if it already exists
        test_output_filename = "./src/bulum/io/tests/test_outputs/test_data.idx"
        if os.path.isfile(test_output_filename):
            os.remove(test_output_filename)
        # read a dataframe and write to new format
        df = bio.read_ts_csv("./src/bulum/io/tests/test_data.csv")
        bio.write_idx(df, test_output_filename)
        self.assertTrue(os.path.isfile(test_output_filename))
        self.assertGreater(os.path.getsize(test_output_filename), 0)
        # end = timer()
        # print(f"write time = {(end - start)}") # Time in seconds, e.g. 5.38091952400282

    def test_write_idx_native(self):
        # delete test output if it already exists
        test_output_filename = "./src/bulum/io/tests/test_outputs/test_data.idx"
        if os.path.isfile(test_output_filename):
            os.remove(test_output_filename)
        df = bio.read_ts_csv("./src/bulum/io/tests/test_data.csv")
        bio.write_idx_native(df, test_output_filename)
        self.assertTrue(os.path.isfile(test_output_filename))
        self.assertGreater(os.path.getsize(test_output_filename), 0)

    def test_idx_native_roundtrip_from_csv(self):
        """Test that writing and reading back preserves data size (from CSV source)."""
        # delete test output if it already exists
        test_output_filename = "./src/bulum/io/tests/test_outputs/test_roundtrip.idx"
        test_output_out = "./src/bulum/io/tests/test_outputs/test_roundtrip.out"
        for f in [test_output_filename, test_output_out]:
            if os.path.isfile(f):
                os.remove(f)

        # Read CSV, write as IDX, read back
        df_original = bio.read_ts_csv("./src/bulum/io/tests/test_data.csv")
        bio.write_idx_native(df_original, test_output_filename)
        df_roundtrip = bio.read_idx(test_output_filename)

        # Check dimensions are preserved
        self.assertEqual(len(df_original), len(df_roundtrip),
                         "Row count changed after round-trip")
        self.assertEqual(len(df_original.columns), len(df_roundtrip.columns),
                         "Column count changed after round-trip")
        self.assertEqual(df_original.shape, df_roundtrip.shape,
                         "DataFrame shape changed after round-trip")

    def test_idx_native_roundtrip_from_idx(self):
        """Test that writing and reading back preserves data size (from IDX source)."""
        # delete test output if it already exists
        test_output_filename = "./src/bulum/io/tests/test_outputs/test_roundtrip2.idx"
        test_output_out = "./src/bulum/io/tests/test_outputs/test_roundtrip2.out"
        for f in [test_output_filename, test_output_out]:
            if os.path.isfile(f):
                os.remove(f)

        # Read existing IDX, write it out, read back
        test_idx_filename = "./src/bulum/io/tests/da_file/BUR_FLWX.IDX"
        df_original = bio.read_idx(test_idx_filename)
        bio.write_idx_native(df_original, test_output_filename)
        df_roundtrip = bio.read_idx(test_output_filename)

        # Check dimensions are preserved
        self.assertEqual(len(df_original), len(df_roundtrip),
                         "Row count changed after round-trip")
        self.assertEqual(len(df_original.columns), len(df_roundtrip.columns),
                         "Column count changed after round-trip")
        self.assertEqual(df_original.shape, df_roundtrip.shape,
                         "DataFrame shape changed after round-trip")

    def test_ts_csv_roundtrip(self):
        """Test that writing and reading back preserves data size for ts_csv."""
        test_output_filename = "./src/bulum/io/tests/test_outputs/test_ts_roundtrip.csv"
        if os.path.isfile(test_output_filename):
            os.remove(test_output_filename)

        # Read CSV, write it out, read back
        df_original = bio.read_ts_csv("./src/bulum/io/tests/test_data.csv")
        bio.write_ts_csv(df_original, test_output_filename)
        df_roundtrip = bio.read_ts_csv(test_output_filename)

        # Check dimensions are preserved
        self.assertEqual(len(df_original), len(df_roundtrip),
                         "Row count changed after round-trip")
        self.assertEqual(len(df_original.columns), len(df_roundtrip.columns),
                         "Column count changed after round-trip")
        self.assertEqual(df_original.shape, df_roundtrip.shape,
                         "DataFrame shape changed after round-trip")

    def test_res_csv_roundtrip(self):
        """Test that writing and reading back preserves data size for res_csv."""
        test_output_filename = "./src/bulum/io/tests/test_outputs/test_res_roundtrip.res.csv"
        if os.path.isfile(test_output_filename):
            os.remove(test_output_filename)

        # Read RES CSV, write it out, read back
        df_original = bio.read_res_csv("./src/bulum/io/tests/res_csv_files/simple_model.res.csv")
        bio.write_res_csv(df_original, test_output_filename)
        df_roundtrip = bio.read_res_csv(test_output_filename)

        # Check dimensions are preserved
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

        # Read existing IDX, write using csvidx, read back
        test_idx_filename = "./src/bulum/io/tests/da_file/BUR_FLWX.IDX"
        df_original = bio.read_idx(test_idx_filename)
        bio.write_idx(df_original, test_output_filename)
        df_roundtrip = bio.read_idx(test_output_filename)

        # Check dimensions are preserved
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
        # Verify the dataframe was created successfully
        self.assertEqual(len(df), 10)
        # Verify dates are in string format and in standard YYYY-MM-DD format
        self.assertIsInstance(df.index[0], str)
        self.assertEqual(len(df.index[0]), 10)  # YYYY-MM-DD format
        # Verify we can handle very early dates (year 0001)
        self.assertEqual(df.index[0], "0001-01-01")
        self.assertEqual(df.index[-1], "0001-01-10")

    def test_read_iqqm_lqn_output_date_format_regression(self):
        """Regression test: Ensure lqn files produce clean YYYY-MM-DD format without timestamps.

        This test prevents regression where numpy datetime64 string conversion
        produced timestamps like "2000-01-01T00:00:00.000000" instead of "2000-01-01".
        """
        df = bio.read_iqqm_lqn_output("./src/bulum/io/tests/M_L1#030.01d")
        # Verify dates are strings
        self.assertIsInstance(df.index[0], str)
        # Verify no timestamp component (no 'T' character)
        for date_str in df.index[:100]:  # Check first 100 dates
            self.assertNotIn('T', date_str, f"Date {date_str} contains timestamp component")
            # Verify exact YYYY-MM-DD format (10 characters)
            self.assertEqual(len(date_str), 10, f"Date {date_str} is not in YYYY-MM-DD format")
        # Verify format matches expected pattern
        date_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}$')
        for date_str in df.index[:100]:
            self.assertRegex(date_str, date_pattern, f"Date {date_str} doesn't match YYYY-MM-DD pattern")

    # def test_iqqm_out_reader(self):
    #     reader = oio.iqqm_out_reader("./src/bulum/io/tests/iqqm_results/O02l.IQN") #O02l.IQN
    #     reader.require(node="030", output="01") #node_number, rec_number.
    #     reader.require(node="065", output="01") #node_number, rec_number.
    #     df = reader.read()
    #     self.assertAlmostEqual(len(df.columns), 2)
    #     self.assertAlmostEqual(len(df), 43464)
    #     #print(df.head())
    #     self.assertAlmostEqual(df["030_01.d"].sum(), 30598077.0495)

    # def test_iqqm_out_reader2(self):
    #     reader = oio.iqqm_out_reader("./src/bulum/io/tests/iqqm_results/O02l.IQN") #O02l.IQN
    #     df = reader.read(read_all_availabe=True)
    #     #print(df.head())
    #     self.assertAlmostEqual(df["030_01.d"].sum(), 30598077.0495)

    def test_iqqm_out_reader3(self):
        reader = bio.iqqm_out_reader("./src/bulum/io/tests/iqqm_results/O02l.IQN")
        reader.require(type=3.1, output=2)
        reader.require(supertype=8, output=2)
        reader.require(node=30, output=1)
        df = reader.read()
        self.assertEqual(len(df.columns), 14)
        self.assertEqual(len(df), 43464)
        self.assertAlmostEqual(df["020_02.d"].sum(), 561119.5652, delta=5)
        self.assertAlmostEqual(df["030_01.d"].sum(), 30598077.0495, delta=30)

    def test_iqqm_out_reader3_py(self):
        """As above, with python engine"""
        reader = bio.IqqmOutReader("./src/bulum/io/tests/iqqm_results/O02l.IQN")
        reader.require(type=3.1, output=2)
        reader.require(supertype=8, output=2)
        reader.require(node=30, output=1)
        df = reader.read(engine="python")
        self.assertEqual(len(df.columns), 14)
        self.assertEqual(len(df), 43464)
        self.assertAlmostEqual(df["020_02.d"].sum(), 561119.5652, delta=5)
        self.assertAlmostEqual(df["030_01.d"].sum(), 30598077.0495, delta=30)


if __name__ == "__main__":
    unittest.main()
