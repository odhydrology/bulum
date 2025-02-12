import unittest
import os
import bulum.io as oio
import bulum.clim as ocl
import bulum.utils as out
import pandas as pd

class Tests(unittest.TestCase):

    def test_load_data_into_df(self):
        """
        The purpose of this is just to test that if I load 2 different files as 
        dataframes, and then simply put them together. I.e. does it join on the
        dates? YES IT APPEARS TO LEFT-JOIN on the dates from the first file.
        """
        df1 = oio.read_ts_csv("./src/bulum/clim/tests/Flow.out.withmissing.csv", date_format=r"%Y-%m-%d")["FRex_FR.qqm"]
        df2 = oio.read_ts_csv("./src/bulum/clim/tests/Flow.out.RCP85_P2050_GCM8.csv", date_format=r"%Y-%m-%d")["FRex_FR.qqm"]
        df3 = pd.DataFrame()
        df3["x"] = df1
        df3["y"] = df2
        # print(df3.head())
        # print(df3.tail())
        self.assertTrue(len(df3) == len(df1))
        self.assertAlmostEqual(df3.x.values[-1], 136) #these values should line up despite different starting dates for each file
        self.assertAlmostEqual(df3.y.values[-1], 62) #these values should line up despite different starting dates for each file
        # And now doe the same again with files loaded in the opposite order
        df1 = oio.read_ts_csv("./src/bulum/clim/tests/Flow.out.RCP85_P2050_GCM8.csv", date_format=r"%Y-%m-%d")["FRex_FR.qqm"]
        df2 = oio.read_ts_csv("./src/bulum/clim/tests/Flow.out.withmissing.csv", date_format=r"%Y-%m-%d")["FRex_FR.qqm"]
        df3 = pd.DataFrame()
        df3["x"] = df1
        df3["y"] = df2
        # print(df3.head())
        # print(df3.tail())
        self.assertTrue(len(df3) == len(df1))
        self.assertAlmostEqual(df3.x.values[-1], 62) #these values should line up despite different starting dates for each file
        self.assertAlmostEqual(df3.y.values[-1], 136) #these values should line up despite different starting dates for each file

    def test_get_exc_transform(self):
        """
        This gets transformation curves based on current & future climate datasets, and then checks that the transformation
        curves have the same total number of points regardless of whether we use default seasons (monthly) or define 3 custom 
        seasons.
        """
        current_climate = oio.read_ts_csv("./src/bulum/clim/tests/Flow.out.withmissing.csv", date_format=r"%Y-%m-%d")
        future_climate = oio.read_ts_csv("./src/bulum/clim/tests/Flow.out.RCP85_P2050_GCM8.csv", date_format=r"%Y-%m-%d")
        answer1 = ocl.derive_transformation_curves(current_climate["FRex_FR.qqm"], future_climate["FRex_FR.qqm"])
        answer2 = ocl.derive_transformation_curves(current_climate["FRex_FR.qqm"], future_climate["FRex_FR.qqm"], season_start_months=[3,7,9])
        total_len_1 = sum([len(a[1]) for a in answer1.values()])
        total_len_2 = sum([len(a[1]) for a in answer2.values()])
        self.assertEqual(total_len_1, total_len_2)
        self.assertTrue(total_len_1 > 0)

    def test_apply_transformation_curves(self):
        current_climate = oio.read_ts_csv("./src/bulum/clim/tests/Flow.out.withmissing.csv", date_format=r"%Y-%m-%d")
        future_climate = oio.read_ts_csv("./src/bulum/clim/tests/Flow.out.RCP85_P2050_GCM8.csv", date_format=r"%Y-%m-%d")
        t = ocl.derive_transformation_curves(current_climate["FRex_FR.qqm"], future_climate["FRex_FR.qqm"])
        answer = pd.DataFrame()
        answer["RexCurrent"] = current_climate["FRex_FR.qqm"]
        answer["RexCurrent"] = future_climate["FRex_FR.qqm"]
        answer["Transf"] = ocl.apply_transformation_curves(t, current_climate["FRex_FR.qqm"])
        # print(answer["Current"].sum())
        # print(answer["Future"].sum())
        # print(answer["Transf"].sum())
        self.assertAlmostEqual(answer["Transf"].sum(), 4845710.795475168) # Actually haven't checked that this is the correct answer but looks plausible.
        answer.to_csv("./trash/transformed.csv")
        


