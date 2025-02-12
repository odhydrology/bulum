import pandas as pd
import numpy as np
import unittest
from bulum import utils
from bulum import io
import bulum.stats.swflo2s as swflo2s

class TestSwflo2s(unittest.TestCase):
            
    def test_days_in_no_flow_periods(self):
        df = io.read_ts_csv("./src/bulum/stats/swflo2s/tests/s00_etl.csv")
        ans = {}    

        node = utils.find_col(df, "125002C").name #1/9
        temp_df = df[[node]]
        ans[node] = {}
        ans[node]["EFO2 No flow"] = swflo2s.days_in_no_flow_periods(temp_df, flow_threshold=2, duration_days=60, as_percentage=True)["Value"]
        self.assertAlmostEqual(ans[node]["EFO2 No flow"], 4.909571566447065)

        node = utils.find_col(df, "125004B").name #2/9
        temp_df = df[[node]]
        ans[node] = {}
        ans[node]["EFO2 No flow"] = swflo2s.days_in_no_flow_periods(temp_df, flow_threshold=2, duration_days=30, as_percentage=True)["Value"]
        self.assertAlmostEqual(ans[node]["EFO2 No flow"], 0.32310855608)

        node = utils.find_col(df, "125014A").name #7/9
        temp_df = df[[node]]
        ans[node] = {}
        ans[node]["EFO2 No flow"] = swflo2s.days_in_no_flow_periods(temp_df, flow_threshold=2, duration_days=180, as_percentage=True)["Value"]
        self.assertAlmostEqual(ans[node]["EFO2 No flow"], 0.40283664134950276)

        node = utils.find_col(df, "125016A").name #8/9
        temp_df = df[[node]]
        ans[node] = {}
        ans[node]["EFO2 No flow"] = swflo2s.days_in_no_flow_periods(temp_df, flow_threshold=10, duration_days=30, as_percentage=True)["Value"]
        self.assertAlmostEqual(ans[node]["EFO2 No flow"], 3.1471612605429904)


    def test_days_in_riffle_periods(self):
        df = io.read_ts_csv("./src/bulum/stats/swflo2s/tests/s00_etl.csv")
        ans = {}    

        node = utils.find_col(df, "125004B").name #2/9
        temp_df = df[[node]]
        ans[node] = {}
        ans[node]["EFO3 Riffles"] = swflo2s.days_in_riffle_drown_out_periods(temp_df, 1498, as_percentage=True)["Value"]
        self.assertAlmostEqual(ans[node]["EFO3 Riffles"], 10.417103772397297)

        node = utils.find_col(df, "125005A").name #3/9
        temp_df = df[[node]]
        ans[node] = {}
        ans[node]["EFO3 Riffles"] = swflo2s.days_in_riffle_drown_out_periods(temp_df, 1120, as_percentage=True)["Value"]
        self.assertAlmostEqual(ans[node]["EFO3 Riffles"], 7.29721790944568)



if __name__ == '__main__':
    unittest.main()
