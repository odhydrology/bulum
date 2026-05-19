"""Tests for the negflo class."""
# pylint: skip-file

import logging
import unittest
from datetime import datetime

import numpy as np
import pandas as pd

from bulum.stats import Negflo

logging.getLogger().setLevel(logging.CRITICAL)  # ignores warnings for carried negative flow

# TODO write tests with larger/randomised dataframes/series/data sets


class Tests(unittest.TestCase):

    def test_cl1(self):
        """Test CL1 - clipping."""
        df = pd.DataFrame({
            "a": [-1.0, 1.0],
            "b": [-5.0, -3.0]
        })
        negflo = Negflo(df, 0)
        negflo.cl1()
        self.assertEqual(1, np.count_nonzero(negflo.df_residual))

    def test_sm1(self):
        """Test SM1 - global smoothing."""
        df = pd.DataFrame({
            "a": [-1.0, 1.0],
            "b": [0.0, 4.0]
        })
        negflo = Negflo(df, 0)
        negflo.sm1()
        self.assertTrue(all(0 == negflo.df_residual["a"]))
        self.assertTrue(all(df["b"] == negflo.df_residual["b"]))

    def test__sm2_3_helper(self):
        """Test the smoothing algorithm for SM2"""
        negflo = Negflo(pd.DataFrame(), 0)
        self.assertTrue(all(0 == negflo._sm_forward_series(pd.Series([-1, 1]))))

    def test_sm2(self):
        """Tests to make sure ordering is correct i.e. smooths forward not backward."""
        df = pd.DataFrame({
            "a": [1.0, -1.0],
            "b": [-1.0, 1.0]
        })
        negflo = Negflo(df, 0)
        negflo.sm2()
        self.assertEqual(1, np.count_nonzero(negflo.df_residual["a"]))
        self.assertEqual(0, np.count_nonzero(negflo.df_residual["b"]))

    def test_sm2_2(self):
        """Tests both types of smoothing"""
        df = pd.DataFrame({
            "a": [-4.0, 1.0, 1.0, -1.0, 8.0, 0.0],
        })
        negflo = Negflo(df, 0)
        negflo.sm2()
        s = negflo.df_residual["a"]
        self.assertEqual(1, np.count_nonzero(s))
        self.assertEqual(5, np.count_nonzero(s == 0))
        self.assertEqual(5, s[len(s) - 2])

    def test_sm2_3(self):
        """SM2 non-zero flow limit"""
        df = pd.DataFrame({
            "a": [-4., 1., 1., -1., 8., 0.],  # tests basic function
            "b": [-10.0, 8.0, 6.0, 2.0, 4.0, 10.0],  # tests whether flow limit is preserved
        })
        negflo = Negflo(df, 2.0)
        negflo.sm2()
        s1 = negflo.df_residual["a"]
        self.assertEqual(3, np.count_nonzero(s1))
        self.assertEqual(3, np.count_nonzero(s1 == 0))
        self.assertEqual(3, s1[len(s1) - 2])
        self.assertEqual(sum(s1), sum(negflo.df_residual["a"]))

        s2 = negflo.df_residual["b"]
        # The 2.0 at index 3 is exactly at flow_limit, so it ends the positive period.
        # [-10] distributes into [8, 6, 2] (sum_above_lim=10, rf=0); [4, 10] untouched.
        self.assertTrue(all(pd.Series([0.0, 2.0, 2.0, 2.0, 4.0, 10.0]) == s2))

    def test_sm3(self):
        """SM3 general test"""
        df = pd.DataFrame({
            "a": [-4.0, 1.0, 1.0, -1.0, 8.0, 0.0],
        })
        negflo = Negflo(df, 1)
        negflo.sm3()
        s = negflo.df_residual["a"]
        self.assertEqual(3, np.count_nonzero(s))
        self.assertEqual(3, np.count_nonzero(s == 0))
        self.assertEqual(7, s[len(s) - 2])

    def test_sm4(self):
        """Tests to make sure ordering is correct i.e. smooths backward not
        forward."""
        df = pd.DataFrame({
            "a": [1, -1],
            "b": [-1, 1]
        })
        negflo = Negflo(df, 0)
        negflo.sm4()
        self.assertEqual(0, negflo.neg_overflows["a"])
        self.assertEqual(0, np.count_nonzero(negflo.df_residual["a"]))

        self.assertEqual(-1, negflo.neg_overflows["b"])
        self.assertEqual(1, np.count_nonzero(negflo.df_residual["b"]))

    def test_sm4_carry(self):
        """Tests to make sure ordering is correct i.e. smooths backward not
        forward."""
        df = pd.DataFrame({
            "a": [-1.0, 0.0, 3.0, -2.0, 4.0, -1.0],
        })
        expect = pd.Series([0.0, 0.0, 2.0, 0.0, 2.0, 0.0])
        negflo = Negflo(df, 2)
        negflo.sm4()
        self.assertTrue(all(expect == negflo.df_residual["a"]))

    def test_sm5_carry(self):
        """Tests to make sure ordering is correct i.e. smooths backward not
        forward."""
        df = pd.DataFrame({
            "a": [-1, 0, 3, -2, 4, -1],
        })
        expect = pd.Series([0.0, 0.0, 2.0, 0.0, 3.0, 0.0])
        negflo = Negflo(df, 2)
        negflo.sm5()
        self.assertEqual(0, negflo.neg_overflows["a"])
        self.assertTrue(all(expect == negflo.df_residual["a"]))

    def test_sm6_period_specification(self):
        """Period specification working?"""
        df = pd.DataFrame({"a":
                           [-2, 2, 2, 0, 1, 1, -2, -2, 0, -1, 1]})
        expected = pd.DataFrame({"a": [0, 1, 1, 0, 1, 1, 0, 0, 0, -1, 1]})

        def read_date(s):
            return datetime.strptime(s, r"%d %m %y")
        start_date = read_date("1 1 00")
        expected.index = df.index = pd.date_range(start_date, periods=11)
        segments = [
            (read_date("1 1 00"), read_date("3 1 00")),
            (read_date("4 1 00"), read_date("6 1 00")),
            (read_date("7 1 00"), read_date("9 1 00")),
        ]
        negflo = Negflo(df, segments=segments)
        negflo.sm6()
        self.assertTrue(all(negflo.df_residual["a"] == expected["a"]))
        self.assertEqual(negflo.neg_overflows["a"], -4)

    def test_sm6_sampling(self):
        """Sampling working as expected?"""
        df = pd.DataFrame({"a":
                           [-2, 2, 2, 0, 1, 1, -2, -2, 0, -1, 1]})
        expected = pd.DataFrame({"a": [0, 1, 1, 0, 1, 1, 0, 0, 0, 0, 0]})

        def read_date(s):
            return datetime.strptime(s, r"%d %m %y")
        start_date = read_date("1 1 00")
        expected.index = df.index = pd.date_range(start_date, periods=11)
        negflo = Negflo(df)
        negflo.sm6(sampling_frequency=pd.DateOffset(days=3))
        self.assertTrue(all(negflo.df_residual["a"] == expected["a"]))
        self.assertEqual(negflo.neg_overflows["a"], -4)

    def test_sm6_start_date(self):
        """SM6 start date parameter working?"""
        df = pd.DataFrame({"a":
                           [-2, 2, 2, 0, 1, 1, -2, -2, 0, -1, 1]})
        expected = pd.DataFrame({"a": [-2, 1, 1, 0, 1, 1, 0, 0, 0, -1, 1]})

        def read_date(s):
            return datetime.strptime(s, r"%d %m %y")
        idx_start_date = read_date("1 1 00")
        expected.index = df.index = pd.date_range(idx_start_date, periods=11)
        sample_start_date = read_date("3 1 00")
        negflo = Negflo(df)
        negflo.sm6(sampling_start_date=sample_start_date)
        self.assertEqual(negflo.df_residual["a"].iloc[0], -2,
                         f"Got\n{negflo.df_residual}\nExpected\n{expected}")

    def test_sm7(self):
        """Chooses the larger flow period?"""
        df = pd.DataFrame({
            "a": [2, -1, 1],
        })
        expect = pd.Series([1, 0, 1])
        negflo = Negflo(df)
        negflo.sm7()
        self.assertTrue(all(expect == negflo.df_residual["a"]))

    def test_sm7_unidir(self):
        """Works on the boundaries?"""
        df = pd.DataFrame({
            "a": [2, -1],
            "b": [-1, 2],
        })
        negflo = Negflo(df)
        negflo.sm7()
        self.assertEqual(1, negflo.df_residual["a"][0])
        self.assertEqual(0, negflo.df_residual["a"][1])

        self.assertEqual(1, negflo.df_residual["b"][1])
        self.assertEqual(0, negflo.df_residual["b"][0])

    def test_sm7_flow_lim(self):
        """Respects flow limits?"""
        df = pd.DataFrame({
            "a": [4, -2, 2, 2],
        })
        expect = pd.DataFrame({
            "a": [2, 0, 2, 2]
        })
        negflo = Negflo(df, 2)
        negflo.sm7()
        self.assertEqual(0, negflo.neg_overflows["a"])
        self.assertTrue(all(expect["a"] == negflo.df_residual["a"]))

    def _make_string_indexed_df(self, data, start="1983-05-24"):
        dates = pd.date_range(start=start, periods=len(data), freq="D").strftime("%Y-%m-%d").tolist()
        return pd.DataFrame({"flow": data}, index=dates)

    def test_sm2_string_index_no_extra_rows(self):
        """sm2 with a string date index must not create extra integer-keyed rows."""
        df = self._make_string_indexed_df([280, 40, 100, 49, 27, -50, 10])
        negflo = Negflo(df, flow_limit=0)
        negflo.sm2()
        self.assertEqual(len(df), len(negflo.df_residual))

    def test_sm2_string_index_neg_zeroed(self):
        """sm2 with a string date index must zero negatives and carry forward correctly.

        No negative precedes the first positive period, so it must remain unchanged.
        The negative at index 5 carries forward into index 6 (the next positive period),
        fully consuming it.
        """
        orig = [100.0, 5.0, 10.0, 50.0, 30.0, -70.0, 10.0, -150.0, -200.0]
        df = self._make_string_indexed_df(orig)
        negflo = Negflo(df, flow_limit=0)
        negflo.sm2()
        result = negflo.df_residual["flow"]
        # Preceding positive period is unchanged (no negative before it)
        for i in range(5):
            with self.subTest(i=i):
                self.assertEqual(result.iloc[i], orig[i])
        # Negatives and following positive period zeroed by carry-forward
        self.assertEqual(result.iloc[5], 0.0)   # -70 zeroed
        self.assertEqual(result.iloc[6], 0.0)   # 10 consumed by carried -70
        self.assertEqual(result.iloc[7], 0.0)   # -150 zeroed
        self.assertEqual(result.iloc[8], 0.0)   # -200 zeroed

    def test_sm3_string_index_neg_zeroed(self):
        """sm3 with a string date index must zero negatives at the correct rows."""
        df = self._make_string_indexed_df([100.0, 5.0, 10.0, 50.0, 30.0, -70.0, 10.0, -150.0, -200.0])
        negflo = Negflo(df, flow_limit=0)
        negflo.sm3()
        result = negflo.df_residual["flow"]
        self.assertEqual(result.iloc[5], 0.0)   # -70 zeroed
        self.assertEqual(result.iloc[6], 0.0)   # 10 consumed by carried -70
        self.assertEqual(result.iloc[7], 0.0)   # -150 zeroed
        self.assertEqual(result.iloc[8], 0.0)   # -200 zeroed

    def test_sm4_string_index_no_extra_rows(self):
        """sm4 with a string date index must not create extra integer-keyed rows."""
        df = self._make_string_indexed_df([1.0, -1.0, 3.0, -2.0])
        negflo = Negflo(df, flow_limit=0)
        negflo.sm4()
        self.assertEqual(len(df), len(negflo.df_residual))

    def test_sm7_string_index_no_extra_rows(self):
        """sm7 with a string date index must not create extra integer-keyed rows."""
        df = self._make_string_indexed_df([2.0, -1.0, 1.0])
        negflo = Negflo(df, flow_limit=0)
        negflo.sm7()
        self.assertEqual(len(df), len(negflo.df_residual))


if __name__ == '__main__':
    unittest.main()
