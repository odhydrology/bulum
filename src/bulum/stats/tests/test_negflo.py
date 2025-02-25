import logging
import unittest

import numpy as np
import pandas as pd

from bulum.stats import Negflo

logging.getLogger().setLevel(logging.CRITICAL)  # ignores warnings for carried negative flow

# TODO: I need some reference data to be able to test the smoothing functions properly


class Tests(unittest.TestCase):
    # TODO these tests will be somewhat ad-hoc and require modification as the API (especially constructor) of the Negflo class is solidified.
    # TODO write tests with larger dataframes/series/data sets
    # TODO test data loading from file

    def test_cl1(self):
        df = pd.DataFrame({
            "a": [-1.0, 1.0],
            "b": [-5.0, -3.0]
        })
        negflo = Negflo(df, 0)
        negflo.cl1()
        self.assertEqual(1, np.count_nonzero(negflo.df_residual))

    def test_sm1(self):
        df = pd.DataFrame({
            "a": [-1.0, 1.0],
            "b": [0.0, 4.0]
        })
        negflo = Negflo(df, 0)
        negflo.sm1()
        self.assertTrue(all(0 == negflo.df_residual["a"]))
        self.assertTrue(all(df["b"] == negflo.df_residual["b"]))

    def test__smooth_flows(self):
        negflo = Negflo(pd.DataFrame(), 0)
        self.assertEqual(0, negflo._smooth_flows(-1, [1])[1][0])
        self.assertEqual(1, negflo._smooth_flows(0, [1])[1][0])

    def test__sm2_3_helper(self):
        negflo = Negflo(pd.DataFrame(), 0)
        self.assertTrue(all(0 == negflo._sm_forward_helper(pd.Series([-1, 1]))))

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
        """tests non-zero flow limit"""
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
        self.assertTrue(all(pd.Series([0.0, 5.0, 4.0, 2.0, 3.0, 6.0]) == s2))

    def test_sm3(self):
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
        """Tests to make sure ordering is correct i.e. smooths backward and not forward."""
        df = pd.DataFrame({
            "a": [1.0, -1.0],
            "b": [-1.0, 1.0]
        })
        negflo = Negflo(df, 0)
        negflo.sm4()
        self.assertEqual(0, np.count_nonzero(negflo.df_residual["a"]))
        self.assertEqual(1, np.count_nonzero(negflo.df_residual["b"]))

    def test_sm4_carry(self):
        """Tests to make sure ordering is correct i.e. smooths backward and not forward."""
        df = pd.DataFrame({
            "a": [-1.0, 0.0, 3.0, -2.0, 4.0, -1.0],
        })
        expect = pd.Series([0.0, 0.0, 2.0, 0.0, 2.0, 0.0])
        negflo = Negflo(df, 2)
        negflo.sm4()
        self.assertTrue(all(expect == negflo.df_residual["a"]))

    def test_sm5_carry(self):
        """Tests to make sure ordering is correct i.e. smooths backward and not forward."""
        df = pd.DataFrame({
            "a": [-1, 0, 3, -2, 4, -1],
        })
        expect = pd.Series([0.0, 0.0, 2.0, 0.0, 3.0, 0.0])
        negflo = Negflo(df, 2)
        negflo.sm5()
        self.assertTrue(all(expect == negflo.df_residual["a"]))

    # TODO implement and write tests for sm6

    # TODO write tests for sm7, see 4 (or more!) cases


if __name__ == '__main__':
    unittest.main()
