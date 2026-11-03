"""Tests for the negflo class."""
# pylint: skip-file

import logging
import os
import tempfile
import unittest
from datetime import datetime

import numpy as np
import pandas as pd

from bulum.stats import Negflo

logging.getLogger().setLevel(logging.CRITICAL)  # ignores logs for unredistributed negative flow

SM_METHODS = ("sm1", "sm2", "sm3", "sm4", "sm5", "sm6", "sm7")
nan = np.nan


def run(method, values, flow_limit=0.0, **kwargs):
    """Run a Negflo method on a single series; return (result list, overflow)."""
    df = pd.DataFrame({"a": values}, index=pd.date_range("2000-01-01", periods=len(values)))
    negflo = Negflo(df, flow_limit, **kwargs)
    result = getattr(negflo, method)()["a"]
    return result.tolist(), negflo.neg_overflows["a"]


class Tests(unittest.TestCase):

    def assertSeriesEqual(self, expected, actual, atol=1e-9):
        np.testing.assert_allclose(np.asarray(actual, dtype=float), np.asarray(expected, dtype=float),
                                   atol=atol, rtol=0)

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

    def test_sm1_ignores_flow_limit(self):
        """SM1 factors all positive flows, as documented for NEGFLO6."""
        result, _ = run("sm1", [-3.0, 4.0, 8.0], flow_limit=5.0)
        self.assertSeriesEqual([0.0, 3.0, 6.0], result)

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
        # [-10] distributes into [8, 6] (excess=10, rf=0); [4, 10] untouched.
        self.assertTrue(all(pd.Series([0.0, 2.0, 2.0, 2.0, 4.0, 10.0]) == s2))

    def test_sm3(self):
        """SM3 general test.

        Flows equal to the flow limit (the 1s) do not form a flow event, so all
        the negatives (4 + 1) precede the event [8], whose excess of 7 is
        reduced to 2.
        """
        df = pd.DataFrame({
            "a": [-4.0, 1.0, 1.0, -1.0, 8.0, 0.0],
        })
        negflo = Negflo(df, 1)
        negflo.sm3()
        s = negflo.df_residual["a"]
        self.assertEqual(3, np.count_nonzero(s))
        self.assertEqual(3, np.count_nonzero(s == 0))
        self.assertEqual(3, s[len(s) - 2])

    def test_sm3_does_not_carry(self):
        """Negative flow an event cannot absorb is lost, not carried."""
        result, overflow = run("sm3", [-6.0, 4.0, -1.0, 10.0])
        self.assertSeriesEqual([0.0, 0.0, 0.0, 9.0], result)
        self.assertEqual(-2.0, overflow)

    def test_sm4(self):
        """Tests ordering: smooths backward into preceding positives first.
        Negatives before the first event are carried into it."""
        df = pd.DataFrame({
            "a": [1, -1],
            "b": [-1, 1]
        })
        negflo = Negflo(df, 0)
        negflo.sm4()
        self.assertEqual(0, negflo.neg_overflows["a"])
        self.assertEqual(0, np.count_nonzero(negflo.df_residual["a"]))

        # b=[-1, 1]: no preceding positive, so the -1 is carried into the 1.
        self.assertEqual(0, negflo.neg_overflows["b"])
        self.assertEqual(0, np.count_nonzero(negflo.df_residual["b"]))

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

    def test_sm4_leading_negative(self):
        """Leading negatives with no preceding positive are carried into the
        first positive period.

        The leading -1080 and the following -400 exceed positive period 1
        [60, 80], so it is zeroed. The remaining carry also exceeds positive
        period 2 [15, 80, 105] plus the trailing -80, so it is zeroed too.
        """
        df = pd.DataFrame({
            "a": [-80.0, -160.0, -240.0, -300.0, -230.0, -40.0, -30.0,  # leading neg
                  60.0, 80.0,                                              # positive 1
                  -40.0, -140.0, -160.0, -60.0,                           # neg 2
                  15.0, 80.0, 105.0,                                       # positive 2
                  -80.0],                                                  # trailing neg
        })
        negflo = Negflo(df, 0)
        negflo.sm4()
        s = negflo.df_residual["a"]
        with self.subTest("positives absorbed to zero when neg exceeds period sum"):
            for i in range(7, 9):   # positive period 1
                self.assertEqual(0.0, s.iloc[i])
        with self.subTest("neg period zeroed"):
            for i in [0, 1, 2, 3, 4, 5, 6, 9, 10, 11, 12, 16]:
                self.assertEqual(0.0, s.iloc[i])

    def test_sm5_carry(self):
        """Tests to make sure ordering is correct i.e. smooths backward not
        forward.

        The leading -1 has no preceding event, and the 3 can only absorb 1 of
        the following -2, so 2 in total is not redistributed.
        """
        df = pd.DataFrame({
            "a": [-1, 0, 3, -2, 4, -1],
        })
        expect = pd.Series([0.0, 0.0, 2.0, 0.0, 3.0, 0.0])
        negflo = Negflo(df, 2)
        negflo.sm5()
        self.assertEqual(-2, negflo.neg_overflows["a"])
        self.assertTrue(all(expect == negflo.df_residual["a"]))

    def test_sm6_period_specification(self):
        """Period specification working? Negatives outside every segment are
        set to zero and count towards the overflow."""
        df = pd.DataFrame({"a":
                           [-2, 2, 2, 0, 1, 1, -2, -2, 0, -1, 1]})
        expected = pd.DataFrame({"a": [0, 1, 1, 0, 1, 1, 0, 0, 0, 0, 1]})

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
        self.assertEqual(negflo.neg_overflows["a"], -5)

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
        """SM6 start date parameter working? Days before the first segment are
        not smoothed, only clipped."""
        df = pd.DataFrame({"a":
                           [-2, 2, 2, 0, 1, 1, -2, -2, 0, -1, 1]})

        def read_date(s):
            return datetime.strptime(s, r"%d %m %y")
        df.index = pd.date_range(read_date("1 1 00"), periods=11)
        negflo = Negflo(df)
        negflo.sm6(sampling_start_date=read_date("3 1 00"))
        s = negflo.df_residual["a"]
        self.assertEqual(0, s.iloc[0])
        self.assertEqual(2, s.iloc[1])
        self.assertEqual(-2, negflo.neg_overflows["a"])

    def test_sm6_single_day_segment(self):
        """A segment may start and end on the same day (NEGFLO6 never closed
        such a segment)."""
        dates = pd.date_range("2000-01-01", periods=4)
        result, _ = run("sm6", [5.0, -1.0, 4.0, -2.0],
                        segments=[(dates[0], dates[0]), (dates[1], dates[3])])
        self.assertSeriesEqual([5.0, 0.0, 1.0, 0.0], result)

    def test_sm6_does_not_store_generated_segments(self):
        """Generated segments are not reused by later calls."""
        df = pd.DataFrame({"a": [-2.0, 2.0, 2.0, -1.0]}, index=pd.date_range("2000-01-01", periods=4))
        negflo = Negflo(df)
        negflo.sm6(sampling_frequency=pd.DateOffset(days=2))
        self.assertIsNone(negflo.segments)
        self.assertSeriesEqual([0.0, 0.5, 0.5, 0.0], negflo.sm6()["a"])

    def test_sm6_invalid_segments(self):
        dates = pd.date_range("2000-01-01", periods=6)
        df = pd.DataFrame({"a": np.ones(6)}, index=dates)
        with self.subTest("overlapping"):
            with self.assertRaises(ValueError):
                Negflo(df, segments=[(dates[0], dates[3]), (dates[3], dates[5])]).sm6()
        with self.subTest("reversed"):
            with self.assertRaises(ValueError):
                Negflo(df, segments=[(dates[3], dates[0])]).sm6()

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

    def test_sm7_absorbs_from_both_sides(self):
        """An event absorbs the negatives either side of it if it is the larger
        neighbour of both."""
        result, overflow = run("sm7", [3.0, -2.0, 10.0, -4.0, 2.0, -1.0])
        self.assertSeriesEqual([3.0, 0.0, 4.0, 0.0, 1.0, 0.0], result)
        self.assertEqual(0, overflow)

    def test_sm7_tie_goes_to_preceding_event(self):
        result, _ = run("sm7", [2.0, -1.0, 2.0])
        self.assertSeriesEqual([1.0, 0.0, 2.0], result)

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

    def _simple_df(self):
        return pd.DataFrame({"a": [-1.0, 2.0, -1.0, 3.0]}, index=pd.date_range("2000-01-01", periods=4))

    def test_sm_methods_return_dataframe(self):
        """Each sm* and cl1/rw1 method returns the result as a DataFrame."""
        df = self._simple_df()
        for method_name in ("rw1", "cl1", *SM_METHODS):
            with self.subTest(method=method_name):
                negflo = Negflo(df.copy(), 0)
                result = getattr(negflo, method_name)()
                self.assertIsInstance(result, pd.DataFrame, f"{method_name} did not return a DataFrame")

    def test_return_value_equals_df_residual(self):
        """The returned DataFrame is a copy equal in value to df_residual."""
        negflo = Negflo(self._simple_df(), 0)
        result = negflo.sm1()
        self.assertIsNot(result, negflo.df_residual)
        self.assertTrue(result.equals(negflo.df_residual))

    def test_df_residual_is_readonly(self):
        """Assigning to df_residual raises AttributeError."""
        negflo = Negflo(self._simple_df(), 0)
        with self.assertRaises(AttributeError):
            setattr(negflo, "df_residual", pd.DataFrame())

    def test_auto_reset_between_calls(self):
        """Calling sm2() twice returns the same result (auto-reset proves independence)."""
        negflo = Negflo(self._simple_df(), 0)
        result1 = negflo.sm2()
        result2 = negflo.sm2()
        self.assertTrue(result1.equals(result2))

    def test_no_chaining_across_methods(self):
        """sm1() after sm2() gives the sm1 result, not a chained sm2->sm1 result."""
        df = self._simple_df()
        negflo = Negflo(df.copy(), 0)
        standalone_sm1 = negflo.sm1()

        negflo2 = Negflo(df.copy(), 0)
        negflo2.sm2()
        after_sm2 = negflo2.sm1()

        self.assertTrue(standalone_sm1.equals(after_sm2))

    def test_input_not_modified(self):
        df = self._simple_df()
        original = df.copy()
        negflo = Negflo(df, 0)
        for method_name in ("cl1", *SM_METHODS):
            getattr(negflo, method_name)()
        self.assertTrue(df.equals(original))

    def test_negative_flow_limit_rejected(self):
        with self.assertRaises(ValueError):
            Negflo(self._simple_df(), -1.0)

    # --- Missing data -----------------------------------------------------

    def test_missing_data_preserved(self):
        """Missing days stay missing in every result, and nothing else is."""
        values = [5.0, nan, -2.0, 8.0, nan, nan, -1.0, 4.0]
        for method_name in ("rw1", "cl1", *SM_METHODS):
            with self.subTest(method=method_name):
                result, _ = run(method_name, values)
                self.assertTrue(np.array_equal(np.isnan(values), np.isnan(result)))

    def test_sm1_with_missing_data(self):
        """Missing days are excluded from the totals (previously a single NaN
        zeroed the whole series)."""
        result, overflow = run("sm1", [6.0, nan, -3.0, 6.0])
        self.assertSeriesEqual([4.5, nan, 0.0, 4.5], result)
        self.assertEqual(0, overflow)

    def test_missing_data_breaks_events(self):
        """A gap ends a flow event, so each part absorbs separately."""
        result, overflow = run("sm3", [-2.0, 4.0, nan, 4.0, -1.0, 1.0])
        self.assertSeriesEqual([0.0, 2.0, nan, 4.0, 0.0, 0.0], result)
        self.assertEqual(0, overflow)

    def test_negatives_carried_across_missing_data(self):
        """Negatives are carried across gaps like any other day."""
        values = [-6.0, nan, -2.0, 4.0, nan, 10.0, -1.0]
        with self.subTest("sm2"):
            result, overflow = run("sm2", values)
            self.assertSeriesEqual([0.0, nan, 0.0, 0.0, nan, 6.0, 0.0], result)
            self.assertEqual(-1.0, overflow)
        with self.subTest("sm4"):
            result, overflow = run("sm4", values)
            self.assertSeriesEqual([0.0, nan, 0.0, 0.0, nan, 5.0, 0.0], result)
            self.assertEqual(0, overflow)

    def test_matches_negflo6_with_missing_data(self):
        """Regression against NEGFLO6S (rebuilt from source with the SM4 typo
        fixed), with missing days excluded. NEGFLO6 rounds smoothed results to
        whole numbers (rounding 0.5 down), hence the tolerance."""
        values = [5.0, nan, nan, 8.0, -3.0, -4.0, 12.0, nan, 6.0, -2.0, nan, 9.0]
        expected = {
            "cl1": [5, nan, nan, 8, 0, 0, 12, nan, 6, 0, nan, 9],
            "sm1": [4, nan, nan, 6, 0, 0, 9, nan, 5, 0, nan, 7],
            "sm2": [5, nan, nan, 8, 0, 0, 5, nan, 6, 0, nan, 7],
            "sm3": [5, nan, nan, 8, 0, 0, 5, nan, 6, 0, nan, 7],
            # NEGFLO6 gives 7 for the last value of SM4, as it reuses the
            # previous event's negative total for an event still open at the end.
            "sm4": [5, nan, nan, 1, 0, 0, 12, nan, 4, 0, nan, 9],
            "sm5": [5, nan, nan, 1, 0, 0, 12, nan, 4, 0, nan, 9],
        }
        for method_name, expect in expected.items():
            with self.subTest(method=method_name):
                result, _ = run(method_name, values)
                self.assertSeriesEqual(expect, result, atol=0.5)

    def test_volume_balance(self):
        """For every method, the result totals the positive residual less the
        redistributed negative volume (i.e. total residual - overflow)."""
        rng = np.random.default_rng(42)
        values = rng.normal(0, 10, 400) * (rng.random(400) < 0.7)
        values[rng.random(400) < 0.1] = nan
        dates = pd.date_range("2000-01-01", periods=len(values))
        segments = [(dates[0], dates[150]), (dates[200], dates[399])]
        for flow_limit in (0.0, 5.0):
            for method_name in ("cl1", *SM_METHODS):
                with self.subTest(method=method_name, flow_limit=flow_limit):
                    result, overflow = run(method_name, values, flow_limit, segments=segments)
                    self.assertAlmostEqual(np.nansum(values) - overflow, np.nansum(result), places=6)
                    self.assertTrue(np.nanmin(result) >= 0)

    # --- Construction and output ------------------------------------------

    def test_from_flows(self):
        """Residual is measured - modelled, missing where either is missing."""
        dates = pd.date_range("2000-01-01", periods=4)
        measured = pd.DataFrame({"a": [10.0, nan, 5.0, 3.0]}, index=dates)
        modelled = pd.DataFrame({"a": [4.0, 1.0, nan]}, index=dates[:3])
        negflo = Negflo.from_flows(measured, modelled)
        self.assertSeriesEqual([6.0, nan, nan, nan], negflo.rw1()["a"])

    def test_from_flows_column_mismatch(self):
        with self.assertRaises(ValueError):
            Negflo.from_flows(pd.DataFrame({"a": [1.0]}), pd.DataFrame({"b": [1.0]}))

    def test_to_file_extension(self):
        negflo = Negflo(self._simple_df(), 0)
        negflo.sm2()
        with tempfile.TemporaryDirectory() as folder:
            negflo.to_file(out_filename=os.path.join(folder, "out"))
            negflo.to_file(out_filename=os.path.join(folder, "named.csv"))
            self.assertEqual(["named.csv", "out.sm2"], sorted(os.listdir(folder)))

    def test_run_all(self):
        df = self._simple_df()
        df.iloc[1, 0] = nan
        with tempfile.TemporaryDirectory() as folder:
            Negflo(df, 0).run_all(folder)
            self.assertEqual(sorted(["result.rw1", "result.cl1", *(f"result.{m}" for m in SM_METHODS)]),
                             sorted(os.listdir(folder)))
            written = pd.read_csv(os.path.join(folder, "result.sm2"), index_col=0)
            self.assertTrue(np.isnan(written["a"].iloc[1]))


if __name__ == '__main__':
    unittest.main()
