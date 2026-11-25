"""
Rule tests for qastat2s.

Each test pins one rule confirmed against the legacy C#/R QAStat (or chosen
deliberately), using small synthetic series whose expected answers can be
worked out by hand. See the class docstrings for what each group protects.
"""
import unittest
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

from bulum.stats.qastat2s.reportcard_data import ReportCardConfig, ReportCardProcessor
from bulum.stats.qastat2s.reportcard_plot import _r_limits

FIVE, FOUR, THREE, TWO, ONE = (ReportCardProcessor._stars(n) for n in (5, 4, 3, 2, 1))


def _processor(obs, mod=None, rain=None, start="2000-07-01", **config):
    """Aligned processor (exceedance computed) from daily arrays; NaN = missing."""
    obs = np.asarray(obs, dtype=float)
    mod = obs.copy() if mod is None else np.asarray(mod, dtype=float)
    rain = np.zeros(len(obs)) if rain is None else np.asarray(rain, dtype=float)
    idx = pd.date_range(start, periods=len(obs), freq="D")
    p = ReportCardProcessor(ReportCardConfig(input_file_path=Path("."), **config))
    p.obs_raw = pd.DataFrame({"obs": obs}, index=idx)
    p.mod_raw = pd.DataFrame({"mod": mod}, index=idx)
    p.rain_raw = pd.DataFrame({"rain": rain}, index=idx)
    p.align_data()
    p.compute_exceedance()
    return p


def _stats(obs, mod=None, **config):
    """Statistics with manual thresholds unless overridden."""
    config = {"p_high": 0.1, "p_low": 0.8, **config}
    p = _processor(obs, mod, **config)
    p.resolve_thresholds()
    return p.compute_statistics()


class TestZeroCrossoverPoint(unittest.TestCase):
    """ZCP: Cunnane position of the first sorted obs value <= 0.1 ML/d, obs only."""

    OBS = [50, 0.05, 30, 0.1, 20, 40, 0.5, 0]  # sorted: 50 40 30 20 0.5 0.1 0.05 0

    def test_cunnane_position_of_first_value_at_or_below_threshold(self):
        # First value <= 0.1 is the 0.1 at sorted position 5 (0-based);
        # a strict < would pick position 6
        p = _processor(self.OBS)
        self.assertAlmostEqual(p.zero_crossover_point(), (5 + 0.6) / (8 + 0.2))

    def test_uses_observed_only(self):
        p_same = _processor(self.OBS)
        p_zero_mod = _processor(self.OBS, mod=np.zeros(8))
        self.assertEqual(p_same.zero_crossover_point(), p_zero_mod.zero_crossover_point())

    def test_never_reaches_threshold_returns_one(self):
        p = _processor(np.arange(1, 21))
        self.assertEqual(p.zero_crossover_point(), 1.0)
        p.resolve_thresholds()
        self.assertEqual((p.config.p_high, p.config.p_low), (0.1, 0.8))


class TestThresholdResolution(unittest.TestCase):
    """Default auto thresholds, per-threshold precedence q_* > p_* > auto, warnings."""

    @classmethod
    def setUpClass(cls):
        rng = np.random.default_rng(3)
        obs = rng.lognormal(1, 2, 1000)
        obs[rng.random(1000) < 0.3] = 0.0
        cls.obs = obs

    def _resolved(self, **config):
        p = _processor(self.obs, **config)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            p.resolve_thresholds()
        return p

    def test_default_is_auto_rounded_to_whole_percent(self):
        p = self._resolved()
        zcp = p.zero_crossover_point()
        self.assertEqual(p.config.p_high, round(0.1 * zcp, 2))
        self.assertEqual(p.config.p_low, round(0.8 * zcp, 2))

    def test_precedence_per_threshold(self):
        auto = self._resolved().config
        n = len(self.obs)
        from_q = lambda q: np.sum(self.obs >= q) / n
        cases = [
            ({"p_high": 0.05}, 0.05, auto.p_low),
            ({"p_low": 0.4}, auto.p_high, 0.4),
            ({"q_high": 50.0}, from_q(50.0), auto.p_low),
            ({"q_low": 0.5, "p_low": 0.6}, auto.p_high, from_q(0.5)),
            ({"p_high": 0.02, "q_high": 50.0, "p_low": 0.6, "q_low": 0.5},
             from_q(50.0), from_q(0.5)),
        ]
        for config, p_high, p_low in cases:
            with self.subTest(config=config):
                cfg = self._resolved(**config).config
                self.assertAlmostEqual(cfg.p_high, p_high)
                self.assertAlmostEqual(cfg.p_low, p_low)

    def test_flow_threshold_cuts_at_exact_count_including_ties(self):
        # 3 x 100, 4 x 50 (tied at q_high), 5 x 10, 8 x 0.5
        obs = np.array([100] * 3 + [50] * 4 + [10] * 5 + [0.5] * 8, dtype=float)
        stats = _stats(np.random.default_rng(0).permutation(obs), q_high=50.0, q_low=10.0,
                       p_high=None, p_low=None)
        self.assertEqual(stats.high_flow_obs, 3 * 100 + 4 * 50)
        self.assertEqual(stats.med_flow_obs, 5 * 10)
        self.assertEqual(stats.low_flow_obs, 8 * 0.5)

    def test_warns_once_when_p_and_q_both_given(self):
        p = _processor(self.obs, p_high=0.1, q_high=50.0)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            p.resolve_thresholds()
            p.resolve_thresholds()  # second call must be a no-op
            p.compute_statistics()
        self.assertEqual(len(w), 1)
        self.assertIn("q_high", str(w[0].message))
        self.assertAlmostEqual(p.config.p_high, np.sum(self.obs >= 50.0) / len(self.obs))

    def test_no_warning_for_q_alone(self):
        p = _processor(self.obs, q_high=50.0, q_low=0.5)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            p.resolve_thresholds()
        self.assertEqual(w, [])

    def test_p_high_above_p_low_raises(self):
        # No zero flows -> ZCP = 1, so auto p_high = 0.1 > manual p_low
        p = _processor(np.arange(1, 101), p_low=0.05)
        with self.assertRaises(ValueError):
            p.resolve_thresholds()


class TestExceedance(unittest.TestCase):
    """Exceedance plot uses Cunnane positions, consistent with the ZCP."""

    def test_cunnane_plotting_positions(self):
        n = 50
        probs = _processor(np.arange(n)).exceedance.probabilities
        np.testing.assert_allclose(probs, (np.arange(1, n + 1) - 0.4) / (n + 0.2))


class TestFlowBands(unittest.TestCase):
    """Band cut = round(p * n) on each series' own sort; ties split by position."""

    def test_cut_rounds_not_truncates(self):
        # n = 26: 0.1 * 26 = 2.6 -> 3 (int() would give 2); 0.9 * 26 = 23.4 -> 23
        obs = np.random.default_rng(1).permutation(np.arange(1, 27))
        stats = _stats(obs, p_high=0.1, p_low=0.9)
        self.assertEqual(stats.high_flow_obs, 26 + 25 + 24)
        self.assertEqual(stats.med_flow_obs, sum(range(4, 24)))
        self.assertEqual(stats.low_flow_obs, 3 + 2 + 1)

    def test_exact_half_rounds_to_even(self):
        # 0.25 * 10 = 2.5 -> 2 (half to even); half up would give 3.
        # Revisit if the legacy Dummy_half run shows half-up.
        obs = np.arange(10, 0, -1)
        stats = _stats(obs, p_high=0.25, p_low=0.8)
        self.assertEqual(stats.high_flow_obs, 10 + 9)

    def test_each_series_uses_own_sort(self):
        rng = np.random.default_rng(2)
        obs = rng.permutation(np.arange(1, 101)).astype(float)
        mod = rng.permutation(obs) * 2  # same distribution x2, different day order
        stats = _stats(obs, mod, p_high=0.1, p_low=0.8)
        self.assertEqual(stats.high_flow_mod, 2 * stats.high_flow_obs)
        self.assertEqual(stats.med_flow_mod, 2 * stats.med_flow_obs)
        self.assertEqual(stats.low_flow_mod, 2 * stats.low_flow_obs)

    def test_ties_at_cut_split_by_position(self):
        # sorted: 9 5 5 | 5 5 5 5 5 | 5 1  (cuts at 3 and 8 both fall inside the tied 5s)
        obs = [5, 5, 1, 5, 5, 9, 5, 5, 5, 5]
        stats = _stats(obs, p_high=0.3, p_low=0.8)
        self.assertEqual(stats.high_flow_obs, 9 + 5 + 5)
        self.assertEqual(stats.med_flow_obs, 5 * 5)
        self.assertEqual(stats.low_flow_obs, 5 + 1)


class TestNonMatchingZeroDays(unittest.TestCase):
    """Zero = flow < 1 ML/d; mismatches counted both ways over paired days."""

    def test_two_way_with_strict_threshold(self):
        obs = [0, 0, 5, 5, 0.5, 1.0, 3, 3]
        mod = [0, 5, 0, 5, 0, 0.5, 3, 0.99]
        # obs zero: T T F F T F F F   (1.0 is not zero)
        # mod zero: T F T F T T F T   -> 4 of 8 differ (one-way would be 3)
        stats = _stats(obs, mod)
        self.assertAlmostEqual(stats.non_matching_zero_days, 50.0)

    def test_denominator_is_paired_days(self):
        obs = [0, 0, 5, 5, np.nan, np.nan]
        mod = [0, 5, 5, 5, 0, 0]
        stats = _stats(obs, mod)
        self.assertAlmostEqual(stats.non_matching_zero_days, 25.0)


class TestRatings(unittest.TestCase):
    """Star bands from the spec tables (hard-coded here, not read from RATING_BANDS)."""

    SPEC = {
        "whole": (0.1, 1, 5, 10),
        "partial": (1, 5, 10, 25),
        "std_dev": (1, 5, 10, 25),
        "zero_diff": (0.1, 1, 5, 10),
        "non_matching": (5, 10, 25, 50),
    }

    def setUp(self):
        self.p = ReportCardProcessor(ReportCardConfig(input_file_path=Path(".")))

    def test_bands_inclusive_upper_limits(self):
        stars = (FIVE, FOUR, THREE, TWO)
        below = (FOUR, THREE, TWO, ONE)
        for kind, limits in self.SPEC.items():
            for limit, at, above in zip(limits, stars, below):
                with self.subTest(kind=kind, limit=limit):
                    self.assertEqual(self.p.get_rating(limit, kind), at)
                    self.assertEqual(self.p.get_rating(limit * 1.01, kind), above)

    def test_negative_rated_by_magnitude(self):
        self.assertEqual(self.p.get_rating(-4.0, "partial"), self.p.get_rating(4.0, "partial"))

    def test_nan_is_blank(self):
        self.assertEqual(self.p.get_rating(np.nan, "whole"), "")
        self.assertEqual(self.p.get_nse_rating(np.nan), "")

    def test_nse_strict_lower_limits(self):
        cases = [(0.901, FIVE), (0.90, FOUR), (0.751, FOUR), (0.75, THREE),
                 (0.501, THREE), (0.50, TWO), (0.251, TWO), (0.25, ONE), (-1.0, ONE)]
        for nse, expected in cases:
            with self.subTest(nse=nse):
                self.assertEqual(self.p.get_nse_rating(nse), expected)


class TestDriest3Year(unittest.TestCase):
    """Driest window chosen by rainfall over valid whole water years (<= 20% missing)."""

    # 9 water years, WY2000-WY2008. Rain lowest in WY2002-04, flow lowest in WY2006-08.
    RAIN = [3, 4, 1, 1, 1, 3, 3, 3, 3]
    FLOW = [10, 10, 5, 5, 5, 10, 1, 1, 1]

    def _data(self):
        idx = pd.date_range("2000-07-01", "2009-06-30", freq="D")
        wy = np.where(idx.month >= 7, idx.year, idx.year - 1) - 2000
        obs = np.array(self.FLOW, dtype=float)[wy]
        rain = np.array(self.RAIN, dtype=float)[wy]
        return idx, wy, obs, rain

    def _driest(self, obs, rain, mod=None):
        p = _processor(obs, mod, rain, p_high=0.1, p_low=0.8)
        return p._compute_driest_3yr_mean(p.get_paired_data())

    def test_window_by_rainfall_not_flow(self):
        _, _, obs, rain = self._data()
        obs_mean, mod_mean = self._driest(obs, rain)
        self.assertAlmostEqual(obs_mean, 5.0)  # lowest-flow window would give 1.0
        self.assertAlmostEqual(mod_mean, 5.0)

    def _drop_in_wy2002(self, n_missing):
        idx, wy, obs, rain = self._data()
        obs = obs.copy()
        obs[np.flatnonzero(wy == 2)[:n_missing]] = np.nan  # WY2002 has 365 days
        return idx, wy, obs, rain

    def test_exactly_20_percent_missing_is_valid(self):
        # 73 / 365 = 20%. Note: the code computes 1 - 292/365 = 0.19999999999999996,
        # so this passes with either <= or < 0.20; it guards against tighter limits only.
        _, _, obs, rain = self._drop_in_wy2002(73)
        obs_mean, _ = self._driest(obs, rain)
        self.assertAlmostEqual(obs_mean, 5.0)

    def test_over_20_percent_missing_excludes_year(self):
        idx, wy, obs, rain = self._drop_in_wy2002(74)
        # Windows containing WY2002 are invalid; next lowest rain is WY2003-05 (1+1+3)
        obs_mean, _ = self._driest(obs, rain)
        in_window = np.isin(wy, [3, 4, 5])
        self.assertAlmostEqual(obs_mean, obs[in_window].mean())

    def test_no_valid_window_is_nan(self):
        idx = pd.date_range("2000-07-01", "2002-06-30", freq="D")  # 2 water years
        obs_mean, mod_mean = self._driest(np.ones(len(idx)), np.ones(len(idx)))
        self.assertTrue(np.isnan(obs_mean) and np.isnan(mod_mean))


class TestPeriodAndPairing(unittest.TestCase):
    """Total uses the full period; annual totals and residual mass use paired days only."""

    def test_total_flow_uses_full_period(self):
        # Starts mid water year, so cropping to whole water years would drop days
        obs = np.arange(1, 801, dtype=float)
        stats = _stats(obs, start="2001-01-01")
        self.assertEqual(stats.total_flow_obs, obs.sum())

    def test_annual_totals_paired_only_and_empty_years_nan(self):
        idx = pd.date_range("2000-07-01", "2003-06-30", freq="D")
        n = len(idx)
        obs = np.full(n, 2.0)
        mod = np.full(n, 3.0)
        obs[:10] = np.nan        # WY2000: obs missing 10 days
        mod[10:30] = np.nan      # WY2000: mod missing 20 other days
        mod[(idx >= "2001-07-01") & (idx < "2002-07-01")] = np.nan  # WY2001: no paired days
        p = _processor(obs, mod)
        wy = p.compute_water_year_totals()

        self.assertEqual(list(wy.years), [2000, 2001, 2002])
        paired_days_2000 = 365 - 30  # WY2000 = Jul 2000 - Jun 2001 (no leap day)
        self.assertAlmostEqual(wy.obs_totals_with_gaps[0], paired_days_2000 * 2.0 / 1000)
        self.assertAlmostEqual(wy.mod_totals_with_gaps[0], paired_days_2000 * 3.0 / 1000)
        self.assertTrue(np.isnan(wy.obs_totals_with_gaps[1]))
        self.assertTrue(np.isnan(wy.mod_totals_with_gaps[1]))
        self.assertFalse(wy.complete_years[0])
        self.assertTrue(wy.complete_years[2])

    def test_residual_mass_nan_on_unpaired_days(self):
        obs = np.array([1, 2, np.nan, 4, 5, 6], dtype=float)
        mod = np.array([1, 2, 3, 4, np.nan, 6], dtype=float)
        p = _processor(obs, mod)
        rm = p.compute_residual_mass()

        self.assertTrue(rm.dates.equals(p.aligned_data.index))
        self.assertEqual(list(np.isnan(rm.obs_residual)), [False, False, True, False, True, False])
        paired = np.array([1, 2, 4, 6]) / 1000
        expected = np.cumsum(paired) - paired.mean() * np.arange(1, 5)
        np.testing.assert_allclose(rm.obs_residual[~np.isnan(rm.obs_residual)], expected)


class TestRLimits(unittest.TestCase):
    """Axis limits padded 4% each side like R, in log10 space for log axes."""

    def test_linear(self):
        np.testing.assert_allclose(_r_limits(0, 100), (-4, 104))

    def test_log(self):
        np.testing.assert_allclose(_r_limits(1, 1000, log=True), (10 ** -0.12, 10 ** 3.12))


if __name__ == "__main__":
    unittest.main()
