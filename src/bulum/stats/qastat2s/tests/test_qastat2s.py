import unittest
from pathlib import Path

import matplotlib
matplotlib.use("Agg")  # headless backend for tests
import matplotlib.pyplot as plt
import pandas as pd

from bulum import io
import bulum.stats.qastat2s as qastat2s
from bulum.stats.qastat2s.reportcard_data import ReportCardProcessor, ReportCardConfig
from bulum.stats.qastat2s.reportcard_plot import _coerce_series

TESTS_DIR = "./src/bulum/stats/qastat2s/tests"
OBS_FILE = f"{TESTS_DIR}/146012A.csv"
MOD_FILE = f"{TESTS_DIR}/result_flow.csv"
RAIN_FILE = f"{TESTS_DIR}/058067.infilled.csv"


def _build_processor(obs, mod, rain):
    """Build and fully process a ReportCardProcessor from in-memory data."""
    config = ReportCardConfig(input_file_path=Path("."), heading="test")
    p = ReportCardProcessor(config)
    p.obs_raw = pd.DataFrame({"obs": _coerce_series(obs, "obs")})
    p.mod_raw = pd.DataFrame({"mod": _coerce_series(mod, "mod")})
    p.rain_raw = pd.DataFrame({"rain": _coerce_series(rain, "rain")})
    p.align_data()
    p.compute_exceedance()
    p.compute_flood_events()
    p.compute_water_year_totals()
    p.compute_residual_mass()
    p.compute_statistics()
    return p


class TestQAStat2s(unittest.TestCase):

    def tearDown(self):
        plt.close("all")

    def test_statistics(self):
        """Statistics computed from the sample data match known-good values."""
        obs = io.read_ts_csv(OBS_FILE, allow_nonnumeric=True).iloc[:, 0]
        mod = io.read_ts_csv(MOD_FILE, allow_nonnumeric=True).iloc[:, 0]
        rain = io.read_ts_csv(RAIN_FILE, allow_nonnumeric=True).iloc[:, 0]
        stats = _build_processor(obs, mod, rain).statistics

        self.assertAlmostEqual(stats.total_flow_obs, 1169959.1, places=1)
        self.assertAlmostEqual(stats.total_flow_mod, 1169959.079702, places=1)
        self.assertAlmostEqual(stats.nse, 0.885608, places=5)
        self.assertAlmostEqual(stats.mean_flow_obs, 68.032744, places=4)
        self.assertAlmostEqual(stats.high_flow_obs, 816812.2, places=1)
        self.assertAlmostEqual(stats.low_flow_obs, 8401.58, places=1)
        self.assertAlmostEqual(stats.zero_flow_days_obs, 4.913648, places=5)
        self.assertEqual(stats.valid_days, 17197)
        self.assertEqual(stats.total_days, 17433)
        self.assertAlmostEqual(stats.percent_available, 98.646246, places=4)

    def test_string_and_datetime_index_agree(self):
        """bulum string-date index and a pandas DatetimeIndex give same results."""
        # bulum-style: string 'YYYY-MM-DD' index via io.read_ts_csv
        obs_s = io.read_ts_csv(OBS_FILE, allow_nonnumeric=True).iloc[:, 0]
        mod_s = io.read_ts_csv(MOD_FILE, allow_nonnumeric=True).iloc[:, 0]
        rain_s = io.read_ts_csv(RAIN_FILE, allow_nonnumeric=True).iloc[:, 0]
        stats_str = _build_processor(obs_s, mod_s, rain_s).statistics

        # pandas DatetimeIndex
        obs_d = pd.read_csv(OBS_FILE, parse_dates=[0], dayfirst=True, index_col=0).iloc[:, 0]
        mod_d = pd.read_csv(MOD_FILE, parse_dates=[0], dayfirst=True, index_col=0).iloc[:, 0]
        rain_d = pd.read_csv(RAIN_FILE, parse_dates=[0], dayfirst=True, index_col=0).iloc[:, 0]
        stats_dt = _build_processor(obs_d, mod_d, rain_d).statistics

        self.assertAlmostEqual(stats_str.total_flow_obs, stats_dt.total_flow_obs, places=6)
        self.assertAlmostEqual(stats_str.nse, stats_dt.nse, places=10)

    def test_report_card_from_files(self):
        """The file-path API returns a populated matplotlib Figure."""
        fig = qastat2s.report_card(
            obs_file=OBS_FILE,
            mod_file=MOD_FILE,
            rain_file=RAIN_FILE,
            heading="Test Report Card",
        )
        self.assertEqual(len(fig.axes), 10)

    def test_report_card_from_data_accepts_series_and_tsdf(self):
        """report_card_from_data accepts a Series or a single-column TSDF."""
        tsdf = io.read_ts_csv(OBS_FILE, allow_nonnumeric=True)
        mod = io.read_ts_csv(MOD_FILE, allow_nonnumeric=True).iloc[:, 0]

        fig_series = qastat2s.report_card_from_data(obs=tsdf.iloc[:, 0], mod=mod)
        fig_tsdf = qastat2s.report_card_from_data(obs=tsdf[["146012A"]], mod=mod)
        self.assertEqual(len(fig_series.axes), 10)
        self.assertEqual(len(fig_tsdf.axes), 10)

    def test_coerce_series_rejects_bad_input(self):
        """Helpful errors for multi-column frames and unsupported types."""
        multi = io.read_ts_csv(OBS_FILE, allow_nonnumeric=True)  # 2 data columns
        with self.assertRaises(ValueError):
            _coerce_series(multi, "obs")
        with self.assertRaises(TypeError):
            _coerce_series([1, 2, 3], "obs")

    def test_get_colors_returns_copy(self):
        colors = qastat2s.get_colors()
        self.assertIn("observed", colors)
        colors["observed"] = "#123456"
        self.assertNotEqual(qastat2s.get_colors()["observed"], "#123456")


if __name__ == "__main__":
    unittest.main()
