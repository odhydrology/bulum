import unittest
import numpy as np
import pandas as pd

import bulum.stats as osta
from bulum import io


class TestStorageLevelAssessment(unittest.TestCase):
    """Test suite for StorageLevelAssessment class."""

    def test_storage_level_assessment_basic(self):
        """Test basic StorageLevelAssessment functionality against known outputs."""
        # Checking answers against "GB_RCP45_2050_02b_StormKingDam.in" outputs in 26009

        # Define a SLA at Storm King Dam
        df = io.read_ts_csv("./src/bulum/stats/tests/test_storage_data.csv")
        sla = osta.StorageLevelAssessment(df[r"Storage\0013 Storm King Dam\Storage Volume (ML)"], [400, 655, 1090, 1530])

        # Test if the SLA calculates the correct events (ref "GB_RCP45_2050_01a_StormKingDam.in_Events.csv")
        answer_events = sla.EventsBelowTrigger()
        read_events = {}
        read_events[400] = [75, 4, 64, 103, 9, 289]
        read_events[655] = [154, 104, 24, 188, 238, 411, 56]
        read_events[1090] = [298, 150, 3, 207, 116, 8, 27, 77, 5, 3, 299, 88, 5, 94, 9, 25, 32, 1, 4, 48, 809, 17, 98, 47, 21, 1, 40, 580, 36, 60, 79, 186]
        read_events[1530] = [3, 443, 2, 95, 255, 4, 147, 52, 355, 28, 232, 179, 174, 269, 2, 8, 104, 165, 202, 584, 142, 92, 230,
                             21, 5, 1, 57, 144, 124, 310, 2, 50, 99, 107, 147, 204, 481, 63, 984, 2, 12, 137, 533, 1167, 47, 23, 175, 257, 471]
        self.assertDictEqual(answer_events, read_events)

        # Spot testing individual results
        self.assertEqual(sla.EventsBelowTriggerMax()[655], 411)
        self.assertEqual(sla.NumberWaterYearsBelow()[1530], 61)
        self.assertAlmostEqual(sla.PercentWaterYearsBelow()[400], 0.038461538)
        self.assertEqual(sla.EventsBelowTriggerCount(365)[1090], 2)

        # Test annual table (ref "GB_RCP45_2050_01a_StormKingDam.in_AnnualDaysBelow.csv")
        answer_annual = sla.AnnualDaysBelowSummary()
        read_annual = pd.read_csv("./src/bulum/stats/tests/test_storage_data_annualdays_answers.csv", index_col=0)
        read_annual.columns = read_annual.columns.astype(int)
        pd.testing.assert_frame_equal(answer_annual, read_annual, check_dtype=False, check_index_type=False, check_column_type=False)

        # Test summary table (ref "GB_RCP45_2050_01a_StormKingDam.in_StorageAssessmentResults.csv")
        answer_summary = sla.Summary()
        read_summary = pd.read_csv("./src/bulum/stats/tests/test_storage_data_summary_answers.csv", index_col=0)

        # Since no trigger_names were provided, the format should match the legacy expected format
        pd.testing.assert_frame_equal(answer_summary, read_summary, check_dtype=False, check_index_type=False, check_column_type=False)


if __name__ == '__main__':
    unittest.main()