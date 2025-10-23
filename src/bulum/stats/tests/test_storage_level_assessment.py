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

    def test_storage_level_assessment_with_trigger_names(self):
        """Test StorageLevelAssessment with trigger names functionality."""
        # Create test data
        dates = pd.date_range('2020-01-01', '2022-12-31', freq='D')
        storage = pd.Series(np.random.uniform(10, 120, len(dates)), index=dates, name='Storage')

        # Test with trigger names
        triggers = [100, 75, 50, 25]
        trigger_names = ["Full Supply", "Level 1", "Level 2", "Critical"]

        sla = osta.StorageLevelAssessment(storage, triggers, trigger_names=trigger_names)

        # Verify trigger_names are stored correctly as dict
        expected_dict = {100: "Full Supply", 75: "Level 1", 50: "Level 2", 25: "Critical"}
        self.assertEqual(sla.trigger_names, expected_dict)
        self.assertEqual(len(sla.trigger_names), len(sla.triggers))

        # Test summary includes trigger names
        summary = sla.Summary()
        self.assertIn('Trigger Name', summary.columns)

        # Verify trigger names appear in summary
        for i, trigger in enumerate(triggers):
            self.assertEqual(summary.loc[trigger, 'Trigger Name'], trigger_names[i])

    def test_storage_level_assessment_without_trigger_names(self):
        """Test StorageLevelAssessment without trigger names (default behavior)."""
        # Create test data
        dates = pd.date_range('2020-01-01', '2022-12-31', freq='D')
        storage = pd.Series(np.random.uniform(10, 120, len(dates)), index=dates, name='Storage')

        # Test without trigger names
        triggers = [100, 75, 50, 25]
        sla = osta.StorageLevelAssessment(storage, triggers)

        # Verify trigger_names is None
        self.assertIsNone(sla.trigger_names)

        # Test summary doesn't include trigger names
        summary = sla.Summary()
        self.assertNotIn('Trigger Name', summary.columns)

    def test_add_trigger_without_names(self):
        """Test adding triggers when no trigger names are used."""
        # Create test data
        dates = pd.date_range('2020-01-01', '2021-12-31', freq='D')
        storage = pd.Series(np.random.uniform(10, 120, len(dates)), index=dates, name='Storage')

        sla = osta.StorageLevelAssessment(storage, [100, 50])

        # Add trigger without name (should work)
        sla.add_trigger(25.0)

        # Verify trigger was added
        self.assertIn(25.0, sla.triggers)
        self.assertIn(25.0, sla.events)
        self.assertIsNone(sla.trigger_names)

        # Test that all methods work with new trigger
        summary = sla.Summary()
        self.assertIn(25.0, summary.index)

        events_count = sla.EventsBelowTriggerCount()
        self.assertIn(25.0, events_count)

    def test_add_trigger_with_names(self):
        """Test adding triggers when trigger names are used."""
        # Create test data
        dates = pd.date_range('2020-01-01', '2021-12-31', freq='D')
        storage = pd.Series(np.random.uniform(10, 120, len(dates)), index=dates, name='Storage')

        sla = osta.StorageLevelAssessment(storage, [100, 50], trigger_names=["High", "Low"])

        # Add trigger with name (should work)
        sla.add_trigger(25.0, name="Critical")

        # Verify trigger and name were added
        self.assertIn(25.0, sla.triggers)
        self.assertIn(25.0, sla.events)
        self.assertEqual(sla.trigger_names[25.0], "Critical")
        self.assertEqual(len(sla.trigger_names), len(sla.triggers))

        # Test summary includes new trigger and name
        summary = sla.Summary()
        self.assertIn(25.0, summary.index)
        self.assertEqual(summary.loc[25.0, 'Trigger Name'], "Critical")

    def test_add_trigger_validation_errors(self):
        """Test validation errors when adding triggers."""
        # Create test data
        dates = pd.date_range('2020-01-01', '2021-12-31', freq='D')
        storage = pd.Series(np.random.uniform(10, 120, len(dates)), index=dates, name='Storage')

        # Test duplicate trigger error
        sla = osta.StorageLevelAssessment(storage, [100, 50])
        with self.assertRaises(ValueError) as cm:
            sla.add_trigger(100.0)  # Duplicate
        self.assertIn("already exists", str(cm.exception))

        # Test name required when trigger_names exist
        sla_named = osta.StorageLevelAssessment(storage, [100, 50], trigger_names=["High", "Low"])
        with self.assertRaises(ValueError) as cm:
            sla_named.add_trigger(25.0)  # Missing name
        self.assertIn("name parameter is required", str(cm.exception))

        # Test name provided when no trigger_names exist
        sla_unnamed = osta.StorageLevelAssessment(storage, [100, 50])
        with self.assertRaises(ValueError) as cm:
            sla_unnamed.add_trigger(25.0, name="Critical")  # Unexpected name
        self.assertIn("no trigger_names exist", str(cm.exception))

    def test_initialization_validation(self):
        """Test initialization validation for trigger_names."""
        # Create test data
        dates = pd.date_range('2020-01-01', '2021-12-31', freq='D')
        storage = pd.Series(np.random.uniform(10, 120, len(dates)), index=dates, name='Storage')

        # Test mismatched lengths
        with self.assertRaises(ValueError) as cm:
            osta.StorageLevelAssessment(storage, [100, 50, 25], trigger_names=["High", "Low"])  # 3 triggers, 2 names
        self.assertIn("trigger_names length", str(cm.exception))

        # Test valid initialization
        sla = osta.StorageLevelAssessment(storage, [100, 50], trigger_names=["High", "Low"])
        self.assertEqual(len(sla.triggers), len(sla.trigger_names))
        # Check that names are correctly mapped
        expected = {100: "High", 50: "Low"}
        self.assertEqual(sla.trigger_names, expected)

    def test_empty_series_error(self):
        """Test error handling for empty series."""
        # Create empty series that would be empty after processing
        # Use allow_part_years=True to avoid crop_to_wy issues
        empty_storage = pd.Series([], dtype=float, name='Storage')

        with self.assertRaises((ValueError, IndexError)):
            # May raise IndexError during processing or ValueError from our check
            osta.StorageLevelAssessment(empty_storage, [100, 50], allow_part_years=True)

    def test_invalid_series_type(self):
        """Test error handling for invalid series type."""
        # Create DataFrame instead of Series
        df = pd.DataFrame({'Storage': [100, 90, 80]})

        with self.assertRaises(TypeError) as cm:
            osta.StorageLevelAssessment(df, [100, 50])
        self.assertIn("pd.Series", str(cm.exception))

    def test_event_algorithm_edge_cases(self):
        """Test event detection algorithm with edge cases."""
        # Create specific test data with known events
        dates = pd.date_range('2020-01-01', '2020-01-10', freq='D')

        # Test case: storage below trigger at start and end
        storage = pd.Series([40, 60, 40, 40, 60, 40, 40, 40, 60, 40], index=dates, name='Storage')
        sla = osta.StorageLevelAssessment(storage, [50], allow_part_years=True)

        events = sla.events[50]
        # Should detect multiple events
        self.assertGreater(len(events), 0)

        # Test all analysis methods work
        annual_days = sla.AnnualDaysBelow()
        self.assertIn(50, annual_days)

        max_events = sla.EventsBelowTriggerMax()
        self.assertIn(50, max_events)

        event_counts = sla.EventsBelowTriggerCount()
        self.assertIn(50, event_counts)

    def test_water_year_parameters(self):
        """Test different water year parameters."""
        # Create test data spanning multiple years
        dates = pd.date_range('2019-07-01', '2022-06-30', freq='D')
        storage = pd.Series(np.random.uniform(10, 120, len(dates)), index=dates, name='Storage')

        # Test different water year start months
        sla_jul = osta.StorageLevelAssessment(storage, [50], wy_month=7)  # July start
        sla_jan = osta.StorageLevelAssessment(storage, [50], wy_month=1)  # January start

        # Both should work and produce results
        summary_jul = sla_jul.Summary()
        summary_jan = sla_jan.Summary()

        self.assertIsInstance(summary_jul, pd.DataFrame)
        self.assertIsInstance(summary_jan, pd.DataFrame)

        # Test allow_part_years parameter
        sla_partial = osta.StorageLevelAssessment(storage, [50], allow_part_years=True)
        sla_complete = osta.StorageLevelAssessment(storage, [50], allow_part_years=False)

        # Partial years should have more or equal data points
        self.assertGreaterEqual(len(sla_partial.df), len(sla_complete.df))

    def test_summary_single_trigger(self):
        """Test Summary method with single trigger parameter."""
        # Create test data
        dates = pd.date_range('2020-01-01', '2021-12-31', freq='D')
        storage = pd.Series(np.random.uniform(10, 120, len(dates)), index=dates, name='Storage')

        sla = osta.StorageLevelAssessment(storage, [100, 50, 25])

        # Test single trigger summary
        single_summary = sla.Summary(trigger=50)
        self.assertIsInstance(single_summary, pd.Series)

        # Test full summary
        full_summary = sla.Summary()
        self.assertIsInstance(full_summary, pd.DataFrame)
        self.assertEqual(len(full_summary), 3)  # 3 triggers

    def test_plotting_methods(self):
        """Test that plotting methods work with new triggers."""
        # Create test data with known patterns
        dates = pd.date_range('2020-01-01', '2021-12-31', freq='D')
        # Create data that will definitely have events below 50
        storage_values = np.random.uniform(30, 100, len(dates))
        storage = pd.Series(storage_values, index=dates, name='Storage')

        sla = osta.StorageLevelAssessment(storage, [75])

        # Add a new trigger and test plotting still works
        sla.add_trigger(50.0)

        # Test plotting methods (should not raise errors)
        try:
            chart1 = sla.plot_events_ranked(50.0)
            self.assertIsNotNone(chart1)

            chart2 = sla.plot_event_length_frequency(50.0)
            self.assertIsNotNone(chart2)
        except Exception as e:
            self.fail(f"Plotting methods failed after adding trigger: {e}")

    def test_trigger_names_property_validation(self):
        """Test that trigger_names property validates and works with both list and dict."""
        # Create test data
        dates = pd.date_range('2020-01-01', '2021-12-31', freq='D')
        storage = pd.Series(np.random.uniform(10, 120, len(dates)), index=dates, name='Storage')

        sla = osta.StorageLevelAssessment(storage, [100, 50, 25])

        # Test valid list assignment (converts to dict)
        sla.trigger_names = ["High", "Medium", "Low"]
        expected_dict = {100: "High", 50: "Medium", 25: "Low"}
        self.assertEqual(sla.trigger_names, expected_dict)

        # Test valid dict assignment
        new_dict = {100: "Level A", 50: "Level B", 25: "Level C"}
        sla.trigger_names = new_dict
        self.assertEqual(sla.trigger_names, new_dict)

        # Test invalid list assignment (wrong length)
        with self.assertRaises(ValueError) as cm:
            sla.trigger_names = ["High", "Low"]  # Too few names
        self.assertIn("trigger_names length", str(cm.exception))

        # Test invalid dict assignment (nonexistent trigger)
        with self.assertRaises(ValueError) as cm:
            sla.trigger_names = {100: "High", 999: "Invalid"}  # 999 not in triggers
        self.assertIn("triggers not in assessment", str(cm.exception))

        # Test setting to None (should work)
        sla.trigger_names = None
        self.assertIsNone(sla.trigger_names)

        # Test setting valid names again
        sla.trigger_names = ["A", "B", "C"]
        self.assertEqual(len(sla.trigger_names), 3)

        # Test invalid type
        with self.assertRaises(TypeError):
            sla.trigger_names = "invalid_type"

    def test_trigger_names_with_unordered_triggers(self):
        """Test trigger_names behavior when triggers are provided out of order."""
        # Create test data
        dates = pd.date_range('2020-01-01', '2021-12-31', freq='D')
        storage = pd.Series(np.random.uniform(10, 120, len(dates)), index=dates, name='Storage')

        # Test with triggers provided out of order (descending)
        unordered_triggers = [100, 25, 75, 50]  # Not sorted
        trigger_names = ["High", "Critical", "Medium", "Low"]  # Names in same order as triggers

        sla = osta.StorageLevelAssessment(storage, unordered_triggers, trigger_names=trigger_names)

        # Verify that trigger names are correctly mapped to their corresponding trigger levels
        expected_mapping = {100: "High", 25: "Critical", 75: "Medium", 50: "Low"}
        self.assertEqual(sla.trigger_names, expected_mapping)

        # Test summary includes correct names for each trigger
        summary = sla.Summary()
        self.assertEqual(summary.loc[100, 'Trigger Name'], "High")
        self.assertEqual(summary.loc[25, 'Trigger Name'], "Critical")
        self.assertEqual(summary.loc[75, 'Trigger Name'], "Medium")
        self.assertEqual(summary.loc[50, 'Trigger Name'], "Low")

    def test_trigger_names_setter_with_unordered_triggers(self):
        """Test trigger_names setter behavior when triggers are unordered."""
        # Create test data
        dates = pd.date_range('2020-01-01', '2021-12-31', freq='D')
        storage = pd.Series(np.random.uniform(10, 120, len(dates)), index=dates, name='Storage')

        # Initialize with unordered triggers, no names
        unordered_triggers = [80, 30, 60, 90]
        sla = osta.StorageLevelAssessment(storage, unordered_triggers)

        # Set trigger names using list (should map in trigger order, not sorted order)
        names_list = ["Level A", "Level B", "Level C", "Level D"]
        sla.trigger_names = names_list

        # Verify correct mapping: first name goes to first trigger, etc.
        expected_mapping = {80: "Level A", 30: "Level B", 60: "Level C", 90: "Level D"}
        self.assertEqual(sla.trigger_names, expected_mapping)

        # Test setting as dict (should work regardless of order)
        dict_names = {80: "Eighty", 30: "Thirty", 60: "Sixty", 90: "Ninety"}
        sla.trigger_names = dict_names
        self.assertEqual(sla.trigger_names, dict_names)

        # Verify summary works correctly with unordered triggers
        summary = sla.Summary()
        self.assertEqual(summary.loc[80, 'Trigger Name'], "Eighty")
        self.assertEqual(summary.loc[30, 'Trigger Name'], "Thirty")
        self.assertEqual(summary.loc[60, 'Trigger Name'], "Sixty")
        self.assertEqual(summary.loc[90, 'Trigger Name'], "Ninety")

    def test_add_trigger_preserves_name_mapping_with_unordered_triggers(self):
        """Test that adding triggers preserves correct name mapping with unordered triggers."""
        # Create test data
        dates = pd.date_range('2020-01-01', '2021-12-31', freq='D')
        storage = pd.Series(np.random.uniform(10, 120, len(dates)), index=dates, name='Storage')

        # Start with unordered triggers and names
        initial_triggers = [70, 20, 90]
        initial_names = ["Medium", "Critical", "High"]

        sla = osta.StorageLevelAssessment(storage, initial_triggers, trigger_names=initial_names)

        # Verify initial mapping
        expected_initial = {70: "Medium", 20: "Critical", 90: "High"}
        self.assertEqual(sla.trigger_names, expected_initial)

        # Add a new trigger with name
        sla.add_trigger(45, name="Low")

        # Verify the new trigger is correctly mapped and existing mappings are preserved
        expected_after_add = {70: "Medium", 20: "Critical", 90: "High", 45.0: "Low"}
        self.assertEqual(sla.trigger_names, expected_after_add)

        # Verify summary works correctly
        summary = sla.Summary()
        self.assertEqual(summary.loc[70, 'Trigger Name'], "Medium")
        self.assertEqual(summary.loc[20, 'Trigger Name'], "Critical")
        self.assertEqual(summary.loc[90, 'Trigger Name'], "High")
        self.assertEqual(summary.loc[45, 'Trigger Name'], "Low")

    def test_events_below_trigger_mean(self):
        """Test EventsBelowTriggerMean method."""
        # Create test data with known events
        dates = pd.date_range('2020-01-01', '2020-01-20', freq='D')
        # Create pattern: 5 days below 50, 3 days above, 7 days below 50, 5 days above
        storage_values = [40, 40, 40, 40, 40, 60, 60, 60, 40, 40, 40, 40, 40, 40, 40, 60, 60, 60, 60, 60]
        storage = pd.Series(storage_values, index=dates, name='Storage')

        sla = osta.StorageLevelAssessment(storage, [50], allow_part_years=True)

        # Calculate mean event lengths
        mean_events = sla.EventsBelowTriggerMean()

        # Should have two events: length 5 and length 7, so mean = (5 + 7) / 2 = 6.0
        expected_mean = 6.0
        self.assertAlmostEqual(mean_events[50], expected_mean, places=1)

        # Test with trigger that has no events
        sla_no_events = osta.StorageLevelAssessment(storage, [10], allow_part_years=True)
        mean_no_events = sla_no_events.EventsBelowTriggerMean()
        self.assertTrue(np.isnan(mean_no_events[10]))

    def test_summary_with_include_mean_false(self):
        """Test Summary method with include_mean=False (default)."""
        # Create test data
        dates = pd.date_range('2020-01-01', '2021-12-31', freq='D')
        storage = pd.Series(np.random.uniform(10, 120, len(dates)), index=dates, name='Storage')

        sla = osta.StorageLevelAssessment(storage, [100, 50])

        # Test default behavior (include_mean=False)
        summary_default = sla.Summary()
        self.assertNotIn('Average period at or below trigger (days)', summary_default.columns)

        # Test explicit include_mean=False
        summary_false = sla.Summary(include_mean=False)
        self.assertNotIn('Average period at or below trigger (days)', summary_false.columns)

    def test_summary_with_include_mean_true(self):
        """Test Summary method with include_mean=True."""
        # Create test data with known events
        dates = pd.date_range('2020-01-01', '2020-01-15', freq='D')
        # Pattern: 3 days below 50, 5 days above, 4 days below 50, 3 days above
        storage_values = [40, 40, 40, 60, 60, 60, 60, 60, 40, 40, 40, 40, 60, 60, 60]
        storage = pd.Series(storage_values, index=dates, name='Storage')

        sla = osta.StorageLevelAssessment(storage, [50, 75], allow_part_years=True)

        # Test with include_mean=True
        summary = sla.Summary(include_mean=True)
        self.assertIn('Average period at or below trigger (days)', summary.columns)
        self.assertIn('Longest period at or below trigger (days)', summary.columns)

        # Verify the average is calculated correctly for trigger 50
        # Should have two events: length 3 and length 4, so mean = (3 + 4) / 2 = 3.5
        expected_mean_50 = 3.5
        self.assertAlmostEqual(summary.loc[50, 'Average period at or below trigger (days)'], expected_mean_50, places=1)

        # For trigger 75, all values should be below, so one event of length 15
        expected_mean_75 = 15.0
        self.assertAlmostEqual(summary.loc[75, 'Average period at or below trigger (days)'], expected_mean_75, places=1)

    def test_summary_single_trigger_with_include_mean(self):
        """Test Summary method for single trigger with include_mean option."""
        # Create test data
        dates = pd.date_range('2020-01-01', '2020-01-10', freq='D')
        storage_values = [40, 40, 60, 40, 40, 40, 60, 60, 40, 60]
        storage = pd.Series(storage_values, index=dates, name='Storage')

        sla = osta.StorageLevelAssessment(storage, [50, 25], allow_part_years=True)

        # Test single trigger with include_mean=True
        single_summary = sla.Summary(trigger=50, include_mean=True)
        self.assertIsInstance(single_summary, pd.Series)
        self.assertIn('Average period at or below trigger (days)', single_summary.index)

        # Test single trigger with include_mean=False
        single_summary_no_mean = sla.Summary(trigger=50, include_mean=False)
        self.assertNotIn('Average period at or below trigger (days)', single_summary_no_mean.index)

    def test_events_below_trigger_mean_edge_cases(self):
        """Test EventsBelowTriggerMean with edge cases."""
        # Create test data
        dates = pd.date_range('2020-01-01', '2020-01-10', freq='D')

        # Test case 1: Single event
        storage_single = pd.Series([40, 40, 40, 60, 60, 60, 60, 60, 60, 60], index=dates, name='Storage')
        sla_single = osta.StorageLevelAssessment(storage_single, [50], allow_part_years=True)
        mean_single = sla_single.EventsBelowTriggerMean()
        self.assertEqual(mean_single[50], 3.0)  # Only one event of length 3

        # Test case 2: All values below trigger (entire period is one event)
        storage_all_below = pd.Series([40, 40, 40, 40, 40, 40, 40, 40, 40, 40], index=dates, name='Storage')
        sla_all_below = osta.StorageLevelAssessment(storage_all_below, [50], allow_part_years=True)
        mean_all_below = sla_all_below.EventsBelowTriggerMean()
        self.assertEqual(mean_all_below[50], 10.0)  # One event spanning all 10 days

        # Test case 3: No events (all values above trigger)
        storage_no_events = pd.Series([60, 60, 60, 60, 60, 60, 60, 60, 60, 60], index=dates, name='Storage')
        sla_no_events = osta.StorageLevelAssessment(storage_no_events, [50], allow_part_years=True)
        mean_no_events = sla_no_events.EventsBelowTriggerMean()
        self.assertTrue(np.isnan(mean_no_events[50]))

    def test_summary_with_trigger_names_and_include_mean(self):
        """Test Summary method with both trigger names and include_mean=True."""
        # Create test data
        dates = pd.date_range('2020-01-01', '2021-12-31', freq='D')
        storage = pd.Series(np.random.uniform(10, 120, len(dates)), index=dates, name='Storage')

        # Test with trigger names and include_mean
        trigger_names = ["High", "Medium", "Low"]
        sla = osta.StorageLevelAssessment(storage, [100, 50, 25], trigger_names=trigger_names)

        summary = sla.Summary(include_mean=True)

        # Should have both trigger names and average columns
        self.assertIn('Trigger Name', summary.columns)
        self.assertIn('Average period at or below trigger (days)', summary.columns)

        # Verify trigger names are correct
        self.assertEqual(summary.loc[100, 'Trigger Name'], "High")
        self.assertEqual(summary.loc[50, 'Trigger Name'], "Medium")
        self.assertEqual(summary.loc[25, 'Trigger Name'], "Low")

        # Verify average column has numeric values (not NaN for these random data triggers)
        for trigger in [100, 50, 25]:
            avg_value = summary.loc[trigger, 'Average period at or below trigger (days)']
            self.assertTrue(isinstance(avg_value, (int, float, np.number)))

    def test_events_below_trigger_max_with_length_filter(self):
        """Test EventsBelowTriggerMax with minimum length filter."""
        # Create test data with known events
        dates = pd.date_range('2020-01-01', '2020-01-20', freq='D')
        # Create pattern: 3 days below, 3 days above, 7 days below, 7 days above
        storage_values = [40]*3 + [60]*3 + [40]*7 + [60]*7
        storage = pd.Series(storage_values, index=dates, name='Storage')

        sla = osta.StorageLevelAssessment(storage, [50], allow_part_years=True)

        # Test default behavior (length=1)
        max_default = sla.EventsBelowTriggerMax()
        self.assertEqual(max_default[50], 7)  # Longest event is 7 days

        # Test with length filter
        max_long = sla.EventsBelowTriggerMax(min_length=5)
        self.assertEqual(max_long[50], 7)  # Only the 7-day event meets criteria

        max_very_long = sla.EventsBelowTriggerMax(min_length=10)
        self.assertTrue(np.isnan(max_very_long[50]))  # No events ≥ 10 days

    def test_events_below_trigger_mean_with_length_filter(self):
        """Test EventsBelowTriggerMean with minimum length filter."""
        # Create test data with known events
        dates = pd.date_range('2020-01-01', '2020-01-25', freq='D')
        # Create pattern: 2 days below, 3 days above, 5 days below, 5 days above, 10 days below
        storage_values = [40]*2 + [60]*3 + [40]*5 + [60]*5 + [40]*10
        storage = pd.Series(storage_values, index=dates, name='Storage')

        sla = osta.StorageLevelAssessment(storage, [50], allow_part_years=True)

        # Test default behavior (length=1) - events: 2, 5, 10 days
        mean_default = sla.EventsBelowTriggerMean()
        expected_mean_default = (2 + 5 + 10) / 3  # 5.67
        self.assertAlmostEqual(mean_default[50], expected_mean_default, places=1)

        # Test with length filter (length=3) - events: 5, 10 days
        mean_medium = sla.EventsBelowTriggerMean(min_length=3)
        expected_mean_medium = (5 + 10) / 2  # 7.5
        self.assertAlmostEqual(mean_medium[50], expected_mean_medium, places=1)

        # Test with length filter (length=7) - events: 10 days only
        mean_long = sla.EventsBelowTriggerMean(min_length=7)
        self.assertEqual(mean_long[50], 10.0)

        # Test with length filter that excludes all events
        mean_very_long = sla.EventsBelowTriggerMean(min_length=15)
        self.assertTrue(np.isnan(mean_very_long[50]))

    def test_events_below_trigger_aggregate_with_length_filter(self):
        """Test EventsBelowTriggerAggregate with minimum length filter."""
        # Create test data with known events
        dates = pd.date_range('2020-01-01', '2020-01-20', freq='D')
        # Create pattern: 1 day below, 2 days above, 4 days below, 3 days above, 10 days below
        storage_values = [40]*1 + [60]*2 + [40]*4 + [60]*3 + [40]*10
        storage = pd.Series(storage_values, index=dates, name='Storage')

        sla = osta.StorageLevelAssessment(storage, [50], allow_part_years=True)

        # Test default behavior (length=1) with median - events: 1, 4, 10 days
        median_default = sla.EventsBelowTriggerAggregate(np.median)
        self.assertEqual(median_default[50], 4.0)  # Median of [1, 4, 10]

        # Test with length filter (length=3) with median - events: 4, 10 days
        median_medium = sla.EventsBelowTriggerAggregate(np.median, min_length=3)
        self.assertEqual(median_medium[50], 7.0)  # Median of [4, 10]

        # Test with std function and length filter
        std_medium = sla.EventsBelowTriggerAggregate(np.std, min_length=3)
        expected_std = np.std([4, 10])
        self.assertAlmostEqual(std_medium[50], expected_std, places=1)

        # Test with length filter that excludes all events
        median_very_long = sla.EventsBelowTriggerAggregate(np.median, min_length=15)
        self.assertTrue(np.isnan(median_very_long[50]))

    def test_backward_compatibility_of_event_methods(self):
        """Test that existing calls to event methods still work (backward compatibility)."""
        # Create test data
        dates = pd.date_range('2020-01-01', '2020-01-15', freq='D')
        storage_values = [40]*3 + [60]*3 + [40]*5 + [60]*4
        storage = pd.Series(storage_values, index=dates, name='Storage')

        sla = osta.StorageLevelAssessment(storage, [50], allow_part_years=True)

        # Test that methods work without length parameter (backward compatibility)
        max_events = sla.EventsBelowTriggerMax()
        self.assertIsInstance(max_events, dict)
        self.assertIn(50, max_events)

        mean_events = sla.EventsBelowTriggerMean()
        self.assertIsInstance(mean_events, dict)
        self.assertIn(50, mean_events)

        agg_events = sla.EventsBelowTriggerAggregate(np.mean)
        self.assertIsInstance(agg_events, dict)
        self.assertIn(50, agg_events)

        # Verify the values are the same as calling with length=1
        max_events_explicit = sla.EventsBelowTriggerMax(min_length=1)
        mean_events_explicit = sla.EventsBelowTriggerMean(min_length=1)
        agg_events_explicit = sla.EventsBelowTriggerAggregate(np.mean, min_length=1)

        self.assertEqual(max_events[50], max_events_explicit[50])
        self.assertEqual(mean_events[50], mean_events_explicit[50])
        self.assertEqual(agg_events[50], agg_events_explicit[50])

    def test_event_methods_consistency_with_length_filter(self):
        """Test that all event methods give consistent results with length filters."""
        # Create test data with known events
        dates = pd.date_range('2020-01-01', '2020-01-15', freq='D')
        # Create pattern: 2 days below, 3 days above, 6 days below, 4 days above
        storage_values = [40]*2 + [60]*3 + [40]*6 + [60]*4
        storage = pd.Series(storage_values, index=dates, name='Storage')

        sla = osta.StorageLevelAssessment(storage, [50], allow_part_years=True)

        # Test consistency between different methods for same length filter
        length_filter = 3

        # Get events that meet the length criteria
        events_filtered = sla.EventsBelowTrigger(length_filter)
        count_filtered = sla.EventsBelowTriggerCount(length_filter)
        max_filtered = sla.EventsBelowTriggerMax(min_length=length_filter)
        mean_filtered = sla.EventsBelowTriggerMean(min_length=length_filter)

        # For our test data, only the 6-day event should meet length≥3 criteria
        self.assertEqual(len(events_filtered[50]), 1)  # Only one event ≥3 days
        self.assertEqual(events_filtered[50][0], 6)    # That event is 6 days
        self.assertEqual(count_filtered[50], 1)        # Count should be 1
        self.assertEqual(max_filtered[50], 6)          # Max should be 6
        self.assertEqual(mean_filtered[50], 6.0)       # Mean should be 6.0

    def test_number_water_years_below_with_length_filter(self):
        """Test NumberWaterYearsBelow with minimum days filter."""
        # Create test data with known annual days below
        dates = pd.date_range('2020-07-01', '2022-06-30', freq='D')  # 2 complete water years
        # Create pattern where some days are below trigger in each year
        storage_values = []
        for year in range(2):
            # Each year: 5 days below, rest above (so each WY has 5 days below 50)
            year_data = [40] * 5 + [60] * (365 - 5)
            storage_values.extend(year_data)

        storage = pd.Series(storage_values[:len(dates)], index=dates, name='Storage')
        sla = osta.StorageLevelAssessment(storage, [50])

        # Test default behavior (length=1) - should count both water years
        years_default = sla.NumberWaterYearsBelow()
        self.assertEqual(years_default[50], 2)  # Both years have >= 1 day below

        # Test with length=3 - should count both water years (each has 5 days >= 3)
        years_3days = sla.NumberWaterYearsBelow(min_days_per_year=3)
        self.assertEqual(years_3days[50], 2)  # Both years have >= 3 days below

        # Test with length=7 - should count no water years (each has only 5 days < 7)
        years_7days = sla.NumberWaterYearsBelow(min_days_per_year=7)
        self.assertEqual(years_7days[50], 0)  # No years have >= 7 days below

    def test_percent_water_years_below_with_length_filter(self):
        """Test PercentWaterYearsBelow with minimum days filter."""
        # Create test data with 3 complete water years
        dates = pd.date_range('2020-07-01', '2023-06-30', freq='D')  # 3 complete water years
        # Year 1: 10 days below, Year 2: 3 days below, Year 3: 1 day below
        storage_values = []
        days_below_per_year = [10, 3, 1]

        for days_below in days_below_per_year:
            year_data = [40] * days_below + [60] * (365 - days_below)
            storage_values.extend(year_data)

        storage = pd.Series(storage_values[:len(dates)], index=dates, name='Storage')
        sla = osta.StorageLevelAssessment(storage, [50])

        # Test default behavior (length=1) - should count all 3 years = 100%
        percent_default = sla.PercentWaterYearsBelow()
        self.assertAlmostEqual(percent_default[50], 1.0, places=2)  # 3/3 = 100%

        # Test with length=5 - should count 1 year (only year 1 has >= 5 days) = 33.3%
        percent_5days = sla.PercentWaterYearsBelow(min_days_per_year=5)
        self.assertAlmostEqual(percent_5days[50], 1/3, places=2)  # 1/3 = 33.3%

        # Test with length=15 - should count 0 years = 0%
        percent_15days = sla.PercentWaterYearsBelow(min_days_per_year=15)
        self.assertEqual(percent_15days[50], 0.0)  # 0/3 = 0%

    def test_water_years_methods_backward_compatibility(self):
        """Test that water years methods maintain backward compatibility."""
        # Create test data
        dates = pd.date_range('2020-01-01', '2021-12-31', freq='D')
        storage = pd.Series(np.random.uniform(10, 120, len(dates)), index=dates, name='Storage')

        sla = osta.StorageLevelAssessment(storage, [100, 50])

        # Test that methods work without length parameter (backward compatibility)
        number_years = sla.NumberWaterYearsBelow()
        self.assertIsInstance(number_years, dict)
        self.assertIn(50, number_years)
        self.assertIn(100, number_years)

        percent_years = sla.PercentWaterYearsBelow()
        self.assertIsInstance(percent_years, dict)
        self.assertIn(50, percent_years)
        self.assertIn(100, percent_years)

        # Verify the values are the same as calling with length=1
        number_years_explicit = sla.NumberWaterYearsBelow(min_days_per_year=1)
        percent_years_explicit = sla.PercentWaterYearsBelow(min_days_per_year=1)

        for trigger in [50, 100]:
            self.assertEqual(number_years[trigger], number_years_explicit[trigger])
            self.assertEqual(percent_years[trigger], percent_years_explicit[trigger])

if __name__ == '__main__':
    unittest.main()