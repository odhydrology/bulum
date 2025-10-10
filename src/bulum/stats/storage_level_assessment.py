"""
Storage level assessment functionality for water resource analysis.

This module provides the StorageLevelAssessment class for analyzing storage
levels against trigger thresholds, including event detection, duration analysis,
and statistical summaries by water year.
"""
# As this class was originally written with Pascal style naming for methods:
# pylint: disable=C0103
from typing import Callable

import numpy as np
import pandas as pd

from bulum import utils


class StorageLevelAssessment:
    """
    Analyze storage levels against trigger thresholds for water resource management.

    This class provides comprehensive analysis of storage time series data,
    including event detection when storage falls below specified trigger levels,
    duration analysis, and statistical summaries organized by water year.
    """

    def __init__(self, df: pd.Series, triggers: list[float], wy_month: int = 7, allow_part_years: bool = False) -> None:
        """
        Initialize StorageLevelAssessment with storage data and trigger thresholds.

        Parameters
        ----------
        df : :class:`pandas.Series`
            Daily timeseries of storage data with date as index.
        triggers : list
            List of trigger thresholds to be assessed.
        wy_month : int, optional
            Water year start month. Default is 7 (July).
        allow_part_years : bool, optional
            Allow partial water years or only complete water years. Default is False.

        Raises
        ------
        ValueError
            Bad argument supplied, namely:
            - Empty series supplied
        TypeError
            - Type of argument `df` is not :class:`pandas.Series`
        """

        if not isinstance(df, pd.Series):
            raise TypeError("Storage data must be a single column of a dataframe (pd.Series)")

        self.triggers = triggers
        self.wy_month = wy_month
        self.allow_part_years = allow_part_years
        self.df = df.copy(deep=True)

        # Calculate whether to include full WYs only
        if not allow_part_years:
            self.df = utils.crop_to_wy(self.df, wy_month)  # type: ignore
        if len(self.df) == 0:
            raise ValueError("Empty series supplied to constructor")
        self.start_date = df.index[0]
        self.end_date = df.index[-1]

        # Run event algorithm on init.
        self.events = {trigger: self.EventsBelowTriggerAlgorithm(trigger) for trigger in self.triggers}

        # Get name of df Series
        self.columnname = self.df.name

        # Get count of WYs
        self.wy_count = self.df.groupby(utils.get_wy(self.df.index, self.wy_month)).sum().count()

    def AnnualDaysBelow(self) -> dict:
        """
        Calculate total days at or below trigger threshold by water year.

        Returns
        -------
        dict
            Dictionary of annual timeseries grouped by trigger threshold,
            where keys are trigger values and values are :class:`pandas.Series`
            with water year counts.
        """

        dailytrigger = {
            trigger: pd.Series(np.where(self.df <= trigger, 1, 0), index=self.df.index)
            for trigger in self.triggers
        }
        annualdaysbelow = {
            trigger: x.groupby(utils.get_wy(x.index, self.wy_month)).sum()
            for trigger, x in dailytrigger.items()
        }
        return annualdaysbelow

    def AnnualDaysBelowSummary(self, trigger: float | None = None, annualdaysbelow: dict | None = None):
        """
        Generate summary of total days at or below trigger threshold by water year.

        Parameters
        ----------
        trigger : any, optional
            Optionally provide single trigger threshold to be assessed.
            Default is None.
        annualdaysbelow : dict, optional
            Optionally provide output from AnnualDaysBelow, otherwise
            recalculate. Default is None.

        Returns
        -------
        :class:`pandas.DataFrame` or :class:`pandas.Series`
            DataFrame of total days at or below threshold by water year, grouped by
            trigger threshold. If trigger is specified, returns Series for that trigger.
        """

        # If not provided, calculate AnnualDaysBelow
        if annualdaysbelow is None:
            annualdaysbelow = self.AnnualDaysBelow()

        # Output as DataFrame
        out_df = pd.DataFrame(annualdaysbelow)

        if trigger is None:
            return out_df
        else:
            return out_df[trigger]

    def NumberWaterYearsBelow(self, annualdaysbelow: dict | None = None):
        """
        Calculate total water years with at least one day at or below trigger threshold.

        Parameters
        ----------
        annualdaysbelow : dict, optional
            Optionally provide output from AnnualDaysBelow, otherwise recalculate.
            Default is None.

        Returns
        -------
        dict
            Dictionary of total years grouped by trigger threshold.
        """

        # If not provided, calculate AnnualDaysBelow
        if annualdaysbelow is None:
            annualdaysbelow = self.AnnualDaysBelow()

        numberyears = {
            trigger: sum(1 if x > 0 else 0 for x in v)
            for trigger, v in annualdaysbelow.items()
        }
        return numberyears

    def PercentWaterYearsBelow(self, numberyears: dict | None = None):
        """
        Calculate percentage of water years with at least one day at or below trigger threshold.

        Parameters
        ----------
        numberyears : dict, optional
            Optionally provide output from NumberWaterYearsBelow, otherwise
            recalculate. Default is None.

        Returns
        -------
        dict
            Dictionary of percentage years grouped by trigger threshold.
        """

        # If not provided, calculate NumberWaterYearsBelow
        if numberyears is None:
            numberyears = self.NumberWaterYearsBelow()

        percent_years = {
            trigger: x / self.wy_count
            for trigger, x in numberyears.items()
        }
        return percent_years

    def EventsBelowTriggerAlgorithm(self, trigger: float) -> list[int]:
        """
        Calculate array of event lengths for a specific trigger threshold.

        Parameters
        ----------
        trigger : float or int
            Trigger threshold against which daily data input is assessed.

        Returns
        -------
        list
            Array where each item represents the length of a single continuous event
            below the trigger threshold.
        """
        previous_ended = True
        length_counter = 0
        event_counter = 0
        output = []

        # Determine last df index
        list_len = len(self.df) - 1

        # For every daily value in df
        for index, x in enumerate(self.df):

            # Storage less than or equal to trigger and currently in event
            # Add to count
            if x <= trigger and previous_ended is False:
                length_counter += 1

            # Storage less than or equal to trigger and not in an event
            # Append current length count to output array (if not in first event)
            # Start new event
            # Add to count
            if x <= trigger and previous_ended:
                # If not first event
                if event_counter > 0:
                    output.append(length_counter)
                    length_counter = 0
                previous_ended = False
                length_counter = length_counter + 1
                event_counter = event_counter + 1

            # Storage greater than trigger
            # End current event
            if x > trigger:
                previous_ended = True

            # If at last day, append current length count to output array
            if index == list_len:
                if event_counter > 0:
                    output.append(length_counter)
                    length_counter = 0

        return output

    def EventsBelowTrigger(self, length: int = 1) -> dict:
        """
        Get event length arrays for each trigger threshold with minimum length filter.

        Parameters
        ----------
        length : int, optional
            Minimum event length to return. Default is 1.

        Returns
        -------
        dict
            Dictionary of event length arrays, grouped by trigger threshold.
        """
        trunc_events = {
            k: [i for i in x if i >= length]
            for k, x in self.events.items()
        }
        return trunc_events

    def EventsBelowTriggerCount(self, length: int = 1) -> dict:
        """
        Count events for each trigger threshold with minimum length filter.

        Parameters
        ----------
        length : int, optional
            Minimum event length to count. Default is 1.

        Returns
        -------
        dict
            Dictionary of event counts, grouped by trigger threshold.
        """
        output = {
            k: sum(i >= length for i in x)
            for k, x in self.events.items()
        }
        return output

    def EventsBelowTriggerMax(self) -> dict:
        """
        Find maximum event length for each trigger threshold.

        Returns
        -------
        dict
            Dictionary of maximum event lengths, grouped by trigger threshold.
        """
        output = {
            k: max(x) if len(x) > 0 else np.nan
            for k, x in self.events.items()
        }
        return output

    def EventsBelowTriggerAggregate(self, function: Callable) -> dict:
        """
        Aggregate event lengths using a custom function for each trigger threshold.

        Parameters
        ----------
        function : :class:`typing.Callable`
            Function that acts on arrays/iterables and returns a single value (e.g., float).

        Returns
        -------
        dict
            Dictionary of aggregated event values, grouped by trigger threshold.
        """
        output = {
            k: function(x) if len(x) > 0 else np.nan
            for k, x in self.events.items()
        }
        return output

    def Summary(self, trigger: float | None = None) -> pd.DataFrame | pd.Series:
        """
        Generate comprehensive summary table of storage level assessment outputs.

        Parameters
        ----------
        trigger : any, optional
            Optionally provide single trigger threshold to be assessed. Default is None.

        Returns
        -------
        :class:`pandas.DataFrame` or :class:`pandas.Series`
            Comprehensive summary including start/end dates, water year statistics,
            event counts for various durations, and maximum event lengths.
            If trigger is specified, returns Series for that trigger only.
        """

        out_df = pd.DataFrame()
        temp_numberyears = self.NumberWaterYearsBelow()
        out_df['Column name'] = {trigger: self.columnname for trigger in self.triggers}
        out_df['Start date'] = {trigger: self.start_date for trigger in self.triggers}
        out_df['End date'] = {trigger: self.end_date for trigger in self.triggers}
        out_df['Number water years with at least 1 day at or below level'] = temp_numberyears
        out_df['Percentage water years with at least 1 day at or below level'] = self.PercentWaterYearsBelow(temp_numberyears)
        out_df['Number of events at or below trigger (>=1day)'] = self.EventsBelowTriggerCount()
        out_df['Number of events at or below trigger (>=7days)'] = self.EventsBelowTriggerCount(7)  # 1 week
        out_df['Number of events at or below trigger (>=30days)'] = self.EventsBelowTriggerCount(30)  # ~1 month
        out_df['Number of events at or below trigger (>=91days)'] = self.EventsBelowTriggerCount(91)  # ~3 months
        out_df['Number of events at or below trigger (>=183days)'] = self.EventsBelowTriggerCount(183)  # ~6 months
        out_df['Number of events at or below trigger (>=365days)'] = self.EventsBelowTriggerCount(365)  # 1 year
        out_df['Longest period at or below trigger (days)'] = self.EventsBelowTriggerMax()

        # If trigger is provided, subset those outputs
        if trigger is None:
            return out_df
        else:
            return out_df.loc[trigger]
