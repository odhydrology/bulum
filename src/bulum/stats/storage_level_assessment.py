"""
Storage level assessment functionality for water resource analysis.

This module provides the StorageLevelAssessment class for analyzing storage
levels against trigger thresholds, including event detection, duration analysis,
and statistical summaries by water year.
"""
# As this class was originally written with Pascal style naming for methods:
# pylint: disable=C0103
from typing import Literal, Optional, Union

from collections.abc import Callable

import altair as alt
import numpy as np
import pandas as pd

from bulum import utils


class StorageLevelAssessment:
    """
    Analyze storage levels against trigger thresholds for water resource management.

    This class provides comprehensive analysis of storage time series data,
    including event detection when storage falls below specified trigger levels,
    duration analysis, and statistical summaries organized by water year.

    Examples
    --------
    Basic usage with numeric triggers:

    >>> import pandas as pd
    >>> from bulum.stats import StorageLevelAssessment
    >>>
    >>> # Create sample storage data
    >>> dates = pd.date_range('2020-01-01', '2022-12-31', freq='D')
    >>> storage = pd.Series(np.random.uniform(20, 120, len(dates)), index=dates, name='Storage')
    >>>
    >>> # Initialize assessment with trigger levels
    >>> sla = StorageLevelAssessment(storage, triggers=[100, 75, 50, 25])
    >>>
    >>> # Get comprehensive summary
    >>> summary = sla.Summary()
    >>> print(summary)

    Usage with named triggers for better readability:

    >>> # Initialize with meaningful trigger names
    >>> trigger_names = ["Full Supply", "Level 1", "Level 2", "Critical"]
    >>> sla = StorageLevelAssessment(storage, triggers=[100, 75, 50, 25],
    ...                            trigger_names=trigger_names)
    >>>
    >>> summary = sla.Summary()  # Shows "Trigger Name" column
    >>>
    >>> # Add additional trigger dynamically
    >>> sla.add_trigger(10.0, name="Emergency")

    Advanced analysis and visualization:

    >>> # Get event statistics for specific trigger
    >>> events_count = sla.EventsBelowTriggerCount(length=7)  # Events >= 7 days
    >>> max_events = sla.EventsBelowTriggerMax()
    >>>
    >>> # Annual analysis
    >>> annual_days = sla.AnnualDaysBelow()
    >>> percent_years = sla.PercentWaterYearsBelow()
    >>>
    >>> # Create visualizations
    >>> chart = sla.plot_events_ranked(50, interactive=True)
    >>> freq_chart = sla.plot_event_length_frequency(25)

    Water year analysis with custom parameters:

    >>> # Use calendar year (wy_month=1) and allow partial years
    >>> sla_cal = StorageLevelAssessment(storage, triggers=[50, 25],
    ...                                wy_month=1, allow_part_years=True)
    >>>
    >>> # Get summary for single trigger
    >>> critical_summary = sla_cal.Summary(trigger=25)

    Custom aggregation of event data:

    >>> # Use custom function to analyze events
    >>> import numpy as np
    >>> mean_events = sla.EventsBelowTriggerAggregate(np.mean)
    >>> median_events = sla.EventsBelowTriggerAggregate(np.median)

    See Also
    --------
    bulum.utils.crop_to_wy : Crop data to complete water years
    bulum.utils.get_wy : Get water year for dates
    """

    def __init__(self, df: pd.Series, triggers: list[float], wy_month: int = 7, allow_part_years: bool = False, trigger_names: list[str] | None = None) -> None:
        """
        Initialize StorageLevelAssessment with storage data and trigger thresholds.

        The constructor automatically runs event detection algorithms for all trigger
        levels and prepares the data for analysis by cropping to complete water years
        if requested.

        Parameters
        ----------
        df : :class:`pandas.Series`
            Daily timeseries of storage data with date as index. The series should
            contain numeric storage values (e.g., ML, GL) and have a datetime index.
        triggers : list of float
            List of trigger thresholds to be assessed. Values should be in the same
            units as the storage data. Triggers are assessed using <= comparison
            (storage at or below trigger).
        wy_month : int, optional
            Water year start month (1-12). Default is 7 (July). For example,
            wy_month=7 means water years run from July 1 to June 30.
        allow_part_years : bool, optional
            Allow partial water years or only complete water years. When False
            (default), data is cropped to include only complete water years.
            When True, partial years at the start/end are included.
        trigger_names : list of str, optional
            Optional list of descriptive names for trigger levels. Must have same
            length as triggers if provided. Names appear in summary tables for
            better readability. Can also be provided as a dict mapping trigger
            levels to names after initialization. Default is None.

        Raises
        ------
        ValueError
            If empty series is supplied after water year cropping, or if
            trigger_names length doesn't match triggers length.
        TypeError
            If df is not a :class:`pandas.Series`.

        Examples
        --------
        >>> # Basic initialization
        >>> sla = StorageLevelAssessment(storage_data, [100, 50, 25])
        >>>
        >>> # With descriptive names and custom water year
        >>> names = ["Normal", "Alert", "Critical"]
        >>> sla = StorageLevelAssessment(storage_data, [100, 50, 25],
        ...                            trigger_names=names, wy_month=1)
        """

        if not isinstance(df, pd.Series):
            raise TypeError("Storage data must be a single column of a dataframe (pd.Series)")

        self.triggers = triggers
        # Use property setter for validation and conversion
        self.trigger_names = trigger_names
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

    @property
    def trigger_names(self) -> dict[float, str] | None:
        """Get trigger names as a dictionary mapping trigger levels to names."""
        return self._trigger_names

    @trigger_names.setter
    def trigger_names(self, value: list[str] | dict[float, str] | None) -> None:
        """Set trigger names with validation.

        Parameters
        ----------
        value : list of str, dict of {float: str}, or None
            If list: Must have same length as triggers, will be mapped in order.
            If dict: Keys must match existing trigger levels.
            If None: Clears all trigger names.
        """
        if value is None:
            self._trigger_names = None
        elif isinstance(value, list):
            if len(value) != len(self.triggers):
                raise ValueError(f"trigger_names length ({len(value)}) must match triggers length ({len(self.triggers)})")
            # Convert list to dict mapping triggers to names in order
            self._trigger_names = {trigger: value[i] for i, trigger in enumerate(self.triggers)}
        elif isinstance(value, dict):
            # Validate that all keys exist in triggers
            missing_triggers = set(value.keys()) - set(self.triggers)
            if missing_triggers:
                raise ValueError(f"trigger_names contains triggers not in assessment: {missing_triggers}")
            self._trigger_names = value.copy()
        else:
            raise TypeError("trigger_names must be a list, dict, or None")

    def add_trigger(self, trigger: float, name: str | None = None) -> None:
        """
        Add an additional trigger level to the assessment.

        Parameters
        ----------
        trigger : float
            New trigger threshold to be assessed. Must not already exist in the
            current trigger list. Value should be in same units as storage data.
        name : str, optional
            Descriptive name for the new trigger level. Required if the assessment
            was initialized with trigger_names, otherwise must be None to maintain
            consistency. Default is None.

        Raises
        ------
        ValueError
            If trigger already exists in the assessment, if name is required but
            not provided (when trigger_names exist), or if name is provided but
            no trigger_names exist.

        Examples
        --------
        >>> # Assessment without names - add trigger without name
        >>> sla = StorageLevelAssessment(storage_data, [100, 50])
        >>> sla.add_trigger(25.0)  # ✓ Valid
        >>>
        >>> # Assessment with names - name is required
        >>> sla_named = StorageLevelAssessment(storage_data, [100, 50],
        ...                                  trigger_names=["High", "Low"])
        >>> sla_named.add_trigger(25.0, name="Critical")  # ✓ Valid
        >>>
        >>> # Immediate availability for analysis
        >>> summary = sla_named.Summary()  # Includes new trigger
        >>> events = sla_named.EventsBelowTriggerCount()  # Includes new trigger
        >>> chart = sla_named.plot_events_ranked(25.0)  # Can plot new trigger

        See Also
        --------
        EventsBelowTriggerAlgorithm : Algorithm used for new trigger analysis
        Summary : Method that includes newly added triggers
        """
        # Check if trigger already exists
        if trigger in self.triggers:
            raise ValueError(f"Trigger {trigger} already exists in the assessment")

        # Validate name parameter based on existing trigger_names
        if self.trigger_names is not None:
            if name is None:
                raise ValueError("name parameter is required when trigger_names are being used")
        else:
            if name is not None:
                raise ValueError("name parameter provided but no trigger_names exist. Initialize with trigger_names or use name=None")

        # Add the trigger and run the algorithm
        self.triggers.append(trigger)
        if self.trigger_names is not None:
            # Add the new trigger and name to the dictionary
            new_names = self.trigger_names.copy()
            new_names[trigger] = name
            self.trigger_names = new_names

        # Run event algorithm for the new trigger
        self.events[trigger] = self.EventsBelowTriggerAlgorithm(trigger)

    def AnnualDaysBelow(self) -> dict:
        """
        Calculate total days at or below trigger threshold by water year.

        This method counts the number of days in each water year where storage
        was at or below each trigger threshold.

        Returns
        -------
        dict of {float: :class:`pandas.Series`}
            Dictionary where keys are trigger threshold values (float) and values
            are :class:`pandas.Series` with water years as index and day counts
            as values.

        Examples
        --------
        >>> annual_days = sla.AnnualDaysBelow()
        >>> print(annual_days[50])  # Days below 50 ML by water year
        >>> # Example output:
        >>> # 2020    45
        >>> # 2021    12
        >>> # 2022    78
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

    def NumberWaterYearsBelow(self, annualdaysbelow: dict | None = None, *, min_days_per_year: int = 1):
        """
        Calculate total water years with at least one day at or below trigger threshold.

        Parameters
        ----------
        annualdaysbelow : dict, optional
            Optionally provide output from AnnualDaysBelow, otherwise recalculate. Default is None.
        min_days_per_year : int, optional
            Minimum number of days in a water year before counting it. Only water years
            with >= this many days below the trigger are counted. Default is 1.

        Returns
        -------
        dict of {float: int}
            Dictionary of total water years grouped by trigger threshold.
        """

        # If not provided, calculate AnnualDaysBelow
        if annualdaysbelow is None:
            annualdaysbelow = self.AnnualDaysBelow()

        # Count water years with at least 'length' days below trigger
        numberyears = {
            trigger: sum(1 if days >= min_days_per_year else 0 for days in v)
            for trigger, v in annualdaysbelow.items()
        }
        return numberyears

    def PercentWaterYearsBelow(self, numberyears: dict | None = None, *, min_days_per_year: int = 1):
        """
        Calculate percentage of water years with at least one day at or below trigger threshold.

        Parameters
        ----------
        numberyears : dict, optional
            Optionally provide output from NumberWaterYearsBelow, otherwise
            recalculate. Default is None.
        min_days_per_year : int, optional
            Minimum number of days in a water year before counting it. Only water years
            with >= this many days below the trigger are counted. Default is 1.

        Returns
        -------
        dict of {float: float}
            Dictionary of percentage years grouped by trigger threshold.
        """

        # If not provided, calculate NumberWaterYearsBelow with the length filter
        if numberyears is None:
            numberyears = self.NumberWaterYearsBelow(min_days_per_year=min_days_per_year)

        percent_years = {
            trigger: x / self.wy_count
            for trigger, x in numberyears.items()
        }
        return percent_years

    def EventsBelowTriggerAlgorithm(self, trigger: float) -> list[int]:
        """
        Calculate array of event lengths for a specific trigger threshold.

        This is the core algorithm that detects continuous periods where storage
        is at or below the trigger threshold. An event starts when storage falls
        to or below the trigger and ends when storage rises above the trigger.

        Parameters
        ----------
        trigger : float
            Trigger threshold against which daily storage data is assessed.
            Uses <= comparison (storage at or below trigger).

        Returns
        -------
        list of int
            List where each element represents the length (in days) of a single
            continuous event below the trigger threshold. Empty list if no events
            occurred.

        Examples
        --------
        >>> # Get event lengths for 50 ML trigger
        >>> events = sla.EventsBelowTriggerAlgorithm(50.0)
        >>> print(events)  # e.g., [5, 12, 3, 45] - four events of different lengths
        >>>
        >>> # Analyze the events
        >>> print(f"Number of events: {len(events)}")
        >>> print(f"Longest event: {max(events)} days" if events else "No events")
        >>> print(f"Average event length: {np.mean(events):.1f} days" if events else "No events")

        Notes
        -----
        This algorithm handles edge cases including:
        - Events that start at the beginning of the time series
        - Events that end at the end of the time series
        - Single-day events
        - No events (returns empty list)
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

    def EventsBelowTrigger(self, min_length: int = 1) -> dict:
        """
        Get event length arrays for each trigger threshold with minimum length filter.

        Parameters
        ----------
        min_length : int, optional
            Minimum event length to return. Default is 1.

        Returns
        -------
        dict of {float: list of int}
            Dictionary of event length arrays, grouped by trigger threshold.
        """
        trunc_events = {
            k: [i for i in x if i >= min_length]
            for k, x in self.events.items()
        }
        return trunc_events

    def EventsBelowTriggerCount(self, min_length: int = 1) -> dict:
        """
        Count events for each trigger threshold with minimum length filter.

        Parameters
        ----------
        min_length : int, optional
            Minimum event length to count. Default is 1.

        Returns
        -------
        dict of {float: int}
            Dictionary of event counts, grouped by trigger threshold.
        """
        output = {
            k: sum(i >= min_length for i in x)
            for k, x in self.events.items()
        }
        return output

    def EventsBelowTriggerMax(self, *, min_length: int = 1) -> dict:
        """
        Find maximum event length for each trigger threshold with minimum length filter.

        Only events with duration >= length days are considered in the analysis.
        This allows filtering out short-duration events that may not be operationally
        significant.

        Parameters
        ----------
        min_length : int, optional
            Minimum event length (in days). Only events with duration >= this
            value are included in the analysis. Default is 1.

        Returns
        -------
        dict of {float: int}
            Dictionary of maximum event lengths, grouped by trigger threshold.
            Returns NaN for triggers with no events meeting the minimum length criteria.
        """
        output = {
            k: max([i for i in x if i >= min_length]) if any(i >= min_length for i in x) else np.nan
            for k, x in self.events.items()
        }
        return output

    def EventsBelowTriggerMean(self, *, min_length: int = 1) -> dict:
        """
        Calculate mean event length for each trigger threshold with minimum length filter.

        Only events with duration >= length days are considered in the analysis.
        This allows filtering out short-duration events that may not be operationally
        significant when calculating average event durations.

        Parameters
        ----------
        min_length : int, optional
            Minimum event length (in days). Only events with duration >= this
            value are included in the analysis. Default is 1.

        Returns
        -------
        dict of {float: float}
            Dictionary of mean event lengths, grouped by trigger threshold.
            Returns NaN for triggers with no events meeting the minimum length criteria.

        Examples
        --------
        >>> mean_events = sla.EventsBelowTriggerMean()
        >>> print(mean_events[50])  # Average event length for 50 ML trigger
        >>> # Example output: 12.5 (average of all events below 50 ML)
        >>>
        >>> # Only consider events 7 days or longer before counting
        >>> mean_long_events = sla.EventsBelowTriggerMean(length=7)
        >>> print(mean_long_events[50])  # Average of events >= 7 days only

        See Also
        --------
        EventsBelowTriggerMax : Maximum event lengths
        EventsBelowTriggerAggregate : Custom aggregation functions
        """
        output = {
            k: np.mean([i for i in x if i >= min_length]) if any(i >= min_length for i in x) else np.nan
            for k, x in self.events.items()
        }
        return output

    def EventsBelowTriggerAggregate(self, function: Callable, *, min_length: int = 1) -> dict:
        """
        Aggregate event lengths using a custom function for each trigger threshold with minimum length filter.

        Only events with duration >= length days are considered in the analysis.
        This allows filtering out short-duration events before applying custom
        aggregation functions like median, standard deviation, percentiles, etc.

        Parameters
        ----------
        function : :class:`typing.Callable`
            Function that acts on arrays/iterables and returns a single value (e.g., float).
        min_length : int, optional
            Minimum event length (in days). Only events with duration >= this
            value are included in the analysis. Default is 1.

        Returns
        -------
        dict of {float: float}
            Dictionary of aggregated event values, grouped by trigger threshold.
            Returns NaN for triggers with no events meeting the minimum length criteria.

        Examples
        --------
        >>> import numpy as np
        >>> # Get median of events 30 days or longer before counting
        >>> median_long_events = sla.EventsBelowTriggerAggregate(np.median, length=30)
        >>>
        >>> # Get 95th percentile of events 7 days or longer
        >>> p95_events = sla.EventsBelowTriggerAggregate(
        ...     lambda x: np.percentile(x, 95), length=7)
        """
        output = {
            k: function([i for i in x if i >= min_length]) if any(i >= min_length for i in x) else np.nan
            for k, x in self.events.items()
        }
        return output

    def Summary(self, trigger: float | None = None, include_mean: bool = False) -> pd.DataFrame | pd.Series:
        """
        Generate comprehensive summary table of storage level assessment outputs.

        Parameters
        ----------
        trigger : any, optional
            Optionally provide single trigger threshold to be assessed. Default is None.
        include_mean : bool, optional
            Include average event length column in the summary. Default is False.

        Returns
        -------
        :class:`pandas.DataFrame` or :class:`pandas.Series`
            Comprehensive summary including start/end dates, water year statistics,
            event counts for various durations, and maximum event lengths.
            If trigger is specified, returns Series for that trigger only.
            When trigger names are provided, they are displayed in the summary.
            When include_mean is True, adds "Average period at or below trigger (days)" column.
        """

        out_df = pd.DataFrame()
        temp_numberyears = self.NumberWaterYearsBelow()

        # Add trigger names column if names are provided
        if self.trigger_names is not None:
            out_df['Trigger Name'] = {trigger: self.trigger_names[trigger] for trigger in self.triggers}
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

        # Add average event length column if requested
        if include_mean:
            out_df['Average period at or below trigger (days)'] = self.EventsBelowTriggerMean()

        # If trigger is provided, subset those outputs
        if trigger is None:
            return out_df
        else:
            return out_df.loc[trigger]

    # Additional arguments for plot functions are there for convenience and to
    # simplify otherwise-tedious manipulation.
    # pylint: disable=too-many-arguments
    def plot_events_ranked(self, trigger: float, *,
                           width=600, height=400,
                           xmax: int | None = None,
                           interactive=False,
                           bind_y=True,
                           mark: Literal["bar", "rect"] = "bar"
                           ) -> alt.Chart:
        """
        Create an Altair chart of ranked event durations below the trigger threshold.

        Parameters
        ----------
        trigger : float
            Trigger level for which to plot the ranking.
        width : int, optional
            Plot width. Default is 600.
        height : int, optional
            Plot height. Default is 400.
        xmax : int, optional
            Controls how many data points to plot. Default is None, indicating all data.
        interactive : bool, optional
            Set to True to enable pan and zoom functionality. Default is False.
        bind_y : bool, optional
            When True (default), zooming will only be horizontal; vertical values remain fixed.
        mark : {"bar", "rect"}, optional
            Controls plot style. "rect" fixes gaps between data but may be harder to read.
            Default is "bar".

        Returns
        -------
        :class:`altair.Chart`
            Altair chart showing ranked event durations with tooltips and interactive features.

        Raises
        ------
        KeyError
            Provided trigger has not been evaluated previously for this assessment.
        ValueError
            Invalid keyword argument supplied.
        """
        try:
            arr = self.events[trigger]
        except KeyError as e:
            raise KeyError(f"{trigger} is not a computed trigger level") from e

        df = pd.DataFrame({"data": sorted(arr, reverse=True)})
        if xmax is None:
            xmax = df.index.max()

        if interactive:
            clip = False
            clamp = True
        else:
            clip = True
            clamp = False

        title = alt.TitleParams(f'Trigger={trigger:,} ML', anchor='middle')
        chart = alt.Chart(df.reset_index(names="index"), title=title)  # type: ignore

        if mark == "bar":
            chart = chart.mark_bar(clip=clip, minBandSize=1)
        elif mark == "rect":
            chart = chart.mark_rect(clip=clip, minBandSize=1)
        else:
            raise ValueError(f"Keyword argument `mark` must be one of 'bar' or 'rect'. Received '{mark}'")

        chart = (chart
                 .encode(x=alt.X('index', title="Event number (ranked)",
                                 scale=alt.Scale(domain=[0, xmax],
                                                 clamp=clamp,
                                                 padding=0.1,
                                                 ),
                                 axis=alt.Axis(tickMinStep=1),
                                 ),
                         y=alt.Y('data:Q', title="Event length (days)",
                                 scale=alt.Scale(clamp=clamp)),
                         tooltip=[alt.Tooltip("index", title="Event rank"),
                                  alt.Tooltip("data", title="Event length (days)")],
                         )
                 .properties(width=width, height=height))
        if interactive:
            chart = chart.interactive(bind_y=bind_y)
        return chart

    def plot_event_length_frequency(self, trigger: float, *,
                                    width=600, height=400,
                                    xmax: int | None = None,
                                    interactive=False,
                                    bind_y=True,
                                    mark: Literal["bar", "rect"] = "bar"
                                    ) -> alt.Chart:
        """
        Create an Altair chart showing frequency distribution of event lengths.

        Parameters
        ----------
        trigger : float
            Trigger level for which to plot the frequency distribution.
        width : int, optional
            Plot width. Default is 600.
        height : int, optional
            Plot height. Default is 400.
        xmax : int, optional
            Maximum event length to plot. Default is None, indicating all data.
        interactive : bool, optional
            Set to True to enable pan and zoom functionality. Default is False.
        bind_y : bool, optional
            When True (default), zooming will only be horizontal; vertical values remain fixed.
        mark : {"bar", "rect"}, optional
            Controls plot style. "rect" fixes gaps between data but may be harder to read.
            Default is "bar".

        Returns
        -------
        :class:`altair.Chart`
            Altair chart showing frequency distribution of event lengths with tooltips.

        Raises
        ------
        KeyError
            Provided trigger has not been evaluated previously for this assessment.
        ValueError
            Invalid keyword argument supplied.
        """
        try:
            arr = self.events[trigger]
        except KeyError as e:
            raise KeyError(f"{trigger} is not a computed trigger level") from e

        df = pd.DataFrame({"data": sorted(arr, reverse=True)})
        if xmax is None:
            xmax = df["data"].max()

        if interactive:
            clip = False
            clamp = True
        else:
            clip = True
            clamp = False

        title = alt.TitleParams(f'Trigger={trigger:,} ML', anchor='middle')
        chart = alt.Chart(df.reset_index(names="index"), title=title)  # type: ignore

        if mark == "bar":
            chart = chart.mark_bar(clip=clip, minBandSize=1)
        elif mark == "rect":
            chart = chart.mark_rect(clip=clip, minBandSize=1)
        else:
            raise ValueError(f"Keyword argument `mark` must be one of 'bar' or 'rect'. Received '{mark}'")

        chart = (chart
                 .encode(x=alt.X('data', title="Event length (days)",
                                 scale=alt.Scale(domain=[0, xmax],
                                                 clamp=clamp,
                                                 padding=0.1,
                                                 ),
                                 axis=alt.Axis(tickMinStep=1),
                                 ),
                         y=alt.Y('count()', title="Occurrences",
                                 scale=alt.Scale(clamp=clamp)),
                         tooltip=[alt.Tooltip("data", title="Event length (days)"),
                                  alt.Tooltip("count()", title="Frequency")],
                         )
                 .properties(width=width, height=height))
        if interactive:
            chart = chart.interactive(bind_y=bind_y)
        return chart
