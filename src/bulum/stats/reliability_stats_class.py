import calendar
from typing import Literal, Union

import numpy as np
import pandas as pd

from bulum import utils


class Reliability:
    """Reliability of water supply statistics generator.

    This class provides methods to calculate reliability statistics comparing
    water supply against demand for different timescales (monthly, annual).
    """

    def __init__(self, demand: Union[pd.Series, list, float, int], supply: pd.Series,
                 demand_timescale: Literal["daily", "monthly", "yearly"] = "daily",
                 demand_type: Literal["total", "daily_constant"] = "total",
                 ignore_leap_years=False, quiet=False) -> None:
        """Initialise Reliability class. Functions available are
        - `Reliability.ReliabilityTS`,
        - `Reliability.MonthlyReliability`, and
        - `Reliability.AnnualReliability`.

        Parameters
        ----------
        demand : :class:`pandas.Series` or list or float or int
            Demand timeseries with date string as index. Alternatively, a list
            of monthly values or single demand which will be disaggregated
            according to other input parameters.
        supply : :class:`pandas.Series`
            Daily supply timeseries with date as index.
        demand_timescale : Literal["daily","monthly","yearly"], optional
            If a float or int demand is provided, which timescale does it apply
            to. Defaults to "daily".
        demand_type : Literal["total","daily_constant"], optional
            If a non-Series demand is provided, does the monthly list or
            float/int refer to a total demand over 'demand_timescale', or a
            daily constant value to be applied. Defaults to "total".
        ignore_leap_years : bool, optional
            Decide whether leap years should be ignored in demand
            disaggregation. When True, calculations will always be based on 28
            days in Feb, and value for the 29th Feb will be equal to the 28th of
            Feb value. Defaults to False.

        Raises
        ------
        TypeError
            If demand is not one of :class:`~pandas.Series`, list, float, or int.
            If supply is not a :class:`~pandas.Series`.
            If ignore_leap_years is not a boolean.
        ValueError
            If demand_type is not "total" or "daily_constant".
            If demand_timescale is not "daily", "monthly", or "yearly".
            If demand list has fewer than 12 elements.

        """

        def maybe_print(string):
            if not quiet:
                print(string)

        if not isinstance(demand, (pd.Series, list, float, int)):
            raise TypeError("Demand must be one of timeseries, monthly list or single demand.")

        if not isinstance(supply, pd.Series):
            raise TypeError("Supply must be a single column of a date-indexed dataframe (pd.Series).")

        if isinstance(demand, pd.Series):
            maybe_print("Comparing provided demand timeseries with supply timeseries.")
            state = "ts"

        if not isinstance(demand, pd.Series):
            if demand_type not in ["total", "daily_constant"]:
                raise ValueError("demand_type must be one of \"total\" or \"daily_constant\".")

            if demand_timescale not in ["daily", "monthly", "yearly"]:
                raise ValueError("demand_timescale must be one of \"daily\", \"monthly\" or \"yearly\".")

            if ignore_leap_years not in [True, False]:
                raise TypeError(f"ignore_leap_years must be one of {True} or {False}.")

        lookup_leap = {True: 28, False: 29}

        if isinstance(demand, list):
            if len(demand) < 12:
                raise ValueError("Monthly demand list must have a length of 12.")

            maybe_print("Comparing list of monthly demands with supply timeseries. demand_timescale parameter is unused.")

            if demand_type == "daily_constant":
                maybe_print("Each monthly value in demand list will be applied as the daily demand for the respective month.")
                maybe_print("ignore_leap_years parameter is unused.")
                state = "monthly_constant_list"
            else:
                maybe_print("Each monthly total in demand list will be disaggregated to a daily demand for the respective month.")
                maybe_print(f"Leap years will be disaggregated assuming {lookup_leap[ignore_leap_years]} days in February.")
                state = "monthly_total_list"

        if isinstance(demand, (float, int)):
            if not quiet:
                maybe_print("Comparing provided demand with supply timeseries.")
            if (demand_timescale == "daily") or (demand_type == "daily_constant"):
                maybe_print(f"Assumed that daily demand is a constant {demand} ML/d.")
                if (demand_timescale == "daily") and (demand_type == "daily_constant"):
                    maybe_print(f"Only one of demand_timescale = {demand_timescale} and demand_type = {demand_type} were required.")
                    maybe_print("ignore_leap_years parameter is unused.")
                if (demand_timescale == "daily") and (demand_type != "daily_constant"):
                    maybe_print("demand_type and ignore_leap_years parameters are unused.")
                if (demand_type == "daily_constant") and (demand_timescale != "daily"):
                    maybe_print("demand_timescale and ignore_leap_years parameters are unused.")
                state = "daily_constant"
            else:
                maybe_print(f"Assumed that {demand} ML is the total demand over the {demand_timescale} timescale. {demand} ML {demand_timescale} will be disaggregated to ML/d.")
                maybe_print(f"Leap years will be disaggregated assuming {lookup_leap[ignore_leap_years]} days in February.")
                if demand_timescale == "yearly":
                    state = "yearly_total"
                if demand_timescale == "monthly":
                    state = "monthly_total"

        self.demand = demand
        self.supply = supply
        self.demand_timescale = demand_timescale
        self.demand_type = demand_type
        self.ignore_leap_years = ignore_leap_years
        self.state = state

    def ReliabilityTS(self, wy_month: int) -> pd.Series:
        """Return demand as a timeseries for input to reliability statistics.

        Matches date range of supply timeseries input.

        Parameters
        ----------
        wy_month : int
            Water year start month

        Returns
        -------
        :class:`pandas.Series`
            Demand timeseries for input to reliability stats
        """
        # If provided demand is a timeseries, just return timeseries.
        if self.state == "ts":
            common_dates = np.intersect1d(self.demand.index, self.supply.index)
            demand_ts = self.demand[common_dates]
            self.supply = self.supply[common_dates]
            return demand_ts

        else:
            # Copy date range of supply TS
            demand_ts = self.supply.copy(deep=True).astype(float)

            #  Overwrite demand_ts with constant daily demand.
            if self.state == "daily_constant":
                demand_ts[:] = self.demand
                return demand_ts

            # Overwrite demand_ts with respective month constant daily demand
            if self.state == "monthly_constant_list":
                month_list = list(utils.get_month(demand_ts.index))
                demand_ts[:] = [self.demand[x-1] for x in month_list]
                return demand_ts

            # Overwrite demand_ts with respective total month demand disaggregated to daily.
            if self.state == "monthly_total_list":
                year_month = utils.get_year_and_month(demand_ts.index)
                if not self.ignore_leap_years:
                    demand_ts[:] = [self.demand[int(x[5:7])-1]/calendar.monthrange(int(x[0:4]), int(x[5:7]))[1]
                                    for x in year_month]  # If using leap years, divide by 28 or 29 Feb days depending on year
                else:
                    demand_ts[:] = [self.demand[int(x[5:7])-1]/calendar.monthrange(2002, int(x[5:7]))[1] for x in year_month]  # If not using leap years, only divide by 28 Feb days
                return demand_ts

            # Overwrite demand_ts with total month demand disaggregated to daily.
            if self.state == "monthly_total":
                year_month = utils.get_year_and_month(demand_ts.index)
                if not self.ignore_leap_years:
                    demand_ts[:] = [self.demand/calendar.monthrange(int(x[0:4]), int(x[5:7]))[1] for x in year_month]  # If using leap years, divide by 28 or 29 Feb days depending on year
                else:
                    demand_ts[:] = [self.demand/calendar.monthrange(2002, int(x[5:7]))[1] for x in year_month]  # If not using leap years, only divide by 28 Feb days
                return demand_ts

            # Overwrite demand_ts with total annual demand disaggregated to daily.
            if self.state == "yearly_total":
                if not self.ignore_leap_years:
                    wy = utils.get_wy(demand_ts.index, wy_month, using_end_year=False)
                    if wy_month > 2:
                        # If using leap years, divide by 365 or 366 days depending on year. If wy_month > 2, leap day will occur in the next calendar year
                        demand_ts[:] = [self.demand/(365+calendar.isleap(x+1)) for x in wy]
                    else:
                        # If using leap years, divide by 365 or 366 days depending on year. If wy_month <= 2, leap day will occur in this calendar year
                        demand_ts[:] = [self.demand/(365+calendar.isleap(x)) for x in wy]
                else:
                    demand_ts[:] = self.demand/365  # If not using leap years, only ever divide by 365
                return demand_ts

    def MonthlyReliability(self, tol: float = 1, allow_part_months: bool = False, wy_month: int = 7) -> float:
        """Calculate the monthly reliability statistic for daily timeseries of demand and supply.

        Parameters
        ----------
        tol : float, optional
            Percentage of demand treated as full demand. Defaults to 1 (100%).
        allow_part_months : bool, optional
            Allow part months or only complete months. Defaults to False.
        wy_month : int, optional
            Water year start month. Defaults to 7.

        Returns
        -------
        float
            Monthly reliability (%)

        """
        # Pass input to reliability_ts first and enforce common date range.
        demand_ts = self.ReliabilityTS(wy_month)

        # Collate timeseries data to monthly
        if allow_part_months:
            dem_month = demand_ts.groupby(utils.get_year_and_month(demand_ts.index)).sum()
            sup_month = self.supply.groupby(utils.get_year_and_month(self.supply.index)).sum()
        else:
            if self.supply.index[0][8:10] == "01":  # 0123-56-89
                # First date is the start of a month; use this as start date.
                start_date = self.supply.index[0]
            else:
                # Start on the first date of the next month
                start_date = utils.get_next_month_start(self.supply.index[0])
            if self.supply.index[-1] == utils.get_this_month_end(self.supply.index[-1]):
                end_date = self.supply.index[-1]
            else:
                end_date = utils.get_prev_month_end(self.supply.index[-1])
            demand_trim = demand_ts[start_date:end_date]
            supply_trim = self.supply[start_date:end_date]
            year_month = utils.get_year_and_month(demand_trim.index)
            dem_month = demand_trim.groupby(year_month).sum()
            sup_month = supply_trim.groupby(year_month).sum()

        # Check whether demand is met within given tolerance (to 6 decimal places) and express as a percentage
        rel = np.where((sup_month-(dem_month*tol) < -0.000001), 0, 1).sum()/sup_month.count()

        return rel

    def AnnualReliability(self, tol: float = 1, wy_month: int = 7, allow_part_years: bool = False) -> float:
        """Calculate the annual reliability statistic for daily timeseries of demand and supply.

        Parameters
        ----------
        tol : float, optional
            Percentage of demand treated as full demand. Defaults to 1 (100%).
        wy_month : int, optional
            Water year start month. Defaults to 7.
        allow_part_years : bool, optional
            Allow part water years or only complete water years. Defaults to False.

        Returns
        -------
        float
            Annual reliability (%)
        """

        # Pass input to reliability_ts first and enforce common date range.
        demand_ts = self.ReliabilityTS(wy_month)

        # Collate timeseries data to annual
        if not allow_part_years:
            demand_ts = utils.crop_to_wy(demand_ts, wy_month)
            supply = utils.crop_to_wy(self.supply, wy_month)
        else:
            supply = self.supply
        if (len(demand_ts) == 0):
            return np.nan
        dem_annual = demand_ts.groupby(utils.get_wy(demand_ts.index, wy_month)).sum()
        sup_annual = supply.groupby(utils.get_wy(supply.index, wy_month)).sum()
        no_years = sup_annual.count()

        # Check whether demand is met within given tolerance (to 6 decimal places) and express as a percentage
        rel = np.where((sup_annual-(dem_annual*tol) < -0.000001), 0, 1).sum()/no_years

        return rel
