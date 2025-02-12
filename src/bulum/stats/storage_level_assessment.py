import pandas as pd
import numpy as np
from bulum import utils
from datetime import datetime, timedelta
from typing import Union

class StorageLevelAssessment:
        
    def __init__(self, df: pd.Series, triggers: list, wy_month=7, allow_part_years=False) -> None:
        """
        Args:
            df (pd.Series): Daily timeseries of storage data with date as index.
            triggers (list): List of trigger thresholds to be assessed.
            wy_month (int, optional): Water year start month. Defaults to 7.
            allow_part_years (bool, optional): Allow part water years or only complete water years. Defaults to False.
        """

        if type(df) != pd.Series:
            raise Exception("Demand must be a single column of a dataframe (pd.Series)")   

        self.triggers = triggers
        self.wy_month = wy_month
        self.allow_part_years = allow_part_years
        self.df = df.copy(deep=True)

        # Calculate whether to include full WYs only
        if not allow_part_years:
            self.df = utils.crop_to_wy(self.df, wy_month)
        if (len(self.df) == 0):
            return np.nan    
        self.start_date=df.index[0]
        self.end_date=df.index[-1]

        # Run event algorithm on init. 
        self.events = {trigger: self.EventsBelowTriggerAlgorithm(trigger) for trigger in self.triggers}      

        # Get name of df Series
        self.columnname=self.df.name

        # Get count of WYs
        self.wy_count = self.df.groupby(utils.get_wy(self.df.index, self.wy_month)).sum().count()

    def AnnualDaysBelow(self):
        """Returns the total days at or below trigger threshold by WY.

        Returns:
            dict: Dictionary of annual timeseries grouped by trigger threshold.
        """

        dailytrigger = {trigger:pd.Series(np.where(self.df<=trigger,1,0),index=self.df.index) for trigger in self.triggers}
        annualdaysbelow = {trigger: x.groupby(utils.get_wy(x.index, self.wy_month)).sum() for trigger,x in dailytrigger.items()}
        return annualdaysbelow
    
    def AnnualDaysBelowSummary(self,trigger=None,annualdaysbelow=None):
        """Returns summary of total days at or below trigger threshold by WY.

        Args:
            trigger (any, optional): Optionally provide single trigger threshold to be assessed. Defaults to None.
            annualdaysbelow (dict, optional): Optionally provide output from AnnualDaysBelow, otherwise recalculate. Defaults to None.

        Returns:
            df: DataFrame of total days at or below threshold by WY, grouped by trigger threshold.
        """

        # If not provided, calculate AnnualDaysBelow
        if annualdaysbelow==None:
            annualdaysbelow = self.AnnualDaysBelow()

        # Output as DataFrame
        out_df=pd.DataFrame(annualdaysbelow)

        if trigger==None:
            return out_df
        else:
            return out_df[trigger]

    def NumberWaterYearsBelow(self,annualdaysbelow=None):
        """Returns total WYs with at least one day at or below trigger threshold.

        Args:
            annualdaysbelow (dict, optional): Optionally provide output from AnnualDaysBelow, otherwise recalculate. Defaults to None.

        Returns:
            dict: Dictionary of total years grouped by trigger threshold.
        """

        # If not provided, calculate AnnualDaysBelow        
        if annualdaysbelow==None:
            annualdaysbelow = self.AnnualDaysBelow()

        numberyears = {trigger: sum(1 if x > 0 else 0 for x in v) for trigger,v in annualdaysbelow.items()}
        return numberyears
    
    def PercentWaterYearsBelow(self,numberyears=None):
        """Returns percent of WYs with at least one day at or below trigger threshold.

        Args:
            numberyears (dict, optional): Optionally provide output from NumberWaterYearsBelow, otherwise recalculate. Defaults to None.

        Returns:
            dict: Dictionary of percent years grouped by trigger threshold.
        """

        # If not provided, calculate NumberWaterYearsBelow
        if numberyears==None:
            numberyears = self.NumberWaterYearsBelow()

        percyears = {trigger: x/self.wy_count for trigger,x in numberyears.items()}
        return percyears
    
    def EventsBelowTriggerAlgorithm(self,trigger):
        """Returns array of event lengths.

        Args:
            trigger (any): Trigger threshold against which daily data input is assessed.

        Returns:
            list: Array where each item represents the length of a single continuous event.
        """

        previous_ended=True
        length_counter=0
        event_counter=0
        output=[]

        # Determine last df index
        list_len = len(self.df)-1

        # For every daily value in df
        for index, x in enumerate(self.df):

            ## Storage less than or equal to trigger and currently in event
            # Add to count
            if x <= trigger and previous_ended==False:
                length_counter=length_counter+1

            ## Storage less than or equal to trigger and not in an event
            # Append current length count to output array (if not in first event)
            # Start new event
            # Add to count
            if x <= trigger and previous_ended:
                # If not first event
                if event_counter > 0:
                    output.append(length_counter)
                    length_counter=0
                previous_ended=False
                length_counter=length_counter+1
                event_counter=event_counter+1
            
            ## Storage greater than trigger
            # End current event
            if x > trigger:
                previous_ended=True

            # If at last day, append current length count to output array
            if index==list_len:
                if event_counter > 0:
                    output.append(length_counter)
                    length_counter=0

        return output

    def EventsBelowTrigger(self,length=1):
        """Returns event length array for each trigger threshold.

        Args:
            length (int, optional): Optional minimum event length to return. Defaults to 1.

        Returns:
            dict: Dictionary of event length arrays, grouped by trigger threshold.
        """

        trunc_events = {k:[i for i in x if i>=length] for k,x in self.events.items()}
        return trunc_events
        
    def EventsBelowTriggerCount(self,length=1):
        """Returns count of events for each trigger threshold

        Args:
            length (int, optional): Optional minimum event length to count. Defaults to 1.

        Returns:
            dict: Dictionary of event counts, grouped by trigger threshold.
        """
        
        output = {k:sum(i >= length for i in x) for k,x in self.events.items()}
        return output
    
    def EventsBelowTriggerMax(self):
        """Returns max event length for each trigger threshold

        Returns:
            dict: Dictionary of event counts, grouped by trigger threshold.
        """
                
        output = {k:max(x) if len(x)>0 else np.nan for k,x in self.events.items()}
        return output
    
    def Summary(self,trigger=None):
        """Returns table summary of key storage level assessment outputs.

        Args:
            trigger (any, optional): Optionally provide single trigger threshold to be assessed. Defaults to None.

        Returns:
            df: Dataframe summary
        """

        out_df = pd.DataFrame()
        temp_numberyears=self.NumberWaterYearsBelow()
        out_df['Column name']={trigger: self.columnname for trigger in self.triggers}
        out_df['Start date']={trigger: self.start_date for trigger in self.triggers}
        out_df['End date']={trigger: self.end_date for trigger in self.triggers}
        out_df['Number water years with at least 1 day at or below level']=temp_numberyears
        out_df['Percentage water years with at least 1 day at or below level']=self.PercentWaterYearsBelow(temp_numberyears)
        out_df['Number of events at or below trigger (>=1day)']=self.EventsBelowTriggerCount()
        out_df['Number of events at or below trigger (>=7days)']=self.EventsBelowTriggerCount(7)
        out_df['Number of events at or below trigger (>=30days)']=self.EventsBelowTriggerCount(30)
        out_df['Number of events at or below trigger (>=91days)']=self.EventsBelowTriggerCount(91)
        out_df['Number of events at or below trigger (>=183days)']=self.EventsBelowTriggerCount(183)
        out_df['Number of events at or below trigger (>=365days)']=self.EventsBelowTriggerCount(365)
        out_df['Longest period at or below trigger (days)']=self.EventsBelowTriggerMax()

        # If trigger is provided, subset those outputs
        if trigger==None:
            return out_df
        else:
            return out_df.loc[trigger]
