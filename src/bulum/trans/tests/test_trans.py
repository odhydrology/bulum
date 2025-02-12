import unittest
from bulum import trans
from bulum import utils
import pandas as pd
from datetime import datetime, timedelta

class Tests(unittest.TestCase):
    
    def make_test_df(self, dt, length, value):
        df = pd.DataFrame()
        df["Date"] = utils.get_dates(dt, days=length, str_format=r"%Y-%m-%d")
        df["Value"] = value
        df.set_index("Date",inplace=True)
        return df

    def test_join_on_date(self):
        df1 = self.make_test_df(datetime(2000,1,1),30,101)
        df2 = self.make_test_df(datetime(2000,1,4),30,102)
        df3 = trans.join_on_dates(df1, df2)
        self.assertTrue(len(df3),33)
