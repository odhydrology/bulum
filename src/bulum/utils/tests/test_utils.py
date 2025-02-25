import unittest
import pandas as pd
from bulum import utils
import bulum.io as bio
from datetime import datetime


class Tests(unittest.TestCase):

    def test_meets_ts_standards_1(self):
        df = pd.read_csv("./src/bulum/utils/tests/test_data.csv")
        violations = utils.check_df_format_standards(df)
        # There should be one violation because the index are still integers
        self.assertEqual(violations, ["Dataframe index name is not 'Date'"])

    def test_meets_ts_standards_2(self):
        df = pd.read_csv("./src/bulum/utils/tests/test_data.csv")
        df["Date"] = pd.to_datetime(df["Date"])
        df.set_index("Date", inplace=True)
        df.index.name = "date" # Change to lowercase to cause violation
        violations = utils.check_df_format_standards(df)        
        self.assertEqual(violations, ["Dataframe index name is not 'Date'"])

    def test_meets_ts_standards_3(self):
        df = pd.read_csv("./src/bulum/utils/tests/test_data.csv")
        df["Date"] = utils.standardize_datestring_format(df["Date"].values)        
        df.set_index("Date", inplace=True)
        # There should be nothing wrong
        violations = utils.check_df_format_standards(df)        
        self.assertEqual(violations, [])

    def test_meets_ts_standards_4(self):
        df = pd.read_csv("./src/bulum/utils/tests/test_data_missing.csv")
        df["Date"] = utils.standardize_datestring_format(df["Date"].values)
        df.set_index("Date", inplace=True)
        # The missing value will be read as a string, causing the column to have type "object"
        violations = utils.check_df_format_standards(df)        
        self.assertEqual(violations, [f"Column 'col_1' is not int64 or float64: object"])

    def test_generate_dates(self):
        dates = utils.get_dates(datetime(2000,1,1), datetime(2020,1,4))
        n = 7308
        self.assertEqual(len(dates), n)
        self.assertEqual(dates[0],datetime(2000,1,1))
        self.assertEqual(dates[n-1],datetime(2020,1,3))

    def test_generate_string_dates(self):
        date_strings = utils.get_dates('2000-01-01', '2020-01-04') #it should automatically determine the string format. str_format=r"%Y-%m-%d"
        n = 7308
        self.assertEqual(len(date_strings), n)
        self.assertEqual(date_strings[0],'2000-01-01')
        self.assertEqual(date_strings[n-1],'2020-01-03')

    def test_wy(self):
        dates = utils.get_dates(datetime(2000,1,1), datetime(2020,1,4), str_format=r"%Y-%m-%d")
        wy = utils.get_wy(dates)                #default conventions are wy_month=7 and using_end_year=False
        self.assertEqual(len(dates), len(wy))
        self.assertEqual(wy[0], 1999)           #with a default wy_month (=7), the WY on the first date should be 1999
        self.assertEqual(wy[len(wy) - 1], 2019) #with a default wy_month (=7), the WY on the last date should be 2019
        wy = utils.get_wy(dates, wy_month=1)
        self.assertEqual(wy[len(wy) - 1], 2020) #with the above custom wy_month=1, the WY on the last date should be 2020
        wy = utils.get_wy(dates, wy_month=1, using_end_year=True)
        self.assertEqual(wy[len(wy) - 1], 2021) #with the above custom wy_month=1, and the fiscal conventions (using_end_year=True), the WY on the last date should be 2021

    def test_set_index_dt(self):
        df = pd.DataFrame()
        df["Date"] = utils.get_dates(datetime(2000,1,1), datetime(2000,1,8))
        df["y1"] = 1
        df["y2"] = 2
        ans = utils.set_index_dt(df)
        self.assertEqual(len(ans), 7)
        self.assertEqual(len(ans.columns), 2) #"Date" is converted to the index thus we only have 2 columns left
        self.assertFalse("Date" in ans.columns)

    def test_set_index_dt_lowercase_whitespace(self):
        df = pd.DataFrame()
        df["date "] = utils.get_dates(datetime(2000,1,1), datetime(2000,1,8))
        df["y1"] = 1
        df["y2"] = 2
        ans = utils.set_index_dt(df)
        self.assertEqual(len(ans), 7)
        self.assertEqual(len(ans.columns), 2) #"date " is converted to the index "Date" thus we only have 2 columns left
        self.assertFalse("Date" in ans.columns)

    # TODO: this is a test for future functionality. I want to automatically attempt to parse
    #       the first column as dates, if no column can be found by name.
    #
    # def test_set_index_dt_first_colum_are_dates_but_name_is_weird(self):
    #     df = pd.DataFrame()
    #     df["year-month-day"] = utils.get_dates(datetime(2000,1,1), datetime(2000,1,8))
    #     df["y1"] = 1
    #     df["y2"] = 2
    #     ans = utils.set_index_dt(df)
    #     self.assertEqual(len(ans), 7)
    #     self.assertEqual(len(ans.columns), 3) #"date " is converted to the index "Date" thus we only have 2 columns left
    #     self.assertFalse("Date" in ans.columns)

    def test_set_index_dt_ignored(self):
        df = pd.DataFrame()
        df["Date"] = utils.get_dates(datetime(2004,1,1), datetime(2004,1,8))
        df["date"] = utils.get_dates(datetime(2001,1,1), datetime(2001,1,8))
        df["y1"] = 1
        df["y2"] = 2
        ans = utils.set_index_dt(df)
        self.assertEqual(len(ans.columns), 3)
        self.assertTrue("date" in ans.columns)       #"date" should still be a column
        self.assertFalse("Date" in ans.columns)      #but "Date" (the index) should not be a column
        self.assertTrue(min(ans.index).year == 2004) #Make sure the index dates are in 2004, not 2001
        ans2 = utils.set_index_dt(ans)
        self.assertTrue(set(ans.columns) == set(ans2.columns))

    def test_set_index_dt_start_dt(self):
        df = pd.DataFrame()
        df["y1"] = [i for i in range(9)]
        df["y2"] = 2
        ans = utils.set_index_dt(df, start_dt=datetime(2000,1,1))
        self.assertEqual(len(ans.columns), 2)
        self.assertFalse("Date" in ans.columns)
        self.assertEqual(max(ans.index), datetime(2000,1,9))

    def test_set_index_dt_values(self):
        df = pd.DataFrame()
        df["y1"] = [i for i in range(9)]
        df["y2"] = 2
        ans = utils.set_index_dt(df, dt_values=utils.get_dates(datetime(2000,1,1),days=999))
        self.assertEqual(len(ans.columns), 2)
        self.assertFalse("Date" in ans.columns)
        self.assertEqual(max(ans.index), datetime(2000,1,9))

    def test_check_data_equivalence_true(self):
        df = pd.read_csv("./src/bulum/utils/tests/test_data.csv")
        df["Date"] = pd.to_datetime(df["Date"], format=r"%d/%m/%Y")
        df.set_index("Date", inplace=True)
        df2 = df.copy(deep=True)
        self.assertTrue(utils.check_data_equivalence(df, df2))
        self.assertFalse(utils.check_data_equivalence(df, df2 * 1.00001))

    def test_wy_start_date(self):
        df = bio.read_ts_csv("./src/bulum/stats/tests/test_div_data.csv",r"%Y-%m-%d")
        start_jul=utils.get_wy_start_date(df)
        start_jan=utils.get_wy_start_date(df,1)
        self.assertEqual(start_jul,datetime(1889,7,1))
        self.assertEqual(start_jan,datetime(1890,1,1))

    def test_wy_end_date(self):
        df = bio.read_ts_csv("./src/bulum/stats/tests/test_div_data.csv",r"%Y-%m-%d")
        start_jul=utils.get_wy_end_date(df)
        start_mar=utils.get_wy_end_date(df,3)
        self.assertEqual(start_jul,datetime(1919,6,30))
        self.assertEqual(start_mar,datetime(1920,2,29))
