import pandas as pd
import unittest
from datetime import datetime
from bulum import stoch
from bulum import utils

class Tests(unittest.TestCase):
    
    def test_nothing(self):
        df = pd.DataFrame()
        df.index = utils.get_dates(datetime(2000, 5, 6), datetime(2008, 2, 5))
        df["Values"] = stoch.from_pattern(df.index, monthly_pattern=[20,40,20,40,20,40,20,40,20,40,20,40])
        self.assertAlmostEqual(df.iloc[-1, 0], 40/29)
