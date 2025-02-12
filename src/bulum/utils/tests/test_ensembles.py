import unittest
from bulum import utils
import bulum.io as oio
import re


class Tests(unittest.TestCase):

    def test_create_ensemble(self):
        ensemble = utils.DataframeEnsemble()
        for filename in ["./src/bulum/io/tests/test_data.csv",
                         "./src/bulum/io/tests/test_data.csv",
                         "./src/bulum/io/tests/test_data2.csv"]:
            ensemble.add_dataframe(oio.read(filename))
            #ensemble.add_dataframe_from_file(filename) //TODO: I have replaced this with above until we can unpick the cicrular import issue
        self.assertEqual(min(ensemble.ensemble.keys()), 0)
        self.assertEqual(max(ensemble.ensemble.keys()), 2)
        self.assertEqual(len(ensemble), 3)

    def test_create_ensemble2(self):
        ensemble = utils.DataframeEnsemble()
        for filename in ["./src/bulum/io/tests/test_data.csv",
                         "./src/bulum/io/tests/test_data2.csv"]:
            ensemble.add_dataframe(oio.read(filename), key=filename.split('/')[-1], tag="hist_clim")
            #ensemble.add_dataframe_from_file(filename, key=filename.split('/')[-1], tag="hist_clim") //TODO: I have replaced this with above until we can unpick the cicrular import issue
        # The below dataframe should not be the same shape.
        other_df = oio.read("./src/bulum/io/tests/modelled_flow.csv")
        self.assertFalse(ensemble.df_shape_matches_ensemble(other_df))
        # Assert raise exception
        # ensemble.add_dataframe("whatever", other_df)
        self.assertRaises(Exception, ensemble.add_dataframe,
                          "whatever", other_df)

    # def test_create_ensemble_from_files(self):
    #     ensemble = utils.DataframeEnsemble.from_files([
    #         "./src/bulum/io/tests/test_data.csv",
    #         "./src/bulum/io/tests/test_data.csv",
    #         "./src/bulum/io/tests/test_data2.csv"
    #     ])
    #     self.assertEqual(min(ensemble.ensemble.keys()), 0)
    #     self.assertEqual(max(ensemble.ensemble.keys()), 2)

    # def test_create_ensemble_from_iterable(self):
    #     l = []
    #     for file in ["./src/bulum/io/tests/test_data.csv",
    #                  "./src/bulum/io/tests/test_data.csv",
    #                  "./src/bulum/io/tests/test_data2.csv"]:
    #         l.append(utils.TimeseriesDataframe.from_file(file))
    #     ensemble = utils.DataframeEnsemble(l)
    #     self.assertEqual(min(ensemble.ensemble.keys()), 0)
    #     self.assertEqual(max(ensemble.ensemble.keys()), 2)

    def test_add_tag(self):
        """Testing stripping"""
        df1 = utils.TimeseriesDataframe()
        df2 = utils.TimeseriesDataframe()
        df1.add_tag("tag")
        df2.add_tag("tag ")
        self.assertEqual(df1.tags, df2.tags)
        self.assertEqual(df1.count_tags(), 1)

    def test_add_tag2(self):
        df = utils.TimeseriesDataframe()
        df.add_tag("tagged", True)
        self.assertRaises(ValueError, df.add_tag, "tag", True)

    def test_has_tag(self):
        df = utils.TimeseriesDataframe()
        df.add_tag("tag")
        df.add_tag("ABC01")
        df.add_tag("DEF01a")
        self.assertFalse(df.has_tag("z"))
        self.assertTrue(df.has_tag("ABC"))
        self.assertTrue(df.has_tag("ABC01"))
        self.assertFalse(df.has_tag("ABC", exact=True))
        self.assertTrue(df.has_tag("ABC01", exact=True))

    def test_has_tag_regex(self):
        df = utils.TimeseriesDataframe()
        df.add_tag("ABC01")
        df.add_tag("DEF01a")
        pattern = r"ABC[0-9]1"
        re_object = re.compile(pattern)
        self.assertTrue(df.has_tag(pattern, regex=utils.RegexArg.PATTERN))
        self.assertTrue(df.has_tag(re_object, regex=utils.RegexArg.OBJECT))
        self.assertTrue(df.has_tag(r"\bABC01\b", regex=utils.RegexArg.PATTERN))

    def test_ensemble_filter_tag(self):
        df1 = utils.TimeseriesDataframe()
        df2 = utils.TimeseriesDataframe()
        df1.add_tag("a")
        df1.add_tag("b")
        df2.add_tag("a")
        df2.add_tag("c")
        ensemble = utils.DataframeEnsemble([df1, df2])
        self.assertEqual(len(ensemble.filter_tag("a")), 2)
        self.assertEqual(len(ensemble.filter_tag("b")), 1)
