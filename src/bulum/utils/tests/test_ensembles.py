"""
This file contains tests for the DataframeEnsemble class from bulum.utils.dataframe_extensions.
"""

import re
import unittest

import numpy as np
import pandas as pd

import bulum.io as bio
from bulum import utils


class Tests(unittest.TestCase):

    def test_create_ensemble(self):
        ensemble = utils.DataframeEnsemble()
        for filename in ["./src/bulum/io/tests/test_data.csv",
                         "./src/bulum/io/tests/test_data.csv",
                         "./src/bulum/io/tests/test_data2.csv"]:
            ensemble.add_dataframe(bio.read(filename))
            # ensemble.add_dataframe_from_file(filename) //TODO: I have replaced this with above until we can unpick the cicrular import issue
        self.assertEqual(min(ensemble.ensemble.keys()), 0)
        self.assertEqual(max(ensemble.ensemble.keys()), 2)
        self.assertEqual(len(ensemble), 3)

    def test_create_ensemble2(self):
        ensemble = utils.DataframeEnsemble()
        for filename in ["./src/bulum/io/tests/test_data.csv",
                         "./src/bulum/io/tests/test_data2.csv"]:
            ensemble.add_dataframe(bio.read(filename), key=filename.split('/')[-1], tag="hist_clim")
            # ensemble.add_dataframe_from_file(filename, key=filename.split('/')[-1], tag="hist_clim") //TODO: I have replaced this with above until we can unpick the cicrular import issue
        # The below dataframe should not be the same shape.
        other_df = bio.read("./src/bulum/io/tests/modelled_flow.csv")
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

    def test_map(self):
        ensemble = utils.DataframeEnsemble()
        for filename in ["./src/bulum/utils/tests/test_data.csv"]:
            df = bio.read(filename)
            ensemble.add_dataframe(df)

        def internal_fn(df: pd.DataFrame) -> pd.DataFrame:
            return pd.DataFrame(np.mean(df.values, axis=0))

        new_ensemble = ensemble.map(internal_fn)
        for _, df in new_ensemble.ensemble.items():
            self.assertEqual(df.shape, (2, 1))
            self.assertAlmostEqual(df.iloc[0, 0], 1.5681824, places=6) # type: ignore
            self.assertAlmostEqual(df.iloc[1, 0], 0.5, places=6) # type: ignore

    def test_map_and_tsdf_apply(self):
        """Test ensemble.map with tsdf_map for operations that create new DataFrames."""
        ensemble = utils.DataframeEnsemble()
        df = bio.read("./src/bulum/utils/tests/test_data.csv")
        tsdf = utils.TimeseriesDataframe.from_dataframe(df)
        tsdf.add_tag("tag")
        ensemble.add_dataframe(tsdf)

        def compute_column_means(tsdf: utils.TimeseriesDataframe) -> utils.TimeseriesDataframe:
            # pd.DataFrame() creates a new DataFrame, bypassing _constructor
            # Use tsdf_map to preserve metadata
            return tsdf.tsdf_apply(lambda df: pd.DataFrame(np.mean(df.values, axis=0)))

        new_ensemble = ensemble.map(compute_column_means)
        for _, df in new_ensemble.ensemble.items():
            self.assertEqual(df.shape, (2, 1))
            self.assertAlmostEqual(df.iloc[0, 0], 1.5681824, places=6) # type: ignore
            self.assertAlmostEqual(df.iloc[1, 0], 0.5, places=6) # type: ignore
            self.assertTrue(df.has_tag("tag"))

    def test_map_with_automatic_metadata_preservation(self):
        """Test ensemble.map with operations that automatically preserve metadata."""
        ensemble = utils.DataframeEnsemble()
        df = bio.read("./src/bulum/utils/tests/test_data.csv")
        tsdf = utils.TimeseriesDataframe.from_dataframe(df, name="original", source="test.csv")
        tsdf.add_tag("validated,processed")
        ensemble.add_dataframe(tsdf)

        def double_values(tsdf: utils.TimeseriesDataframe) -> utils.TimeseriesDataframe:
            # With _constructor, arithmetic operations preserve metadata automatically
            return tsdf * 2

        new_ensemble = ensemble.map(double_values)
        for _, df in new_ensemble.ensemble.items():
            # Verify metadata was preserved
            self.assertIsInstance(df, utils.TimeseriesDataframe)
            self.assertEqual(df.name, "original")
            self.assertEqual(df.source, "test.csv")
            self.assertTrue(df.has_tag("validated"))
            self.assertTrue(df.has_tag("processed"))
            # Verify transformation was applied
            self.assertEqual(df.shape, (10, 2))
            self.assertAlmostEqual(df.iloc[0, 0], 2.267945, places=5) # type: ignore

if __name__ == '__main__':
    unittest.main()
