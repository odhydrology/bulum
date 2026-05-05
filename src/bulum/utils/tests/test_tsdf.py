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

    def test_create_tsdf(self):
        df = bio.read("./src/bulum/io/tests/test_data.csv")
        tsdf = utils.TimeseriesDataframe.from_dataframe(df)
        self.assertIsInstance(tsdf, utils.TimeseriesDataframe)
        self.assertEqual(tsdf.tags, "")
        self.assertEqual(tsdf.count_tags(), 0)

        tsdf.add_tag("tag")
        self.assertTrue(tsdf.has_tag("tag"))
        self.assertEqual(tsdf.count_tags(), 1)

    def test_map(self):
        filename = "./src/bulum/utils/tests/test_data.csv"
        df = bio.read(filename)
        tsdf = utils.TimeseriesDataframe.from_dataframe(df)
        tsdf.add_tag("tag")

        def internal_fn(df):
            return df.map(np.mean)

        new_tsdf = tsdf.tsdf_apply(internal_fn)
        self.assertIsInstance(new_tsdf, utils.TimeseriesDataframe)
        self.assertTrue(new_tsdf.has_tag("tag"))
        self.assertEqual(new_tsdf.tags, "tag")
        self.assertEqual(new_tsdf.count_tags(), 1)

    def test_constructor_propagates_metadata(self):
        """Test that _constructor ensures pandas operations return TimeseriesDataframe."""
        filename = "./src/bulum/utils/tests/test_data.csv"
        df = bio.read(filename)
        tsdf = utils.TimeseriesDataframe.from_dataframe(df, name="test", source="test_source")
        tsdf.add_tag("tag1,tag2")

        # Direct pandas operations should now return TimeseriesDataframe
        result = tsdf.map(np.mean)
        self.assertIsInstance(result, utils.TimeseriesDataframe)
        self.assertEqual(result.tags, "tag1,tag2")
        self.assertEqual(result.name, "test")
        self.assertEqual(result.source, "test_source")

        # Slicing should also preserve type and metadata
        sliced = tsdf.iloc[:1]
        self.assertIsInstance(sliced, utils.TimeseriesDataframe)
        self.assertEqual(sliced.tags, "tag1,tag2")

        # Copy should preserve metadata
        copied = tsdf.copy()
        self.assertIsInstance(copied, utils.TimeseriesDataframe)
        self.assertEqual(copied.tags, "tag1,tag2")
        self.assertEqual(copied.name, "test")


class TestMetadataPropagation(unittest.TestCase):
    """Comprehensive tests for _metadata and _constructor behavior."""

    def setUp(self):
        """Create a TSDF with all metadata fields populated."""
        df = pd.DataFrame(
            {"A": [1.0, 2.0, 3.0], "B": [4.0, 5.0, 6.0]},
            index=pd.date_range("2020-01-01", periods=3)
        )
        self.tsdf = utils.TimeseriesDataframe.from_dataframe(
            df, name="test_name", source="test_source", description="test_desc"
        )
        self.tsdf.add_tag("tag1,tag2")

    def _assert_full_metadata(self, tsdf, msg=""):
        """Helper to assert all metadata fields are preserved."""
        self.assertIsInstance(tsdf, utils.TimeseriesDataframe, msg)
        self.assertEqual(tsdf.name, "test_name", msg)
        self.assertEqual(tsdf.source, "test_source", msg)
        self.assertEqual(tsdf.description, "test_desc", msg)
        self.assertEqual(tsdf.tags, "tag1,tag2", msg)

    def test_all_metadata_fields_preserved(self):
        """Verify all four metadata fields exist and persist."""
        self._assert_full_metadata(self.tsdf)

    def test_copy_preserves_metadata(self):
        """Test that .copy() preserves type and all metadata."""
        copied = self.tsdf.copy()
        self._assert_full_metadata(copied, "copy() failed")
        # Ensure it's a true copy (not same object)
        self.assertIsNot(copied, self.tsdf)

    def test_slicing_preserves_metadata(self):
        """Test that slicing operations preserve type and metadata."""
        # Row slicing
        row_slice = self.tsdf.iloc[:2]
        self._assert_full_metadata(row_slice, "iloc row slicing failed")

        # Column slicing (single column)
        col_slice = self.tsdf[["A"]]
        self._assert_full_metadata(col_slice, "column slicing failed")

        # Boolean indexing
        bool_slice = self.tsdf[self.tsdf["A"] > 1.5]
        self._assert_full_metadata(bool_slice, "boolean indexing failed")

    def test_arithmetic_operations_preserve_metadata(self):
        """Test that arithmetic operations preserve type and metadata."""
        result = self.tsdf + 10
        self._assert_full_metadata(result, "addition failed")

        result = self.tsdf * 2
        self._assert_full_metadata(result, "multiplication failed")

        result = self.tsdf - self.tsdf.mean()
        self._assert_full_metadata(result, "subtraction failed")

    def test_statistical_operations_preserve_metadata(self):
        """Test operations that return DataFrames of same shape."""
        # Rank operation uses _constructor and preserves metadata
        result = self.tsdf.rank()
        self._assert_full_metadata(result, "rank failed")

    def test_rolling_operations_lose_metadata(self):
        """Document that rolling operations don't preserve metadata (known limitation)."""
        # Rolling window operations go through a different code path that doesn't
        # use _constructor properly, so metadata attributes are not even initialized.
        # This is a pandas limitation with how Rolling objects create results.
        result = self.tsdf.rolling(window=2).mean()
        self.assertIsInstance(result, utils.TimeseriesDataframe)
        # Metadata attributes are not even initialized in rolling operations
        self.assertFalse(hasattr(result, 'name'))
        self.assertFalse(hasattr(result, 'tags'))

    def test_fillna_preserves_metadata(self):
        """Test that fillna preserves type and metadata."""
        tsdf_with_nan = self.tsdf.copy()
        tsdf_with_nan.iloc[0, 0] = np.nan
        filled = tsdf_with_nan.fillna(0)
        self._assert_full_metadata(filled, "fillna failed")

    def test_reset_index_preserves_metadata(self):
        """Test that reset_index preserves metadata."""
        result = self.tsdf.reset_index(drop=True)
        self._assert_full_metadata(result, "reset_index failed")

    def test_transpose_preserves_metadata(self):
        """Test that transpose preserves metadata."""
        result = self.tsdf.T
        self._assert_full_metadata(result, "transpose failed")

    def test_apply_preserves_metadata(self):
        """Test that apply operations preserve metadata."""
        result = self.tsdf.apply(lambda x: x * 2)
        self._assert_full_metadata(result, "apply failed")

    def test_direct_instantiation_with_data(self):
        """Test that TimeseriesDataframe can be instantiated with data directly."""
        data = {"X": [1, 2, 3], "Y": [4, 5, 6]}
        tsdf = utils.TimeseriesDataframe(
            data, name="direct", source="test", description="desc"
        )
        self.assertIsInstance(tsdf, utils.TimeseriesDataframe)
        self.assertEqual(tsdf.name, "direct")
        self.assertEqual(tsdf.source, "test")
        self.assertEqual(tsdf.description, "desc")
        self.assertEqual(tsdf.tags, "")
        self.assertEqual(list(tsdf.columns), ["X", "Y"])

    def test_empty_dataframe_preserves_metadata(self):
        """Test that operations on empty DataFrames preserve metadata."""
        empty_tsdf = utils.TimeseriesDataframe(
            name="empty", source="test", description="empty"
        )
        empty_tsdf.add_tag("empty_tag")

        copied = empty_tsdf.copy()
        self.assertIsInstance(copied, utils.TimeseriesDataframe)
        self.assertEqual(copied.name, "empty")
        self.assertEqual(copied.tags, "empty_tag")

    def test_concat_loses_metadata(self):
        """Document that pd.concat doesn't preserve metadata (known limitation)."""
        # pd.concat uses pandas internals that bypass _constructor, so metadata
        # is not preserved. This is a pandas limitation.
        tsdf2 = utils.TimeseriesDataframe.from_dataframe(
            pd.DataFrame({"A": [7.0], "B": [8.0]},
                        index=pd.date_range("2020-01-04", periods=1))
        )

        result = pd.concat([self.tsdf, tsdf2])
        # Type is preserved, but metadata is lost
        self.assertIsInstance(result, utils.TimeseriesDataframe)
        self.assertEqual(result.name, "")
        self.assertEqual(result.tags, "")

    def test_metadata_independence_after_copy(self):
        """Test that metadata changes don't affect the original after copy."""
        copied = self.tsdf.copy()
        copied.name = "modified"
        copied.add_tag("new_tag")

        # Original should be unchanged
        self.assertEqual(self.tsdf.name, "test_name")
        self.assertEqual(self.tsdf.tags, "tag1,tag2")

        # Copy should have new values
        self.assertEqual(copied.name, "modified")
        self.assertEqual(copied.tags, "tag1,tag2,new_tag")

    def test_pickle_round_trip_preserves_metadata(self):
        """Test that pickling/unpickling preserves metadata."""
        import pickle

        pickled = pickle.dumps(self.tsdf)
        unpickled = pickle.loads(pickled)

        self._assert_full_metadata(unpickled, "pickle round-trip failed")
        # Verify data is also preserved
        pd.testing.assert_frame_equal(self.tsdf, unpickled)


if __name__ == '__main__':
    unittest.main()
