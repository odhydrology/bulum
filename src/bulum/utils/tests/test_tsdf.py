"""
This file contains tests for the DataframeEnsemble class from bulum.utils.dataframe_extensions.
"""

import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

import bulum.io as bio
from bulum import utils

# pylint: disable=missing-class-docstring, missing-function-docstring, protected-access

class TestTSDF(unittest.TestCase):

    def setUp(self):
        df = bio.read("./src/bulum/io/tests/test_data.csv")
        self.tsdf = utils.TimeseriesDataframe.from_dataframe(df, 
                                                             name="test", 
                                                             source="test_source")

    def test_create_tsdf(self):
        self.assertIsInstance(self.tsdf, utils.TimeseriesDataframe)
        self.assertEqual(self.tsdf.tags, "")
        self.assertEqual(self.tsdf.count_tags(), 0)

        self.tsdf.add_tag("tag")
        self.assertTrue(self.tsdf.has_tag("tag"))
        self.assertEqual(self.tsdf.count_tags(), 1)

    def test_map(self):
        self.tsdf.add_tag("tag")

        def internal_fn(df):
            return df.map(np.mean)

        new_tsdf = self.tsdf.tsdf_apply(internal_fn)
        self.assertIsInstance(new_tsdf, utils.TimeseriesDataframe)
        self.assertTrue(new_tsdf.has_tag("tag"))
        self.assertEqual(new_tsdf.tags, "tag")
        self.assertEqual(new_tsdf.count_tags(), 1)

    def test_constructor_propagates_metadata(self):
        """Test that _constructor ensures pandas operations return TimeseriesDataframe."""
        self.tsdf.add_tag("tag1,tag2")

        # Direct pandas operations should now return TimeseriesDataframe
        result = self.tsdf.map(np.mean)
        self.assertIsInstance(result, utils.TimeseriesDataframe)
        self.assertEqual(result.tags, "tag1,tag2")
        self.assertEqual(result.name, "test")
        self.assertEqual(result.source, "test_source")

        # Slicing should also preserve type and metadata
        sliced = self.tsdf.iloc[:1]
        self.assertIsInstance(sliced, utils.TimeseriesDataframe)
        self.assertEqual(sliced.tags, "tag1,tag2")

        # Copy should preserve metadata
        copied = self.tsdf.copy()
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


class TestSerialise(unittest.TestCase):
    """Tests for TimeseriesDataframe.save() and .load() methods."""

    def setUp(self):
        """Create a TSDF with all metadata fields populated."""
        df = pd.DataFrame(
            {"A": [1.0, 2.0, 3.0], "B": [4.0, 5.0, 6.0]},
            index=["r1", "r2", "r3"]
        )
        self.tsdf = utils.TimeseriesDataframe.from_dataframe(
            df, name="test_save", source="test_source.csv", description="Test save/load"
        )
        self.tsdf.add_tag("tag1,tag2,tag3")

    def test_json_round_trip(self):
        """Test saving and loading as JSON preserves all data and metadata."""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "test"
            self.tsdf.save(path, save_format="json")

            # Verify file was created
            self.assertTrue((Path(tmp) / "test.json").exists())

            # Load and verify
            loaded = utils.TimeseriesDataframe.load(Path(tmp) / "test.json")
            self.assertIsInstance(loaded, utils.TimeseriesDataframe)
            self.assertEqual(loaded.name, "test_save")
            self.assertEqual(loaded.source, "test_source.csv")
            self.assertEqual(loaded.description, "Test save/load")
            self.assertEqual(loaded.tags, "tag1,tag2,tag3")
            pd.testing.assert_frame_equal(self.tsdf, loaded)

    def test_csv_round_trip(self):
        """Test saving and loading as CSV+metadata preserves all data and metadata."""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "test"
            self.tsdf.save(path, save_format="csv")

            # Verify files were created
            self.assertTrue((Path(tmp) / "test.csv").exists())
            self.assertTrue((Path(tmp) / "test.metadata.json").exists())

            # Load and verify
            loaded = utils.TimeseriesDataframe.load(Path(tmp) / "test.csv")
            self.assertIsInstance(loaded, utils.TimeseriesDataframe)
            self.assertEqual(loaded.name, "test_save")
            self.assertEqual(loaded.source, "test_source.csv")
            self.assertEqual(loaded.description, "Test save/load")
            self.assertEqual(loaded.tags, "tag1,tag2,tag3")
            pd.testing.assert_frame_equal(self.tsdf, loaded)

    def test_csv_without_metadata(self):
        """Test that loading CSV without metadata file raises FileNotFoundError."""
        with tempfile.TemporaryDirectory() as tmp:
            # Save just the CSV without metadata
            csv_path = Path(tmp) / "test.csv"
            self.tsdf.to_csv(csv_path)

            # Should raise FileNotFoundError with helpful message
            with self.assertRaises(FileNotFoundError) as cm:
                utils.TimeseriesDataframe.load(csv_path)

            # Verify error message is helpful
            error_msg = str(cm.exception)
            self.assertIn("Metadata file not found", error_msg)
            self.assertIn("bulum.io.read", error_msg)

    def test_overwrite_false_raises(self):
        """Test that overwrite=False raises when file exists."""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "test"
            self.tsdf.save(path, save_format="json")
            self.assertRaises(FileExistsError, self.tsdf.save, path, "json", overwrite=False)

    def test_overwrite_true(self):
        """Test that overwrite=True allows overwriting existing files."""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "test"
            self.tsdf.save(path, save_format="json")
            # Should not raise
            self.tsdf.save(path, save_format="json", overwrite=True)

    def test_format_override(self):
        """Test that format_override works for non-standard extensions."""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "test.dat"
            self.tsdf.save(path.with_suffix(''), save_format="json")
            path.with_suffix('.json').rename(path)  # Rename to .dat

            # Load with format override
            loaded = utils.TimeseriesDataframe.load(path, format_override="json")
            self.assertEqual(loaded.name, "test_save")
            pd.testing.assert_frame_equal(self.tsdf, loaded)

    def test_invalid_format_raises(self):
        """Test that invalid format raises ValueError."""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "test"
            self.assertRaises(ValueError, self.tsdf.save, path, save_format="invalid")

    def test_unknown_extension_raises(self):
        """Test that unknown extension without format_override raises."""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "test.xyz"
            self.assertRaises(ValueError, utils.TimeseriesDataframe.load, path)

    # --- Error Handling Tests ---

    def test_json_malformed_file(self):
        """Test that loading malformed JSON raises ValueError with helpful message."""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "malformed.json"
            with open(path, 'w') as f:
                f.write("{invalid json syntax")

            with self.assertRaises(ValueError) as cm:
                utils.TimeseriesDataframe.load(path)

            error_msg = str(cm.exception)
            self.assertIn("Failed to parse JSON file", error_msg)
            self.assertIn(str(path), error_msg)

    def test_json_wrong_type(self):
        """Test that loading JSON with wrong __type__ raises ValueError."""
        import json
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "wrong_type.json"
            with open(path, 'w') as f:
                json.dump({"__type__": "SomeOtherClass", "version": 1, "data": {}}, f)

            with self.assertRaises(ValueError) as cm:
                utils.TimeseriesDataframe.load(path)

            error_msg = str(cm.exception)
            self.assertIn("does not contain a TimeseriesDataframe", error_msg)

    def test_json_unsupported_version(self):
        """Test that loading JSON with unsupported version raises ValueError."""
        import json
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "future_version.json"
            # Create valid TSDF structure but with future version
            with open(path, 'w') as f:
                json.dump({
                    "__type__": "TimeseriesDataframe",
                    "version": 999,
                    "data": {"columns": ["A"], "index": [0], "data": [[1.0]]},
                    "metadata": {"name": "", "source": "", "description": "", "tags": ""}
                }, f)

            with self.assertRaises(ValueError) as cm:
                utils.TimeseriesDataframe.load(path)

            error_msg = str(cm.exception)
            self.assertIn("Unsupported TimeseriesDataframe version", error_msg)
            self.assertIn("999", error_msg)

    def test_csv_metadata_malformed_json(self):
        """Test that malformed metadata JSON raises ValueError."""
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "test.csv"
            metadata_path = Path(tmp) / "test.metadata.json"

            self.tsdf.to_csv(csv_path)
            with open(metadata_path, 'w') as f:
                f.write("{malformed json")

            with self.assertRaises(ValueError) as cm:
                utils.TimeseriesDataframe.load(csv_path)

            error_msg = str(cm.exception)
            self.assertIn("Failed to parse metadata file", error_msg)
            self.assertIn(str(metadata_path), error_msg)

    def test_csv_metadata_wrong_type(self):
        """Test that metadata with wrong __type__ raises ValueError."""
        import json
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "test.csv"
            metadata_path = Path(tmp) / "test.metadata.json"

            self.tsdf.to_csv(csv_path)
            with open(metadata_path, 'w') as f:
                json.dump({"__type__": "DataframeEnsemble", "version": 1}, f)

            with self.assertRaises(ValueError) as cm:
                utils.TimeseriesDataframe.load(csv_path)

            error_msg = str(cm.exception)
            self.assertIn("not for a TimeseriesDataframe", error_msg)
            self.assertIn("DataframeEnsemble", error_msg)

    def test_csv_metadata_wrong_version(self):
        """Test that metadata with unsupported version raises ValueError."""
        import json
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "test.csv"
            metadata_path = Path(tmp) / "test.metadata.json"

            self.tsdf.to_csv(csv_path)
            with open(metadata_path, 'w') as f:
                json.dump({
                    "__type__": "TimeseriesDataframe",
                    "version": 42,
                    "name": "", "source": "", "description": "", "tags": ""
                }, f)

            with self.assertRaises(ValueError) as cm:
                utils.TimeseriesDataframe.load(csv_path)

            error_msg = str(cm.exception)
            self.assertIn("unsupported version", error_msg)
            self.assertIn("42", error_msg)

    def test_csv_metadata_non_dict_structure(self):
        """Test that metadata as JSON array instead of object raises ValueError."""
        import json
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "test.csv"
            metadata_path = Path(tmp) / "test.metadata.json"

            self.tsdf.to_csv(csv_path)
            with open(metadata_path, 'w') as f:
                json.dump(["not", "a", "dict"], f)

            with self.assertRaises(ValueError) as cm:
                utils.TimeseriesDataframe.load(csv_path)

            error_msg = str(cm.exception)
            self.assertIn("invalid structure", error_msg)
            self.assertIn("expected a JSON object", error_msg)

    def test_csv_metadata_non_string_field(self):
        """Test that non-string metadata field values raise ValueError."""
        import json
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "test.csv"
            metadata_path = Path(tmp) / "test.metadata.json"

            self.tsdf.to_csv(csv_path)
            with open(metadata_path, 'w') as f:
                json.dump({
                    "__type__": "TimeseriesDataframe",
                    "version": 1,
                    "name": 123,  # Should be string
                    "source": "", "description": "", "tags": ""
                }, f)

            with self.assertRaises(ValueError) as cm:
                utils.TimeseriesDataframe.load(csv_path)

            error_msg = str(cm.exception)
            self.assertIn("must be a string", error_msg)
            self.assertIn("name", error_msg)

    def test_csv_file_missing(self):
        """Test that loading non-existent CSV file raises FileNotFoundError."""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "nonexistent.csv"

            with self.assertRaises(FileNotFoundError) as cm:
                utils.TimeseriesDataframe.load(path)

            error_msg = str(cm.exception)
            self.assertIn("CSV file not found", error_msg)

    def test_empty_tsdf_serialization(self):
        """Test that empty TSDF can be saved and loaded."""
        empty_tsdf = utils.TimeseriesDataframe(name="empty", source="test")

        with tempfile.TemporaryDirectory() as tmp:
            # JSON format
            json_path = Path(tmp) / "empty"
            empty_tsdf.save(json_path, save_format="json")
            loaded_json = utils.TimeseriesDataframe.load(Path(tmp) / "empty.json")
            self.assertEqual(loaded_json.name, "empty")
            self.assertEqual(loaded_json.source, "test")
            self.assertEqual(len(loaded_json), 0)

            # CSV format
            csv_path = Path(tmp) / "empty_csv"
            empty_tsdf.save(csv_path, save_format="csv")
            loaded_csv = utils.TimeseriesDataframe.load(Path(tmp) / "empty_csv.csv")
            self.assertEqual(loaded_csv.name, "empty")
            self.assertEqual(loaded_csv.source, "test")
            self.assertEqual(len(loaded_csv), 0)

    def test_csv_overwrite_partial_collision(self):
        """Test overwrite when only metadata file exists."""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "test"

            # Create only metadata file (unusual but possible)
            metadata_path = Path(tmp) / "test.metadata.json"
            with open(metadata_path, 'w') as f:
                f.write("{}")

            # Should fail with overwrite=False
            with self.assertRaises(FileExistsError):
                self.tsdf.save(path, save_format="csv", overwrite=False)

            # Should succeed with overwrite=True
            self.tsdf.save(path, save_format="csv", overwrite=True)
            self.assertTrue((Path(tmp) / "test.csv").exists())
            self.assertTrue(metadata_path.exists())


if __name__ == '__main__':
    unittest.main()
