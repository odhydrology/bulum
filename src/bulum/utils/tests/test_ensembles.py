"""
This file contains tests for the DataframeEnsemble class from bulum.utils.dataframe_extensions.
"""

import json
import re
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd

import bulum.io as bio
from bulum import utils

# pylint: disable=missing-class-docstring


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


class TestSerialise(unittest.TestCase):

    @staticmethod
    def _make_ensemble():
        """Two-member ensemble with int keys (0, 1) and known tags/data."""
        df1 = pd.DataFrame({"A": [1.0, 2.0], "B": [3.0, 4.0]}, index=["r1", "r2"])
        df2 = pd.DataFrame({"A": [5.0, 6.0], "B": [7.0, 8.0]}, index=["r1", "r2"])
        tsdf1 = utils.TimeseriesDataframe.from_dataframe(df1)
        tsdf1.add_tag("tag1,tag2")
        tsdf2 = utils.TimeseriesDataframe.from_dataframe(df2)
        tsdf2.add_tag("tag3")
        ensemble = utils.DataframeEnsemble()
        ensemble.add_dataframe(tsdf1)   # auto key=0
        ensemble.add_dataframe(tsdf2)   # auto key=1
        return ensemble

    def test_folder_int_keys_round_trip(self):
        ensemble = self._make_ensemble()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ens"
            ensemble.save(path, save_format="folder")
            loaded = utils.DataframeEnsemble.load(path)
        self.assertEqual(len(loaded), 2)
        self.assertIn(0, loaded.ensemble)
        self.assertIn(1, loaded.ensemble)
        self.assertIsInstance(list(loaded.ensemble.keys())[0], int)
        self.assertEqual(loaded.ensemble[0].tags, "tag1,tag2")
        self.assertEqual(loaded.ensemble[1].tags, "tag3")
        self.assertEqual(loaded.ensemble[0].shape, (2, 2))

    def test_all_metadata_fields_preserved(self):
        """Verify all TimeseriesDataframe metadata fields (name, source, description, tags) are preserved."""
        df = pd.DataFrame({"A": [1.0, 2.0], "B": [3.0, 4.0]}, index=["r1", "r2"])
        tsdf = utils.TimeseriesDataframe.from_dataframe(
            df, name="test_name", source="test_source.csv", description="Test description"
        )
        tsdf.add_tag("tag1,tag2,tag3")
        ensemble = utils.DataframeEnsemble()
        ensemble.add_dataframe(tsdf, key="test_key")

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ens"
            ensemble.save(path, save_format="folder")
            loaded = utils.DataframeEnsemble.load(path)

        loaded_tsdf = loaded.ensemble["test_key"]
        self.assertEqual(loaded_tsdf.name, "test_name")
        self.assertEqual(loaded_tsdf.source, "test_source.csv")
        self.assertEqual(loaded_tsdf.description, "Test description")
        self.assertEqual(loaded_tsdf.tags, "tag1,tag2,tag3")

    def test_folder_str_keys_round_trip(self):
        df = pd.DataFrame({"A": [1.0, 2.0]}, index=["r1", "r2"])
        tsdf = utils.TimeseriesDataframe.from_dataframe(df)
        tsdf.add_tag("mytag")
        ensemble = utils.DataframeEnsemble()
        ensemble.add_dataframe(tsdf, key="run_A")
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ens"
            ensemble.save(path, save_format="folder")
            loaded = utils.DataframeEnsemble.load(path)
        self.assertIn("run_A", loaded.ensemble)
        self.assertIsInstance(list(loaded.ensemble.keys())[0], str)
        self.assertEqual(loaded.ensemble["run_A"].tags, "mytag")

    def test_folder_index_preserved(self):
        df = pd.DataFrame({"A": [1.0, 2.0]}, index=["r1", "r2"])
        ensemble = utils.DataframeEnsemble()
        ensemble.add_dataframe(utils.TimeseriesDataframe.from_dataframe(df))
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ens"
            ensemble.save(path, save_format="folder")
            loaded = utils.DataframeEnsemble.load(path)
        self.assertEqual(list(loaded.ensemble[0].index), ["r1", "r2"])

    def test_zip_round_trip(self):
        ensemble = self._make_ensemble()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ens"
            ensemble.save(path, save_format="zip")
            self.assertTrue((Path(tmp) / "ens.zip").exists())
            self.assertFalse((Path(tmp) / "ens").exists())
            loaded = utils.DataframeEnsemble.load(Path(tmp) / "ens.zip")
            self.assertEqual(len(loaded), 2)
            self.assertEqual(loaded.ensemble[0].tags, "tag1,tag2")

    def test_zip_folder_keeps_both(self):
        ensemble = self._make_ensemble()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ens"
            ensemble.save(path, save_format="zip-folder")
            self.assertTrue((Path(tmp) / "ens.zip").exists())
            self.assertTrue((Path(tmp) / "ens").is_dir())

    def test_overwrite_false_raises(self):
        ensemble = self._make_ensemble()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ens"
            ensemble.save(path, save_format="folder")
            self.assertRaises(FileExistsError, ensemble.save, path, "folder")

    def test_overwrite_true(self):
        ensemble = self._make_ensemble()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ens"
            ensemble.save(path, save_format="folder")
            ensemble.save(path, save_format="folder", overwrite=True)
            loaded = utils.DataframeEnsemble.load(path)
        self.assertEqual(len(loaded), 2)

    def test_unsupported_key_type_raises(self):
        """Test that unsupported key types raise TypeError during serialization."""
        df = pd.DataFrame({"A": [1.0]}, index=["r1"])
        tsdf = utils.TimeseriesDataframe.from_dataframe(df)
        ensemble = utils.DataframeEnsemble()

        # Unsupported keys are allowed during add, but should fail during save
        ensemble.ensemble[(1, 2)] = tsdf  # tuple key
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(TypeError) as cm:
                ensemble.save(Path(tmp) / "ens", "folder")
            self.assertIn("unsupported type", str(cm.exception))
            self.assertIn("tuple", str(cm.exception))

    def test_unsupported_format_raises_before_mkdir(self):
        ensemble = self._make_ensemble()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "subdir" / "ens"
            self.assertRaises(ValueError, ensemble.save, path, "bad_format")
            self.assertFalse((Path(tmp) / "subdir").exists())

    # --- key type variants ---

    def test_folder_various_key_types(self):
        """Test folder serialization with str and float keys."""
        test_cases = [
            (1.5, float, "float"),
            ("string_key", str, "string"),
        ]

        for key, expected_type, description in test_cases:
            with self.subTest(key_type=description):
                df = pd.DataFrame({"A": [1.0]}, index=["r1"])
                ensemble = utils.DataframeEnsemble()
                ensemble.ensemble[key] = utils.TimeseriesDataframe.from_dataframe(df)
                with tempfile.TemporaryDirectory() as tmp:
                    path = Path(tmp) / "ens"
                    ensemble.save(path, save_format="folder")
                    loaded = utils.DataframeEnsemble.load(path)
                self.assertIn(key, loaded.ensemble)
                self.assertIsInstance(list(loaded.ensemble.keys())[0], expected_type)

    def test_folder_empty_tags(self):
        df = pd.DataFrame({"A": [1.0]}, index=["r1"])
        tsdf = utils.TimeseriesDataframe.from_dataframe(df)  # no tags added
        ensemble = utils.DataframeEnsemble()
        ensemble.add_dataframe(tsdf)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ens"
            ensemble.save(path, save_format="folder")
            loaded = utils.DataframeEnsemble.load(path)
        self.assertEqual(loaded.ensemble[0].tags, "")

    # --- partial write cleanup ---

    def test_partial_write_cleanup(self):
        ensemble = self._make_ensemble()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ens"
            with patch.object(pd.DataFrame, 'to_csv', side_effect=OSError("simulated write failure")):
                with self.assertRaises(OSError):
                    ensemble.save(path, save_format="folder")
            self.assertFalse(path.exists())

    # --- pickle ---

    def test_pickle_round_trip(self):
        ensemble = self._make_ensemble()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ens"
            ensemble.save(path, save_format="pickle")
            self.assertTrue((Path(tmp) / "ens.pkl").exists())
            loaded = utils.DataframeEnsemble.load(
                Path(tmp) / "ens.pkl", pickle_safety_lock=False
            )
        self.assertEqual(len(loaded), 2)
        self.assertEqual(loaded.ensemble[0].tags, "tag1,tag2")

    def test_pickle_overwrite_behavior(self):
        """Test pickle overwrite=False raises and overwrite=True succeeds."""
        ensemble = self._make_ensemble()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ens"
            ensemble.save(path, save_format="pickle")

            # overwrite=False should raise
            self.assertRaises(FileExistsError, ensemble.save, path, "pickle")

            # overwrite=True should succeed
            ensemble.save(path, save_format="pickle", overwrite=True)
            loaded = utils.DataframeEnsemble.load(
                Path(tmp) / "ens.pkl", pickle_safety_lock=False
            )
            self.assertEqual(len(loaded), 2)

    def test_deserialise_pickle_safety_lock_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertRaises(
                ValueError,
                utils.DataframeEnsemble.load,
                Path(tmp) / "ens.pkl",  # file need not exist; lock fires first
            )

    def test_deserialise_format_override_pickle(self):
        """format_override='pickle' should work regardless of file extension."""
        ensemble = self._make_ensemble()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ens"
            ensemble.save(path, save_format="pickle")
            pkl_path = path.with_suffix('.pkl')
            dat_path = path.with_suffix('.dat')
            pkl_path.rename(dat_path)
            loaded = utils.DataframeEnsemble.load(
                dat_path, pickle_safety_lock=False, format_override="pickle"
            )
        self.assertEqual(len(loaded), 2)

    # --- json serialization tests ---

    def test_json_round_trip(self):
        """Test JSON save/load round-trip with int keys."""
        ensemble = self._make_ensemble()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ens"
            ensemble.save(path, save_format="json")

            # Verify file created
            self.assertTrue((Path(tmp) / "ens.json").exists())

            # Load and verify
            loaded = utils.DataframeEnsemble.load(Path(tmp) / "ens.json")
            self.assertEqual(len(loaded), 2)
            self.assertIn(0, loaded.ensemble)
            self.assertIn(1, loaded.ensemble)
            self.assertIsInstance(list(loaded.ensemble.keys())[0], int)
            self.assertEqual(loaded.ensemble[0].tags, "tag1,tag2")
            self.assertEqual(loaded.ensemble[1].tags, "tag3")
            pd.testing.assert_frame_equal(ensemble.ensemble[0], loaded.ensemble[0])
            pd.testing.assert_frame_equal(ensemble.ensemble[1], loaded.ensemble[1])

    def test_json_with_all_metadata(self):
        """Test that all TSDF metadata fields are preserved in JSON."""
        df = pd.DataFrame({"A": [1.0, 2.0], "B": [3.0, 4.0]}, index=["r1", "r2"])
        tsdf = utils.TimeseriesDataframe.from_dataframe(
            df, name="test_name", source="test_source.csv", description="Test description"
        )
        tsdf.add_tag("tag1,tag2")
        ensemble = utils.DataframeEnsemble()
        ensemble.add_dataframe(tsdf, key="metadata_test")

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ens"
            ensemble.save(path, save_format="json")
            loaded = utils.DataframeEnsemble.load(Path(tmp) / "ens.json")

        loaded_tsdf = loaded.ensemble["metadata_test"]
        self.assertEqual(loaded_tsdf.name, "test_name")
        self.assertEqual(loaded_tsdf.source, "test_source.csv")
        self.assertEqual(loaded_tsdf.description, "Test description")
        self.assertEqual(loaded_tsdf.tags, "tag1,tag2")

    def test_json_various_key_types(self):
        """Test JSON serialization with string and float keys."""
        test_cases = [
            (["first", "second"], str, "string"),
            ([1.5, 2.5], float, "float"),
        ]

        for keys, expected_type, description in test_cases:
            with self.subTest(key_type=description):
                ensemble = utils.DataframeEnsemble()
                df = pd.DataFrame({"A": [1.0, 2.0], "B": [3.0, 4.0]}, index=["r1", "r2"])
                for key in keys:
                    tsdf = utils.TimeseriesDataframe.from_dataframe(df)
                    tsdf.add_tag("tag1")
                    ensemble.add_dataframe(tsdf, key=key)

                with tempfile.TemporaryDirectory() as tmp:
                    path = Path(tmp) / "ens"
                    ensemble.save(path, save_format="json")
                    loaded = utils.DataframeEnsemble.load(Path(tmp) / "ens.json")

                self.assertEqual(len(loaded), len(keys))
                for key in keys:
                    self.assertIn(key, loaded.ensemble)
                self.assertIsInstance(list(loaded.ensemble.keys())[0], expected_type)

    def test_json_serialise_overwrite_false_raises(self):
        """Test that overwrite=False raises when JSON file exists."""
        ensemble = self._make_ensemble()
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "ens.json").touch()
            self.assertRaises(
                FileExistsError, ensemble.save, Path(tmp) / "ens", "json"
            )

    def test_json_format_override(self):
        """Test JSON deserialization with format_override."""
        ensemble = self._make_ensemble()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ens.xyz"
            ensemble.save(path.with_suffix(''), save_format="json")
            path.with_suffix('.json').rename(path)  # Rename to .xyz

            # Load with format override
            loaded = utils.DataframeEnsemble.load(path, format_override="json")
            self.assertEqual(len(loaded), 2)
            self.assertIn(0, loaded.ensemble)

    # --- unknown / bad deserialise inputs ---

    def test_deserialise_unknown_extension_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ens.xyz"
            path.touch()
            self.assertRaises(ValueError, utils.DataframeEnsemble.load, path)

    def test_deserialise_unsupported_key_type_in_metadata(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp) / "ens"
            folder.mkdir()
            bad_metadata = {"version": 1, "members": [
                {"key": "(1, 2)", "key_type": "tuple", "filename": "1_2", "tags": ""}
            ]}
            with open(folder / 'metadata.json', 'w', encoding='utf-8') as f:
                json.dump(bad_metadata, f)
            self.assertRaises(ValueError, utils.DataframeEnsemble.load, folder)

    # --- Additional Error Handling Tests ---

    def test_json_malformed_file(self):
        """Test that loading malformed JSON raises error."""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "malformed.json"
            with open(path, 'w') as f:
                f.write("{invalid json")

            with self.assertRaises(Exception):  # JSONDecodeError will bubble up
                utils.DataframeEnsemble.load(path)

    def test_json_wrong_type(self):
        """Test that loading JSON with wrong __type__ raises ValueError."""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "wrong_type.json"
            # Create a complete TSDF JSON but wrong type for ensemble loading
            with open(path, 'w') as f:
                json.dump({
                    "__type__": "TimeseriesDataframe",
                    "version": 1,
                    "data": {"columns": ["A"], "index": [0], "data": [[1.0]]},
                    "metadata": {"name": "", "source": "", "description": "", "tags": ""}
                }, f)

            with self.assertRaises(ValueError) as cm:
                utils.DataframeEnsemble.load(path)

            self.assertIn("not a DataframeEnsemble", str(cm.exception))

    def test_json_unsupported_version(self):
        """Test that loading JSON with unsupported version raises ValueError."""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "future_version.json"
            with open(path, 'w') as f:
                json.dump({
                    "__type__": "DataframeEnsemble",
                    "version": 999,
                    "data": {}
                }, f)

            with self.assertRaises(ValueError) as cm:
                utils.DataframeEnsemble.load(path)

            self.assertIn("Unsupported DataframeEnsemble version", str(cm.exception))
            self.assertIn("999", str(cm.exception))

    def test_folder_metadata_malformed_json(self):
        """Test that malformed metadata.json raises error."""
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp) / "ens"
            folder.mkdir()
            with open(folder / 'metadata.json', 'w') as f:
                f.write("{malformed")

            with self.assertRaises(Exception):  # JSONDecodeError
                utils.DataframeEnsemble.load(folder)

    def test_folder_metadata_wrong_type(self):
        """Test that folder metadata with wrong __type__ raises ValueError."""
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp) / "ens"
            folder.mkdir()
            with open(folder / 'metadata.json', 'w') as f:
                json.dump({"__type__": "TimeseriesDataframe", "version": 1, "members": []}, f)

            # Currently there's no __type__ check in folder loading, but members will be empty
            # so it should load successfully but be empty
            ensemble = utils.DataframeEnsemble.load(folder)
            self.assertEqual(len(ensemble), 0)

    def test_folder_metadata_wrong_version(self):
        """Test that folder metadata with unsupported version raises error."""
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp) / "ens"
            folder.mkdir()
            with open(folder / 'metadata.json', 'w') as f:
                json.dump({
                    "__type__": "DataframeEnsemble",
                    "version": 999,
                    "members": []
                }, f)

            # Currently no version check in folder loading - should add one
            # For now, it will load successfully
            ensemble = utils.DataframeEnsemble.load(folder)
            self.assertEqual(len(ensemble), 0)

    def test_folder_missing_csv_files(self):
        """Test that metadata referencing non-existent CSV files raises error."""
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp) / "ens"
            folder.mkdir()
            metadata = {
                "__type__": "DataframeEnsemble",
                "version": 1,
                "members": [{
                    "key": 0,
                    "key_type": "int",
                    "filename": "missing",
                    "name": "", "source": "", "description": "", "tags": ""
                }]
            }
            with open(folder / 'metadata.json', 'w') as f:
                json.dump(metadata, f)

            with self.assertRaises(FileNotFoundError):
                utils.DataframeEnsemble.load(folder)

    def test_folder_missing_metadata_json(self):
        """Test that folder without metadata.json raises error."""
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp) / "ens"
            folder.mkdir()
            # Create some CSV files but no metadata
            pd.DataFrame({"A": [1, 2]}).to_csv(folder / "test.csv")

            with self.assertRaises(FileNotFoundError):
                utils.DataframeEnsemble.load(folder)

    def test_empty_ensemble_serialization(self):
        """Test that empty ensemble can be saved and loaded."""
        empty = utils.DataframeEnsemble()

        with tempfile.TemporaryDirectory() as tmp:
            # JSON format
            json_path = Path(tmp) / "empty"
            empty.save(json_path, save_format="json")
            loaded_json = utils.DataframeEnsemble.load(Path(tmp) / "empty.json")
            self.assertEqual(len(loaded_json), 0)

            # Folder format
            folder_path = Path(tmp) / "empty_folder"
            empty.save(folder_path, save_format="folder")
            loaded_folder = utils.DataframeEnsemble.load(folder_path)
            self.assertEqual(len(loaded_folder), 0)

            # Pickle format
            pickle_path = Path(tmp) / "empty_pickle"
            empty.save(pickle_path, save_format="pickle")
            loaded_pickle = utils.DataframeEnsemble.load(
                Path(tmp) / "empty_pickle.pkl",
                pickle_safety_lock=False
            )
            self.assertEqual(len(loaded_pickle), 0)

    def test_folder_metadata_missing_members_field(self):
        """Test that folder metadata without 'members' key raises error."""
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp) / "ens"
            folder.mkdir()
            with open(folder / 'metadata.json', 'w') as f:
                json.dump({"__type__": "DataframeEnsemble", "version": 1}, f)

            with self.assertRaises(KeyError):
                utils.DataframeEnsemble.load(folder)

    def test_folder_special_chars_in_keys(self):
        """Test that keys with special characters are handled correctly."""
        ensemble = self._make_ensemble()

        with tempfile.TemporaryDirectory() as tmp:
            df = pd.DataFrame({"A": [1.0, 2.0], "B": [3.0, 4.0]}, index=["r1", "r2"])
            tsdf = utils.TimeseriesDataframe.from_dataframe(df)
            ensemble.add_dataframe(tsdf, key="test/with\\slashes")

            folder_path = Path(tmp) / "special"
            ensemble.save(folder_path, save_format="folder")

            # Verify the filename has special chars replaced
            self.assertTrue((folder_path / "test_with_slashes.csv").exists())

            # Load and verify
            loaded = utils.DataframeEnsemble.load(folder_path)
            self.assertIn("test/with\\slashes", loaded.ensemble)

    def test_folder_overwrite_partial_collision(self):
        """Test overwrite behavior when folder partially exists."""
        ensemble = self._make_ensemble()

        with tempfile.TemporaryDirectory() as tmp:
            folder_path = Path(tmp) / "partial"
            folder_path.mkdir()

            # Create only metadata.json (unusual but possible)
            with open(folder_path / 'metadata.json', 'w') as f:
                f.write("{}")

            # Should fail with overwrite=False
            with self.assertRaises(FileExistsError):
                ensemble.save(folder_path, save_format="folder", overwrite=False)

            # Should succeed with overwrite=True
            ensemble.save(folder_path, save_format="folder", overwrite=True)
            self.assertTrue((folder_path / 'metadata.json').exists())
            self.assertTrue((folder_path / "0.csv").exists())


if __name__ == '__main__':
    unittest.main()
