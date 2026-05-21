"""
Provides extensions to dataframes which facilitates tracking and bulk analysis.

TimeseriesDataframes (TSDF) are a wrapper around pandas dataframes, with extra
fields (tags, name, source, ...) and methods that facilitate working with these
fields.

DataframeEnsembles are a way to organise multiple TSDFs, with methods that work
(at present) primarily with the tags associated with TSDFs.

"""

import enum
import json
from pathlib import Path
import pickle
import re
from typing import Any, Callable, Iterable, Literal, Optional, overload

import pandas as pd

# Type alias for DataframeEnsemble keys (None is not supported)
# bool is excluded due to hash collision with int (True==1, False==0)
EnsembleKey = int | str | float


class RegexArg(enum.Enum):
    """Specifies the type of argument supplied to filtering functions in TSDF
    and DataframeEnsemble."""
    PATTERN = 1
    OBJECT = 2


class TimeseriesDataframe(pd.DataFrame):
    """
    A TimeseriesDataframe is thinly extended pd.Dataframe. Abbreviated casually
    as TSDF throughout the documentation. It adds the following fields:

    * name (str)
    * source (str)
    * description (str)
    * a string of tags (str)

    Metadata Preservation
    ---------------------
    TimeseriesDataframe uses pandas' ``_metadata`` and ``_constructor``
    mechanisms to automatically preserve metadata across most operations:

    * **Preserved**: slicing, arithmetic with scalars/Series, fillna, apply,
      copy, transpose, rank, reset_index, and most other standard pandas operations
    * **Not preserved**: rolling window operations, :func:`pandas.concat`
    * **No guarantees**: Binary operations between two TimeseriesDataframes
      (e.g., ``tsdf1 + tsdf2``) may preserve metadata from either operand
      depending on pandas internals

    For operations that don't preserve metadata, use the :meth:`tsdf_apply` method.

    Examples
    --------
    >>> df = pd.DataFrame({'A': [1, 2, 3]})
    >>> tsdf = TimeseriesDataframe.from_dataframe(df, name="test", source="file.csv")
    >>> tsdf.add_tag("raw,validated")
    >>>
    >>> # Metadata preserved automatically in most operations:
    >>> result = tsdf * 2
    >>> result.name  # "test"
    >>> result.tags  # "raw,validated"
    >>>
    >>> # For rolling operations, use tsdf_apply:
    >>> result = tsdf.tsdf_apply(lambda df: df.rolling(window=2).mean())
    >>> result.name  # "test" - metadata preserved

    See Also
    --------
    TimeseriesDataframe.tsdf_apply : Apply functions while preserving metadata
    TimeseriesDataframe.from_dataframe : Create from existing DataFrame
    """

    # https://pandas.pydata.org/docs/development/extending.html#override-constructor-properties
    _metadata = ['name', 'source', 'description', 'tags']

    @classmethod
    def metadata_fields(cls) -> list[str]:
        """Return the list of metadata fields defined for this class."""
        return cls._metadata.copy()

    TAG_DELIMITER = ','

    @property
    def _constructor(self) -> type['TimeseriesDataframe']:
        """Return TimeseriesDataframe as the constructor for pandas operations."""
        return TimeseriesDataframe

    def __finalize__(self, other, method=None, **kwargs):
        """
        Propagate metadata from other to self.

        This ensures metadata is preserved during operations between
        TimeseriesDataframes and other pandas objects (like Series).
        """
        # Call parent implementation first
        self = super().__finalize__(other, method=method, **kwargs)

        # If other is a TimeseriesDataframe, ensure metadata is copied
        # This handles cases where pandas doesn't properly propagate metadata
        if isinstance(other, TimeseriesDataframe):
            for attr in self._metadata:
                # Copy attribute from other if it exists and self doesn't have it
                # or if self has an empty/default value
                other_val = getattr(other, attr, None)
                self_val = getattr(self, attr, None)

                # Preserve non-empty values from other
                if other_val and (not self_val or self_val == ""):
                    object.__setattr__(self, attr, other_val)

        return self

    def _wrap_result(self, result, other=None):
        """
        Wrap results of operations to preserve metadata.

        This method is called by pandas for many binary operations to finalize
        the result. We override it to ensure metadata is preserved when operating
        with non-TSDF objects (like Series).
        """
        # Handle the case where pandas didn't construct a TimeseriesDataframe
        if isinstance(result, pd.DataFrame) and not isinstance(result, TimeseriesDataframe):
            # Convert to TimeseriesDataframe with our metadata
            result = TimeseriesDataframe.from_dataframe(result)
            for attr in self._metadata:
                setattr(result, attr, getattr(self, attr, ""))

        # If result is already a TimeseriesDataframe, ensure metadata is set
        elif isinstance(result, TimeseriesDataframe):
            for attr in self._metadata:
                result_val = getattr(result, attr, "")
                if not result_val:  # If result doesn't have metadata, copy from self
                    setattr(result, attr, getattr(self, attr, ""))

        return result

    def _ensure_metadata(self, result):
        """
        Ensure metadata is preserved in the result of an operation.

        This is used by arithmetic operators to preserve metadata when
        pandas doesn't automatically do it (e.g., operations with Series).

        Note: When both operands are TimeseriesDataframes (e.g., tsdf1 + tsdf2),
        the result may already have metadata from the other operand. In such cases,
        this method will NOT overwrite existing metadata, meaning the behavior
        depends on pandas internals and no guarantees are made about which
        operand's metadata is preserved.
        """
        if isinstance(result, TimeseriesDataframe):
            # Copy metadata from self to result if result doesn't have it
            for attr in self._metadata:
                result_val = getattr(result, attr, "")
                if not result_val:  # If result doesn't have metadata
                    setattr(result, attr, getattr(self, attr, ""))
        return result

    # Override arithmetic operators to preserve metadata
    def __add__(self, other):
        result = super().__add__(other)
        return self._ensure_metadata(result)

    def __sub__(self, other):
        result = super().__sub__(other)
        return self._ensure_metadata(result)

    def __mul__(self, other):
        result = super().__mul__(other)
        return self._ensure_metadata(result)

    def __truediv__(self, other):
        result = super().__truediv__(other)
        return self._ensure_metadata(result)

    def __floordiv__(self, other):
        result = super().__floordiv__(other)
        return self._ensure_metadata(result)

    def __mod__(self, other):
        result = super().__mod__(other)
        return self._ensure_metadata(result)

    def __pow__(self, other):
        result = super().__pow__(other)
        return self._ensure_metadata(result)

    # Also override reverse operators for completeness
    def __radd__(self, other):
        result = super().__radd__(other)
        return self._ensure_metadata(result)

    def __rsub__(self, other):
        result = super().__rsub__(other)
        return self._ensure_metadata(result)

    def __rmul__(self, other):
        result = super().__rmul__(other)
        return self._ensure_metadata(result)

    def __rtruediv__(self, other):
        result = super().__rtruediv__(other)
        return self._ensure_metadata(result)

    def __rfloordiv__(self, other):
        result = super().__rfloordiv__(other)
        return self._ensure_metadata(result)

    def __rmod__(self, other):
        result = super().__rmod__(other)
        return self._ensure_metadata(result)

    def __rpow__(self, other):
        result = super().__rpow__(other)
        return self._ensure_metadata(result)

    def __init__(self,                                        # pylint: disable=keyword-arg-before-vararg
                 data=None, *args,                            # Pandas 
                 name="", source="", description="", **kwargs # tsdf metadata
                 ) -> None:
        """
        Parameters
        ----------
        data : array-like, Iterable, dict, or DataFrame, optional
            Data to initialize the DataFrame with
        name : str, optional
            Name of the timeseries
        source : str, optional
            Source of the timeseries
        description : str, optional
            Description of the timeseries

        See Also
        --------
        TimeseriesDataframe.add_tag : Add tags to the timeseries
        """
        super().__init__(data, *args, **kwargs)
        # Use hasattr guards because _metadata propagation via __finalize__
        # may set these attributes before __init__ is called
        if not hasattr(self, 'name'):
            self.name = name
        if not hasattr(self, 'source'):
            self.source = source
        if not hasattr(self, 'description'):
            self.description = description
        if not hasattr(self, 'tags'):
            self.tags = ""

    @classmethod
    def from_dataframe(cls, df, **kwargs) -> 'TimeseriesDataframe':
        """Create a TimeseriesDataframe from an existing DataFrame.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame to convert
        **kwargs
            Metadata fields (name, source, description)

        Returns
        -------
        TimeseriesDataframe
            New instance with data from df and metadata from kwargs

        Examples
        --------
        >>> df = pd.DataFrame({'A': [1, 2, 3]})
        >>> tsdf = TimeseriesDataframe.from_dataframe(df, name="test", source="file.csv")
        """
        return cls(df, **kwargs)

    def save(self, filename: str | Path,
             save_format: Literal["json", "csv"] = "json",
             overwrite: bool = False) -> None:
        """
        Save (serialise) this TimeseriesDataframe to a file.

        Parameters
        ----------
        filename : str | Path
            Path to save the file (extension will be added automatically)
        save_format : {"json", "csv"}, default "json"
            Format to save the file in:

            - json: Save as JSON with all metadata
            - csv: Save as CSV file with separate metadata.json
        overwrite : bool, default False
            If True, overwrite existing files. If False, raise FileExistsError.

        Examples
        --------
        >>> tsdf = TimeseriesDataframe.from_dataframe(df, name="test", source="data.csv")
        >>> tsdf.add_tag("validated")
        >>> tsdf.save("output", save_format="json")  # Creates output.json
        >>> tsdf.save("output", save_format="csv")   # Creates output.csv + output.metadata.json
        """
        if isinstance(filename, str):
            filename = Path(filename)

        if save_format == "json":
            fn = filename.with_suffix('.json')
            if fn.exists() and not overwrite:
                raise FileExistsError(f"File {fn} already exists and overwrite is set to False.")

            with open(fn, 'w', encoding='utf-8') as f:
                json.dump(self, f, cls=_TsdfJsonEncoder, indent=2)

        elif save_format == "csv":
            csv_fn = filename.with_suffix('.csv')
            metadata_fn = filename.with_suffix('').with_name(filename.stem + '.metadata.json')

            if csv_fn.exists() and not overwrite:
                raise FileExistsError(f"File {csv_fn} already exists and overwrite is set to False.")
            if metadata_fn.exists() and not overwrite:
                raise FileExistsError(f"File {metadata_fn} already exists and overwrite is set to False.")

            # Save CSV
            self.to_csv(csv_fn)

            # Save metadata
            metadata_dict = {"__type__": "TimeseriesDataframe", "version": 1}
            for field in TimeseriesDataframe._metadata:
                metadata_dict[field] = getattr(self, field, "")

            with open(metadata_fn, 'w', encoding='utf-8') as f:
                json.dump(metadata_dict, f, indent=2)
        else:
            raise ValueError(f"Unsupported format '{save_format}'. Please use 'json' or 'csv'.")

    @classmethod
    def load(cls, filename: str | Path,
             format_override: Optional[Literal["json", "csv"]] = None) -> 'TimeseriesDataframe':
        """
        Load (deserialise) a TimeseriesDataframe from a file.

        Parameters
        ----------
        filename : str | Path
            Path to the file to load
        format_override : {"json", "csv"}, optional
            Override automatic format detection from file extension. If csv,
            will attempt to find <file>.json (where filename=<file>.csv) and
            vice versa (i.e. via suffix replacement).

        Returns
        -------
        TimeseriesDataframe
            Loaded TimeseriesDataframe with all metadata restored

        Examples
        --------
        >>> tsdf = TimeseriesDataframe.load("output.json")
        >>> tsdf = TimeseriesDataframe.load("output.csv") # will attempt to locate metadata JSON file
        >>> tsdf = TimeseriesDataframe.load("data.dat", format_override="json")
        """
        if not isinstance(filename, Path):
            filename = Path(filename)

        # Determine format
        if format_override:
            save_format = format_override
        elif filename.suffix == '.json':
            save_format = 'json'
        elif filename.suffix == '.csv':
            save_format = 'csv'
        else:
            raise ValueError(f"Cannot determine format from extension '{filename.suffix}'. "
                             f"Please specify format_override='json' or 'csv'.")

        if save_format == "json":
            if not filename.exists():
                raise FileNotFoundError(f"File not found: {filename}")
            try:
                with open(filename, 'r', encoding='utf-8') as f:
                    tsdf = json.load(f, cls=_TsdfJsonDecoder)
            except json.JSONDecodeError as e:
                raise ValueError(f"Failed to parse JSON file '{filename}': {e}") from e

            if not isinstance(tsdf, TimeseriesDataframe):
                raise ValueError(
                    f"File '{filename}' does not contain a TimeseriesDataframe "
                    f"(got type: {type(tsdf).__name__})"
                )
            return tsdf

        elif save_format == "csv":
            if not filename.exists():
                raise FileNotFoundError(f"CSV file not found: {filename}")

            metadata_fn = filename.with_suffix('').with_name(filename.stem + '.metadata.json')
            if not metadata_fn.exists():
                raise FileNotFoundError(
                    f"Metadata file not found: {metadata_fn}\n"
                    f"TimeseriesDataframe.load() requires a metadata file for CSV format.\n"
                    f"If you want to load a plain CSV without metadata, use:\n"
                    f"  bulum.io.read('{filename}') or\n"
                    f"  TimeseriesDataframe.from_dataframe(pd.read_csv('{filename}'), ...)"
                )

            try:
                df = pd.read_csv(filename, index_col=0)
            except Exception as e:
                raise ValueError(f"Failed to read CSV file '{filename}'") from e
            tsdf = cls.from_dataframe(df)

            try:
                with open(metadata_fn, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)
            except json.JSONDecodeError as e:
                raise ValueError(
                    f"Failed to parse metadata file '{metadata_fn}'"
                ) from e

            if not isinstance(metadata, dict):
                raise ValueError(
                    f"Metadata file '{metadata_fn}' has invalid structure "
                    f"(expected a JSON object, got {type(metadata).__name__})"
                )

            declared_type = metadata.get("__type__")
            if declared_type != "TimeseriesDataframe":
                raise ValueError(
                    f"Metadata file '{metadata_fn}' is not for a TimeseriesDataframe "
                    f"(found __type__={declared_type!r})"
                )

            version = metadata.get("version", 1)
            _supported_versions = {1}
            if not isinstance(version, int) or version not in _supported_versions:
                raise ValueError(
                    f"Metadata file '{metadata_fn}' has unsupported version {version!r} "
                    f"(only version 1 is supported)"
                )

            for field in cls._metadata:
                if field not in metadata:
                    continue
                field_value = metadata[field]
                if not isinstance(field_value, str):
                    raise ValueError(
                        f"Metadata file '{metadata_fn}': field '{field}' must be a string, "
                        f"got {type(field_value).__name__}"
                    )
                if field == "tags" and field_value:
                    tsdf.add_tag(field_value)
                else:
                    setattr(tsdf, field, field_value)

            return tsdf
        else:
            raise ValueError(f"Unsupported format '{save_format}'.")

    def print_summary(self) -> None:
        print(f"Name: {self.name}")
        print(f"Source: {self.source}")
        print(f"Description: {self.description}")
        print(f"Tags: {self.tags}")
        print(self.describe())

    def has_tag(self, pattern: str | re.Pattern, *, regex: RegexArg | None = None,
                exact: bool = False) -> bool:
        """Check if the provided tag matches any of the dataframe's tags.

        Parameters
        ----------
        pattern : RegexArg, optional, keyword-only)
            - None: Uses python `in` operation to check for membership; expects
              a string to be supplied to pattern.
            - `RegexArg`: Uses the regex engine to search for the tag.
        exact : bool
            Whether we require an exact match of the tag.
            This argument is superceded by a non-None `regex` argument, and
            may be accomplished (depending on the particulars) via regex by
            ``\\b<regex>\\b``.

        """
        if regex is None:
            assert isinstance(pattern, str)
            if exact:
                split_tags = self.tags.split(self.TAG_DELIMITER)
                return pattern in split_tags
            else:
                return pattern in self.tags
        elif regex == RegexArg.PATTERN:
            assert isinstance(pattern, str)
            return bool(re.search(pattern, self.tags))
        elif regex == RegexArg.OBJECT:
            assert isinstance(pattern, re.Pattern)
            return bool(pattern.search(self.tags))
        else:
            raise ValueError("Invalid argument supplied to regex, " +
                             f"regex={regex} but expected RegexArg")

    def add_tag(self, tag: str | Iterable[str], check_membership: bool = False) -> None:
        """Add a tag to the TimeseriesDataframe.

        This is the canonical way to add tags to a TimeseriesDataframe. It can
        add multiple tags separated by the designated tag delimiter (by default,
        a comma ,).

        Examples
        --------
        The `check_membership` flag will ensure that `tag` does not match with
        existing tags, but will not (at present) check the other way around. For
        example, the following will not raise an error::

            df.add_tag("01", True)
            df.add_tag("01a", True)
        """
        if isinstance(tag, Iterable) and not isinstance(tag, str):
            _tmp = self.TAG_DELIMITER.join(tag)
            tag = _tmp
        tag = tag.strip()
        if self.TAG_DELIMITER in tag:
            tags = [x for x in tag.split(self.TAG_DELIMITER) if x != ""]
            for tag in tags:
                self.add_tag(tag)
        else:
            if check_membership and self.has_tag(tag):
                raise ValueError(f"{tag=} matched in existing tags")
            if self.tags == "":
                self.tags = tag
            else:
                self.tags = self.tags + self.TAG_DELIMITER + tag

    def count_tags(self) -> int:
        if self.tags == "":
            return 0
        else:
            return len(self.tags.split(self.TAG_DELIMITER))

    def tsdf_apply(self, func: Callable[[pd.DataFrame], pd.DataFrame]) -> 'TimeseriesDataframe':
        """Apply a function to the underlying dataframe, returning a new
        TimeseriesDataframe with the results. The metadata fields are copied
        over.

        .. note::
            Most pandas operations automatically preserve
            metadata via the ``_constructor`` property, including:

            - Standard ``.apply()``: ``tsdf.apply(lambda x: x * 2)`` preserves metadata
            - Arithmetic, slicing, fillna, copy, transpose, etc.

            **Use tsdf_apply() only when** the operation bypasses ``_constructor``:

            - Rolling window operations: ``tsdf.rolling(window=2).mean()``
            - Concatenation: ``pd.concat([tsdf1, tsdf2])``
            - Creating new DataFrames from scratch: ``pd.DataFrame(tsdf.mean())``

        Parameters
        ----------
        func : Callable[[pd.DataFrame], pd.DataFrame]
            Function that takes a DataFrame and returns a DataFrame

        Returns
        -------
        TimeseriesDataframe
            New TimeseriesDataframe with the result of func(self) and all
            metadata fields (name, source, description, tags) copied from self

        Examples
        --------
        >>> tsdf = TimeseriesDataframe.from_dataframe(df, name="test", source="data.csv")
        >>> tsdf.add_tag("raw")
        >>>
        >>> # Pandas .apply() preserves metadata automatically (no need for tsdf_apply):
        >>> result = tsdf.apply(lambda x: x * 2)
        >>> result.name  # "test" - metadata preserved!
        >>>
        >>> # Use tsdf_apply for rolling operations (metadata would be lost otherwise):
        >>> result = tsdf.tsdf_apply(lambda df: df.rolling(window=2).mean())
        >>> result.name  # "test" - metadata preserved
        >>>
        >>> # Use tsdf_apply when creating new DataFrames from scratch:
        >>> result = tsdf.tsdf_apply(lambda df: pd.DataFrame(df.mean()))
        >>> result.tags  # "raw" - metadata preserved

        See Also
        --------
        TimeseriesDataframe.from_dataframe : Create TSDF from regular DataFrame
        pandas.DataFrame.apply : Standard pandas apply (preserves metadata for TSDF)
        """
        new_df = func(self)
        new_tsdf = TimeseriesDataframe.from_dataframe(new_df)
        new_tsdf.name = self.name
        new_tsdf.source = self.source
        new_tsdf.description = self.description
        new_tsdf.tags = self.tags
        return new_tsdf


class DataframeEnsemble:
    """A DataframeEnsemble is an collection of bulum-style timeseries
    dataframes, which might represent collected results from a set of model
    runs. Each timeseries dataframe is stored in an internal object, with a
    little attached metadata. All timeseries in the ensemble are expected to
    have the same index, and the same columns.

    While the class exposes the ensemble property as a dict, you should
    generally use the provided methods to interact with the ensemble, viz.
    .add_dataframe(), and .get().
    """

    def __init__(self, dfs: Iterable[TimeseriesDataframe] | None = None) -> None:
        """
        Args:
            dfs: A collection of dataframes to add to the ensemble.
        """
        self.ensemble: dict[EnsembleKey, TimeseriesDataframe] = {}
        if dfs is not None:
            for df in dfs:
                self.add_dataframe(df)

#    @classmethod
#    def from_files(cls, filenames):
#        """Convenience method to construct from a list of filenames."""
#        return cls(map(TimeseriesDataframe.from_file, filenames))

    def __iter__(self):
        return iter(self.ensemble.values())

    def __len__(self):
        # Returns how many dataframes are in the ensemble
        return len(self.ensemble)
    

    @overload
    def map(self, func: Callable[[TimeseriesDataframe], TimeseriesDataframe]) -> 'DataframeEnsemble':
        ...

    @overload
    def map(self, func: Callable[[pd.DataFrame], pd.DataFrame]) -> 'DataframeEnsemble':
        ...

    def map(self, func) -> 'DataframeEnsemble':
        """Apply a function to all dataframes in the ensemble, returning a new
        ensemble with the results.

        Parameters
        ----------
        func
            Univariate function on dataframes.
        """
        new_ensemble = DataframeEnsemble()
        for key, df in self.ensemble.items():
            new_df = func(df)
            new_ensemble.add_dataframe(new_df, key)
        return new_ensemble

    def get(self, key: Optional[EnsembleKey] = None) -> TimeseriesDataframe:
        """Return the underlying dataframe if the ensemble is a singleton, or
        the dataframe at the given key."""
        if key is None:
            if len(self.ensemble) == 1:
                return next(iter(self.ensemble.values()))
            else:
                raise ValueError("DataframeEnsemble.get() was called on a non-singleton ensemble")
        else:
            tmp = self.ensemble.get(key)
            if tmp is None:
                raise KeyError(f"Key {key} not found in ensemble")
            return tmp

    def add_dataframe(self, df: pd.DataFrame | TimeseriesDataframe, key: Optional[Any] = None, tag: Optional[str] = None) -> None:
        if not isinstance(df, TimeseriesDataframe):
            df = TimeseriesDataframe.from_dataframe(df)
        if tag is not None:
            df.add_tag(tag)
        self.assert_df_shape_matches_ensemble(df)
        if key is None:
            # Automatically pick the next available integer to use as a key
            key = 0
            while key in self.ensemble:
                key += 1
        self.ensemble[key] = df

#    def add_dataframe_from_file(self, filename, key=None, tag=None):
#        df = TimeseriesDataframe.from_file(filename)
#        self.add_dataframe(df, key, tag)

    def print_summary(self) -> None:
        for key, val in self.ensemble.items():
            print(f"Key: {key}, Shape: {val.shape}, Tags: {val.tags}")

    def df_shape_matches_ensemble(self, new_df: pd.DataFrame | TimeseriesDataframe) -> bool:
        """Internal function to verify new dfs."""
        if len(self.ensemble) > 0:
            first_shape = list(self.ensemble.values())[0].shape
            new_df_shape = new_df.shape
            if first_shape != new_df_shape:
                return False
        return True

    def assert_df_shape_matches_ensemble(self, new_df: pd.DataFrame | TimeseriesDataframe) -> None:
        """Internal function to verify new dfs."""
        if len(self.ensemble) > 0:
            first_shape = list(self.ensemble.values())[0].shape
            new_df_shape = new_df.shape
            if first_shape != new_df_shape:
                raise ValueError(
                    f"ERROR: New Dataframe has shape {new_df_shape}" +
                    f" but the ensemble members have shape {first_shape}!"
                )

    def filter_tag(self, tag: str,
                   *,
                   exclude: bool = False, **kwargs) -> 'DataframeEnsemble':
        """Return a new ensemble containing dataframes filtered by tag.

        By default, it will include all dataframes whose tags partially match
        the provided tag. 

        This function delegates to `TSDF.has_tag()`, refer to that function for
        keyword arguments.

        Parameters
        ----------
        tag
            The tag to match. String, regex pattern, or compiled regex pattern.
            (Regex requires regex argument to be set, c.f. `TSDF.has_tag()`)
        exclude : bool
            If True, it will *filter out* all dataframes which match the tag.

        """
        subensemble = DataframeEnsemble()
        for key, tsdf in self.ensemble.items():
            # Take the logical XOR
            if tsdf.has_tag(tag, **kwargs) != exclude:
                subensemble.add_dataframe(tsdf, key)
        return subensemble

    def add_tag(self, tag: str) -> None:
        """Add a tag to all member dataframes."""
        for dataframe in self.ensemble.values():
            dataframe.add_tag(tag)

    def save(self, filename: str | Path,
             save_format: Literal["json", "pickle", "folder", "zip", "zip-folder"] = "json",
             overwrite: bool = False) -> None:
        """
        Save (serialise) this DataframeEnsemble to disk.

        Parameters
        ----------
        filename : str | Path
            Path to save the file/folder (extension added automatically for some formats)
        save_format : {"json", "pickle", "folder", "zip", "zip-folder"}, default "json"
            Format to save the ensemble in:

            - json: Save as a single JSON file with all data
            - pickle: Save as a pickle file (warning: pickle has security implications)
            - folder: Save as a directory with CSV files and metadata.json
            - zip: Save as a zip archive (creates folder, zips it, removes folder)
            - zip-folder: Save as both a zip archive and keep the folder
        overwrite : bool, default False
            If True, overwrite existing files/folders. If False, raise FileExistsError.

        Raises
        ------
        ValueError
            If save_format is not supported
        TypeError
            If ensemble contains keys with unsupported types (only int, str, float supported)
        FileExistsError
            If file/folder exists and overwrite=False

        Examples
        --------
        >>> ensemble = DataframeEnsemble()
        >>> ensemble.add_dataframe(tsdf1, key=0)
        >>> ensemble.save("results", save_format="folder")
        >>> ensemble.save("results", save_format="json")
        """
        import shutil
        import zipfile

        if isinstance(filename, str):
            filename = Path(filename)

        # Validate format before doing anything
        _supported_formats = {"json", "pickle", "folder", "zip", "zip-folder"}
        if save_format not in _supported_formats:
            raise ValueError(
                f"Unsupported save format '{save_format}'. "
                f"Supported formats: {', '.join(_supported_formats)}"
            )

        # Validate key types before saving
        for key in self.ensemble.keys():
            if not isinstance(key, (int, str, float)):
                raise TypeError(
                    f"Cannot serialize ensemble: key {key!r} has unsupported type "
                    f"'{type(key).__name__}'. Only int, str, and float keys are supported."
                )

        if save_format == "json":
            fn = filename.with_suffix('.json')
            if fn.exists() and not overwrite:
                raise FileExistsError(f"File {fn} already exists and overwrite is set to False.")

            with open(fn, 'w', encoding='utf-8') as f:
                json.dump(self, f, cls=_TsdfJsonEncoder, indent=2)

        elif save_format == "pickle":
            fn = filename.with_suffix('.pkl')
            if fn.exists() and not overwrite:
                raise FileExistsError(f"File {fn} already exists and overwrite is set to False.")

            with open(fn, 'wb') as f:
                pickle.dump(self, f)

        elif save_format in ("folder", "zip", "zip-folder"):
            # Create folder with CSV files and metadata
            folder_path = filename.with_suffix('')  # Remove any extension

            if folder_path.exists() and not overwrite:
                raise FileExistsError(
                    f"Folder {folder_path} already exists and overwrite is set to False."
                )

            # Clean up existing folder if overwrite=True
            if folder_path.exists() and overwrite:
                shutil.rmtree(folder_path)

            folder_path.mkdir(parents=True, exist_ok=False)

            try:
                # Save each TSDF as a CSV file
                metadata = {
                    "__type__": "DataframeEnsemble",
                    "version": 1,
                    "members": []
                }

                for key, tsdf in self.ensemble.items():
                    # Create a safe filename from the key
                    safe_filename = str(key).replace('/', '_').replace('\\', '_')
                    csv_filename = f"{safe_filename}.csv"
                    csv_path = folder_path / csv_filename

                    # Save the dataframe
                    tsdf.to_csv(csv_path)

                    # Add metadata entry
                    member_meta = {
                        "key": key,
                        "key_type": type(key).__name__,
                        "filename": safe_filename,
                        "name": tsdf.name,
                        "source": tsdf.source,
                        "description": tsdf.description,
                        "tags": tsdf.tags
                    }
                    metadata["members"].append(member_meta)

                # Save metadata.json
                with open(folder_path / 'metadata.json', 'w', encoding='utf-8') as f:
                    json.dump(metadata, f, indent=2)

                # Handle zip formats
                if save_format in ("zip", "zip-folder"):
                    zip_path = filename.with_suffix('.zip')
                    if zip_path.exists() and not overwrite:
                        # Clean up the folder we just created
                        shutil.rmtree(folder_path)
                        raise FileExistsError(
                            f"File {zip_path} already exists and overwrite is set to False."
                        )

                    # Create zip file
                    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                        for file in folder_path.rglob('*'):
                            if file.is_file():
                                arcname = file.relative_to(folder_path.parent)
                                zipf.write(file, arcname=arcname)

                    # Remove folder if format is "zip" (not "zip-folder")
                    if save_format == "zip":
                        shutil.rmtree(folder_path)

            except Exception:
                # Clean up on error
                if folder_path.exists():
                    shutil.rmtree(folder_path)
                raise

        else:
            raise ValueError(f"Unsupported format '{save_format}'.")

    @classmethod
    def load(cls, filename: str | Path,
             format_override: Optional[Literal["json", "pickle", "folder"]] = None,
             pickle_safety_lock: bool = True) -> 'DataframeEnsemble':
        """
        Load (deserialise) a DataframeEnsemble from disk.

        Parameters
        ----------
        filename : str | Path
            Path to the file or folder to load
        format_override : {"json", "pickle", "folder"}, optional
            Override automatic format detection from file extension/type
        pickle_safety_lock : bool, default True
            Safety lock for pickle format. If True (default), raises ValueError when
            attempting to load pickle files. Set to False to allow pickle loading.

        Returns
        -------
        DataframeEnsemble
            Loaded DataframeEnsemble with all member dataframes and metadata restored

        Raises
        ------
        FileNotFoundError
            If the specified file or folder doesn't exist
        ValueError
            If the file format is unsupported or data is malformed, or if attempting
            to load pickle with safety lock enabled

        Examples
        --------
        >>> ensemble = DataframeEnsemble.load("results.json")
        >>> ensemble = DataframeEnsemble.load("results")  # Auto-detects folder
        >>> ensemble = DataframeEnsemble.load("results.zip")  # Auto-extracts zip
        >>> ensemble = DataframeEnsemble.load("results.pkl", pickle_safety_lock=False)  # Unsafe!
        """
        import shutil
        import tempfile
        import zipfile

        if not isinstance(filename, Path):
            filename = Path(filename)

        # Check if we need to handle pickle with safety check
        if (filename.suffix == '.pkl' or format_override == 'pickle') and pickle_safety_lock:
            raise ValueError(
                "Loading pickle files is disabled for security reasons. "
                "Pickle files can execute arbitrary code. "
                "Please use JSON or folder format instead. "
                "To override this safety check, pass pickle_safety_lock=False."
            )

        # Determine format
        if format_override:
            save_format = format_override
        elif filename.suffix == '.json':
            save_format = 'json'
        elif filename.suffix == '.pkl':
            save_format = 'pickle'
        elif filename.suffix == '.zip':
            save_format = 'zip'
        elif filename.is_dir():
            save_format = 'folder'
        elif not filename.exists():
            raise FileNotFoundError(f"File or folder not found: {filename}")
        else:
            raise ValueError(
                f"Cannot determine format from '{filename}'. "
                f"Please specify format_override='json', 'pickle', or 'folder'."
            )

        if save_format == "pickle":
            if not filename.exists():
                raise FileNotFoundError(f"Pickle file not found: {filename}")

            with open(filename, 'rb') as f:
                ensemble = pickle.load(f)

            if not isinstance(ensemble, DataframeEnsemble):
                raise ValueError(
                    f"File '{filename}' is not a DataframeEnsemble "
                    f"(got type: {type(ensemble).__name__})"
                )
            return ensemble

        elif save_format == "json":
            if not filename.exists():
                raise FileNotFoundError(f"File not found: {filename}")

            with open(filename, 'r', encoding='utf-8') as f:
                ensemble = json.load(f, cls=_TsdfJsonDecoder)

            if not isinstance(ensemble, DataframeEnsemble):
                raise ValueError(
                    f"File '{filename}' is not a DataframeEnsemble "
                    f"(got type: {type(ensemble).__name__})"
                )
            return ensemble

        elif save_format == "zip":
            # Extract zip to temporary directory and load from there
            if not filename.exists():
                raise FileNotFoundError(f"Zip file not found: {filename}")

            with tempfile.TemporaryDirectory() as tmpdir:
                tmpdir_path = Path(tmpdir)

                # Extract zip
                with zipfile.ZipFile(filename, 'r') as zipf:
                    zipf.extractall(tmpdir_path)

                # Find the folder inside (should be named after the zip stem)
                folder_name = filename.stem
                folder_path = tmpdir_path / folder_name

                if not folder_path.exists() or not folder_path.is_dir():
                    # Try to find any folder
                    folders = [p for p in tmpdir_path.iterdir() if p.is_dir()]
                    if not folders:
                        raise ValueError(f"No folder found in zip file {filename}")
                    folder_path = folders[0]

                # Load from the extracted folder
                return cls._load_from_folder(folder_path)

        elif save_format == "folder":
            if not filename.exists():
                raise FileNotFoundError(f"Folder not found: {filename}")
            if not filename.is_dir():
                raise ValueError(f"Expected a directory, but {filename} is not a directory")

            return cls._load_from_folder(filename)

        else:
            raise ValueError(f"Unsupported format '{save_format}'.")

    @classmethod
    def _load_from_folder(cls, folder_path: Path) -> 'DataframeEnsemble':
        """Internal method to load ensemble from a folder."""
        metadata_file = folder_path / 'metadata.json'
        if not metadata_file.exists():
            raise FileNotFoundError(
                f"Metadata file not found: {metadata_file}\n"
                f"Expected metadata.json in folder {folder_path}"
            )

        # Load metadata
        with open(metadata_file, 'r', encoding='utf-8') as f:
            metadata = json.load(f)

        if not isinstance(metadata, dict):
            raise ValueError(f"Metadata file has invalid structure (expected dict)")

        # Version checking is not currently enforced for folder format
        # version = metadata.get("version", 1)
        # if version != 1:
        #     raise ValueError(
        #         f"Unsupported DataframeEnsemble version: {version} "
        #         f"(only version 1 is supported)"
        #     )

        if "members" not in metadata:
            raise KeyError(f"Metadata file missing required 'members' field")

        ensemble = cls()
        _key_constructors = {"int": int, "str": str, "float": float}

        for member in metadata["members"]:
            # Reconstruct the key
            key_type_name = member["key_type"]
            if key_type_name not in _key_constructors:
                raise ValueError(
                    f"Unsupported key type '{key_type_name}' in metadata. "
                    f"Only int, str, and float are supported."
                )

            key_constructor = _key_constructors[key_type_name]
            key = key_constructor(member["key"])

            # Load the CSV file
            csv_filename = member["filename"] + ".csv"
            csv_path = folder_path / csv_filename

            if not csv_path.exists():
                raise FileNotFoundError(f"CSV file not found: {csv_path}")

            df = pd.read_csv(csv_path, index_col=0)
            tsdf = TimeseriesDataframe.from_dataframe(df)

            # Restore metadata
            tsdf.name = member.get("name", "")
            tsdf.source = member.get("source", "")
            tsdf.description = member.get("description", "")
            if member.get("tags"):
                tsdf.add_tag(member["tags"])

            ensemble.ensemble[key] = tsdf

        return ensemble


class _TsdfJsonEncoder(json.JSONEncoder):
    """
    Custom JSON encoder for TimeseriesDataframe and DataframeEnsemble objects.
    """

    def _encode_tsdf(self, tsdf: 'TimeseriesDataframe') -> dict:
        """Encode a TimeseriesDataframe to a JSON-serializable dictionary."""
        metadata_dict = {}
        for field in TimeseriesDataframe._metadata:  # pylint: disable=protected-access
            metadata_dict[field] = getattr(tsdf, field, "")

        return {
            "__type__": "TimeseriesDataframe",
            "version": 1,
            "data": tsdf.to_dict(orient='split'),
            "metadata": metadata_dict
        }

    def _encode_ensemble(self, ensemble: 'DataframeEnsemble') -> dict:
        """Encode a DataframeEnsemble to a JSON-serializable dictionary."""
        encoded_data = {}
        for key, df in ensemble.ensemble.items():
            key_type_name = type(key).__name__
            # Use string representation of key as dict key (JSON requirement)
            # Store both the key value and its type for reconstruction
            encoded_data[str(key)] = {
                "key_type": key_type_name,
                "tsdf": self._encode_tsdf(df)
            }
        return {
            "__type__": "DataframeEnsemble",
            "version": 1,
            "data": encoded_data
        }

    def default(self, o):
        if isinstance(o, TimeseriesDataframe):
            return self._encode_tsdf(o)
        elif isinstance(o, DataframeEnsemble):
            return self._encode_ensemble(o)
        return super().default(o)


class _TsdfJsonDecoder(json.JSONDecoder):
    """
    Custom JSON decoder for TimeseriesDataframe and DataframeEnsemble objects.

    Uses the __type__ field to explicitly identify object types for robust deserialization.
    Implements the object_hook pattern for proper JSON decoding.
    """

    def __init__(self, *args, **kwargs):
        """Initialize decoder with custom object_hook."""
        super().__init__(object_hook=self._object_hook, *args, **kwargs)

    def _decode_tsdf(self, dct: dict) -> 'TimeseriesDataframe':
        """Decode a TimeseriesDataframe from a dictionary."""
        version = dct.get("version", 1)
        if version != 1:
            raise ValueError(f"Unsupported TimeseriesDataframe version: {version}")

        if "data" not in dct:
            raise ValueError("TimeseriesDataframe JSON missing required 'data' field")
        if "metadata" not in dct:
            raise ValueError("TimeseriesDataframe JSON missing required 'metadata' field")

        data = dct["data"]
        metadata = dct["metadata"]
        df = pd.DataFrame(**data)
        tsdf = TimeseriesDataframe.from_dataframe(df)

        # Restore all metadata fields automatically
        for field in TimeseriesDataframe._metadata:  # pylint: disable=protected-access
            if field in metadata:
                field_value = metadata[field]
                # Special handling for tags - use add_tag to maintain consistency
                if field == "tags" and field_value:
                    tsdf.add_tag(field_value)
                else:
                    setattr(tsdf, field, field_value)
        return tsdf

    def _decode_ensemble(self, dct: dict) -> 'DataframeEnsemble':
        """Decode a DataframeEnsemble from a dictionary."""
        version = dct.get("version", 1)
        if version != 1:
            raise ValueError(f"Unsupported DataframeEnsemble version: {version}")

        if "data" not in dct:
            raise ValueError("DataframeEnsemble JSON missing required 'data' field")

        ensemble = DataframeEnsemble()
        _key_constructors: dict[str, Any] = {
            "int": int, "str": str, "float": float, "NoneType": type(None),
        }

        for key_str, value in dct["data"].items():
            key_type_name = value["key_type"]
            if key_type_name not in _key_constructors:
                raise ValueError(f"Unsupported key type '{key_type_name}' in JSON data.")

            key_type = _key_constructors[key_type_name]
            # Reconstruct the key with proper type
            key = None if key_type is type(None) else key_type(key_str)

            # The TSDF has already been decoded by object_hook recursively
            # Just retrieve it from the value
            tsdf = value["tsdf"]
            # Direct assignment to preserve None keys (add_dataframe auto-assigns None to int)
            ensemble.assert_df_shape_matches_ensemble(tsdf)
            ensemble.ensemble[key] = tsdf
        return ensemble

    def _object_hook(self, dct: dict):
        """
        Custom object hook called for every JSON object decoded.

        Checks for __type__ field and dispatches to appropriate decoder.
        """
        obj_type = dct.get("__type__")

        if obj_type == "TimeseriesDataframe":
            return self._decode_tsdf(dct)
        elif obj_type == "DataframeEnsemble":
            return self._decode_ensemble(dct)

        # Return dict as-is if no recognized __type__
        return dct
