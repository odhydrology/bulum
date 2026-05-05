"""
Provides extensions to dataframes which facilitates tracking and bulk analysis.

TimeseriesDataframes (TSDF) are a wrapper around pandas dataframes, with extra
fields (tags, name, source, ...) and methods that facilitate working with these
fields.

DataframeEnsembles are a way to organise multiple TSDFs, with methods that work
(at present) primarily with the tags associated with TSDFs.

"""

import enum
from pathlib import Path
import re
from typing import Any, Iterable, Optional, Union

import pandas as pd


class RegexArg(enum.Enum):
    """Specifies the type of argument supplied to filtering functions in TSDF and DataframeEnsemble."""
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

    TAG_DELIMITER = ','
    """Used to consistently separate tags.
    Kept as a variable for semantic purposes."""

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
            As of version 0.4.0, most pandas operations automatically preserve
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
    have the same index, and the same columns."""

    def __init__(self, dfs: Iterable[TimeseriesDataframe] | None = None) -> None:
        """
        Args:
            dfs: A collection of dataframes to add to the ensemble.
        """
        self.ensemble: dict[Any, TimeseriesDataframe] = {}
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

    def get(self, key: Optional[Any] = None) -> TimeseriesDataframe:
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

    def add_dataframe(self, df: Union[pd.DataFrame, TimeseriesDataframe], key: Optional[Any] = None, tag: Optional[str] = None) -> None:
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
