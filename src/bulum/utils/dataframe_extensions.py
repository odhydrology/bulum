"""
Provides extensions to dataframes which facilitates tracking and bulk analysis.

TimeseriesDataframes (TSDF) are a wrapper around pandas dataframes, with extra
fields (tags, name, source, ...) and methods that facilitate working with these
fields.

DataframeEnsembles are a way to organise multiple TSDFs, with methods that work
(at present) primarily with the tags associated with TSDFs.

"""

import enum
import re
from typing import Any, Optional, Union

from collections.abc import Iterable

import pandas as pd


class RegexArg(enum.Enum):
    """Specifies the type of argument supplied to filtering functions in TSDF and """
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
    """

    TAG_DELIMITER = ','
    """Used to consistently separate tags. 
    Kept as a variable for semantic purposes."""

    def __init__(self, *, name="", source="", description="") -> None:
        """
        Parameters
        ----------
        name : str 
        source : str
        description : str

        See also
        --------
        `TimeseriesDataframe.add_tag()` to add tags.
        """
        super().__init__()
        self.name = name
        self.source = source
        self.description = description
        self.tags = ""

    def copy_from_dataframe(self, df):
        super().__init__(df)

    @classmethod
    def from_dataframe(cls, df, **kwargs):
        tsdf = cls(**kwargs)
        tsdf.copy_from_dataframe(df)
        return tsdf

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

    def add_tag(self, tag: str, check_membership: bool = False) -> None:
        """Add a tag to the TimeseriesDataframe.

        This is the canonical way to add tags to a TimeseriesDataframe. It can
        add multiple tags separated by the designated tag delimiter (by default,
        a comma ,).

        Examples
        --------
        The `check_membership` flag will ensure that `tag` does not match with
        existing tags, but will not (at present) check the other way around. For
        example, the following will not raise an error: 
        ``` 
        df.add_tag("01", True)
        df.add_tag("01a", True)
        ```
        """
        tag = tag.strip()
        if self.TAG_DELIMITER in tag:
            tags = [x for x in tag.split(",") if x != ""]
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
        return len(self.tags.split(self.TAG_DELIMITER))


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
        return len(self.ensemble)

    def get(self, key: Any | None = None) -> TimeseriesDataframe:
        """Return the underlying dataframe if the ensemble is a singleton, or
        the dataframe at the given key."""
        if key is None:
            if len(self.ensemble) == 1:
                return next(iter(self.ensemble.values()))
            else:
                raise ValueError("DataframeEnsemble.get() was ")
        else:
            return self.ensemble.get(key)

    def add_dataframe(self, df: pd.DataFrame | TimeseriesDataframe, key: Any | None = None, tag: str | None = None) -> None:
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

    def filter_tag(self, tag: str, *, exclude: bool = False, **kwargs) -> 'DataframeEnsemble':
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
