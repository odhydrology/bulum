""" 
General use IO functions.
"""
import os
import re
from pathlib import Path

import bulum.io as bio
from bulum import utils


def read(filename: str | os.PathLike, **kwargs) -> utils.TimeseriesDataframe:
    """
    Read the input file.

    It will attempt to determine the filetype and dispatch to the appropriate
    function in `bulum.io`.
    """
    if not isinstance(filename, Path):
        filename = Path(filename)
    filename_suffix = filename.suffix.lower()

    df = None
    if filename.name.lower().endswith(".res.csv"):
        df = bio.read_res_csv(filename, **kwargs)
        if df is None:
            raise ValueError("Res csv could not be read.")
    elif filename_suffix == ".csv":
        df = bio.read_ts_csv(filename, **kwargs)
    elif filename_suffix == ".idx":
        df = bio.read_idx(filename, **kwargs)
    elif re.search(".[0-9a-f]{2}d$", filename_suffix):
        df = bio.read_iqqm_lqn_output(filename, **kwargs)
    else:
        raise ValueError(f"Unknown file extension: {filename}")

    assert isinstance(df, utils.TimeseriesDataframe), \
        "Output of `read` is not a TimeseriesDataframe."
    return df
