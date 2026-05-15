""" 
General use IO functions.
"""
import os
import re
from pathlib import Path

import bulum.io as bio
from bulum import utils


def read(filename: str | os.PathLike, **kwargs) -> utils.TimeseriesDataframe:
    """Read a timeseries file, dispatching to the appropriate reader based on file extension.

    Supported formats: ``.res.csv``, ``.csv``, ``.idx``, and IQQM listquan
    outputs (e.g. ``.01d``).

    Parameters
    ----------
    filename : str or PathLike
        Path to the file to read.
    **kwargs
        Passed through to the underlying reader function.

    Returns
    -------
    utils.TimeseriesDataframe

    Raises
    ------
    ValueError
        If the file extension is not recognised, or if a ``.res.csv`` file
        cannot be parsed.
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
