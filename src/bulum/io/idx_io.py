""" 
IO functions for IDX files. 

See also :py:mod:`bulum.op.idx_io_native`.
"""
import os
import shutil
import subprocess
import uuid
from pathlib import Path

import pandas as pd

from bulum import utils


def write_idx(df: pd.DataFrame, filename: str | Path, cleanup_tempfile=True,
              *, exist_ok: bool = True):
    """Write IDX file from dataframe, requires csvidx.exe.

    Parameters
    ----------
    df : DataFrame
        DataFrame to write.
    filename : str or Path
        Path to the file to write to.
        Will overwrite any existing file if `exist_ok` is `True`.
        May be a str for backwards compatibility.

    Raises
    ------
    FileExistsError
        If `exist_ok` is `True` and `filename` already exists.
    FileNotFoundError
        If csvidx.exe is not found on path.
    """
    if isinstance(filename, str):
        filename = Path(filename)
    if not exist_ok and filename.exists():
        raise FileExistsError(f"{filename.name} already exists!")
    if shutil.which('csvidx') is None:
        raise FileNotFoundError("This method relies on the external program 'csvidx.exe'. ",
                                "Please ensure it is in your path.")
    temp_filename = filename.with_name(f"{uuid.uuid4().hex}.tempfile.csv")
    write_area_ts_csv(df, temp_filename)
    command = f"csvidx {temp_filename} {filename}"
    _ = subprocess.run(command, check=True)
    if cleanup_tempfile:
        os.remove(temp_filename)


def write_area_ts_csv(df: pd.DataFrame, filename, units="(mm.d^-1)"):
    """_summary_

    Parameters
    ----------
    df : DataFrame
    filename
    units : str, optional
        Defaults to "(mm.d^-1)".

    Raises
    ------
    Exception
        If shortened field names are going to clash in output file.
    """
    # ensures dataframe adheres to standards
    utils.assert_df_format_standards(df)
    # convert field names to 12 chars and check for collisions
    fields = {}
    for c in df.columns:
        c12 = f"{c[:12]:<12}"
        if c12 in fields:
            raise Exception(f"Field names clash when shortened to 12 chars: {c} and {fields[c12]}")
        fields[c12] = c
    # create the header text
    header = f"{units}"
    for k in fields:
        header += f',"{k}"'
    header += os.linesep
    header += "Catchment area (km^2)"
    for k in fields:
        header += ", 1.00000000"
    header += os.linesep
    # open a file and write the header and the csv body
    with open(filename, "w+", newline='', encoding='utf-8') as file:
        file.write(header)
        df.to_csv(file, header=False, na_rep=' NaN')
