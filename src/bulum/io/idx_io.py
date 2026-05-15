"""
IO functions for IDX files and IQQM .OUT files.

This module provides functions for reading and writing IDX files (both the text
index files and their corresponding binary .OUT data files) used by the IQQM
hydrological model. Functions include both native Python implementations and
utilities that rely on external tools like csvidx.exe.
"""
import os
import shutil
import subprocess
import uuid
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from bulum import utils


def write_idx(df: pd.DataFrame, filename: str | Path, cleanup_tempfile: bool = True,
              *, exist_ok: bool = True) -> None:
    """Write IDX file from dataframe using csvidx.exe.

    This function creates both an .IDX index file and a corresponding .OUT binary
    file by first writing a temporary CSV file and then calling the external
    csvidx.exe utility.

    Parameters
    ----------
    df : DataFrame
        DataFrame with datetime index to write.
    filename : str or Path
        Path to the IDX file to write. Will overwrite any existing file if
        `exist_ok` is `True`.
    cleanup_tempfile : bool, default=True
        Whether to remove the temporary CSV file after conversion.
    exist_ok : bool, default=True
        If False, raise FileExistsError if the file already exists. If True,
        allow overwriting existing files.

    Raises
    ------
    FileExistsError
        If `exist_ok` is `False` and `filename` already exists.
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


def write_area_ts_csv(df: pd.DataFrame, filename: str | Path, units: str = "(mm.d^-1)") -> None:
    """Write timeseries data to area-weighted CSV format for use with csvidx.

    This function writes a DataFrame to a CSV file in a specific format used by
    the csvidx tool, with column names truncated to 12 characters and a header
    row containing catchment area information (defaulting to 1.0 km^2 for all
    columns).

    Parameters
    ----------
    df : DataFrame
        DataFrame with datetime index to write.
    filename : str or Path
        Path to the output CSV file.
    units : str, default="(mm.d^-1)"
        Units string to write in the header row.

    Raises
    ------
    ValueError
        If column names clash when truncated to 12 characters.
    """
    # ensures dataframe adheres to standards
    utils.assert_df_format_standards(df)
    # convert field names to 12 chars and check for collisions
    fields: dict[str, str] = {}
    for c in df.columns:
        c12 = f"{c[:12]:<12}"
        if c12 in fields:
            raise ValueError(f"Field names clash when shortened to 12 chars: {c} and {fields[c12]}")
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


def _detect_header_bytes(b_data: np.ndarray, expected_rows: Optional[int] = None) -> bool:
    """
    Helper function for :func:`read_idx`. Detects whether the .OUT file was
    written with a version of IQQM with an old compiler with metadata/junk data
    as a header.

    For multi-column files, detection is based on the pattern of the first row
    (first value non-zero, rest zero). For single-column files, detection is
    based on comparing the actual data length to the expected number of rows.

    Parameters
    ----------
    b_data : np.ndarray
        Structured array of binary data filled with float32 data
    expected_rows : int, optional
        Expected number of data rows based on date range. If provided, enables
        length-based detection for single-column files.

    Returns
    -------
    bool
        True if header bytes should be skipped, False otherwise
    """
    b_data_slice: tuple[np.float32] = b_data[0]

    # For single-column files, use length-based detection if expected_rows is provided
    if len(b_data_slice) == 1 and expected_rows is not None:
        # If we have exactly one more row than expected, it's likely a header byte
        if len(b_data) == expected_rows + 1:
            return True
        # If we have the expected number of rows, no header bytes
        elif len(b_data) == expected_rows:
            return False
        # If lengths don't match expectations, fall back to no header detection
        # (let the error surface later in DataFrame construction)
        else:
            return False

    # For multi-column files, use pattern-based detection
    if len(b_data_slice) > 1:
        first_non_zero = b_data_slice[0] != 0.0
        rest_zeroes = not np.any(list(b_data_slice)[1:])
        return first_non_zero and rest_zeroes

    # Default for single-column without expected_rows: assume no header bytes
    return False


def read_idx(filename: str | Path, skip_header_bytes: Optional[bool] = None) -> utils.TimeseriesDataframe:
    """Read IDX file and corresponding IQQM .OUT binary file.

    This function reads an IQQM .IDX index file and its corresponding .OUT binary
    data file, returning the data as a DataFrame. Currently only supports daily
    data (date_flag=0).

    Parameters
    ----------
    filename : str or Path
        Path to the IDX file. The corresponding .OUT file is expected to be in
        the same directory with the same base name.
    skip_header_bytes : bool, optional
        Whether to skip header bytes in the corresponding .OUT file. Some versions
        of IQQM compiled with older compilers include metadata/junk data as a
        header row. If None (default), attempts automatic detection based on file
        structure.

    Returns
    -------
    utils.TimeseriesDataframe
        DataFrame with datetime index and columns named as
        "{num}>{source_file}>{description}".

    Raises
    ------
    FileNotFoundError
        If the IDX file or corresponding OUT file does not exist.
    NotImplementedError
        If the file contains monthly (date_flag=1) or annual (date_flag=3) data.
    ValueError
        If the date_flag in the file is not 0, 1, or 3.
    """
    if not isinstance(filename, Path):
        filename = Path(filename)
    if not (filename.exists() and filename.is_file()):
        raise FileNotFoundError(f"File does not exist: {filename}")
    # Read ".idx" file
    with open(filename, 'r', encoding="UTF-8") as f:
        # Skip line
        _ = f.readline()
        # Start date, end date, date interval
        stmp = f.readline().split()
        date_start = utils.standardize_datestring_format([stmp[0]])[0]
        date_end = utils.standardize_datestring_format([stmp[1]])[0]
        date_flag = int(stmp[2])
        snames = []
        for n, line in enumerate(f):
            sfile = line[0:13].strip()
            sdesc = line[13:54].strip()
            sname = f"{n + 1}>{sfile}>{sdesc}"
            snames.append(sname)
    # Read ".out" file
    out_filename = filename.with_suffix(".out")
    if not os.path.exists(out_filename):
        raise FileNotFoundError(f"File does not exist: {out_filename}")
    # 4-byte reals
    b_types = [(s, 'f4') for s in snames]
    # Read all data in, drop header bytes (first row) if necessary
    b_data = np.fromfile(out_filename, dtype=np.dtype(b_types))
    # Calculate date values for header detection and DataFrame construction
    if date_flag == 0:
        daily_date_values = utils.datetime_functions.get_dates(
            date_start, end_date=date_end, include_end_date=True)
    else:
        raise NotImplementedError(f"Unsupported date interval: {date_flag}")
    # Detection of header bytes
    if skip_header_bytes is None:
        expected_rows = None
        if date_flag == 0:
            expected_rows = len(daily_date_values)
        # Note: For date_flag 1 (monthly) and 3 (annual), we don't calculate
        # expected_rows since these are not yet implemented
        skip_header_bytes = _detect_header_bytes(b_data, expected_rows=expected_rows)
    if skip_header_bytes:
        b_data = b_data[1:]  # skip header bytes
    # Read data
    if date_flag == 0:
        df = pd.DataFrame.from_records(b_data, index=daily_date_values)
        df.columns = snames  # type: ignore
        df.index.name = "Date"
        # Check data types. If not 'float64' or 'int64', convert to 'float64'
        x = df.select_dtypes(exclude=['int64', 'float64']).columns
        if len(x) > 0:
            df = df.astype({i: 'float64' for i in x})
    elif date_flag == 1:
        raise NotImplementedError("Monthly data not yet supported")
    elif date_flag == 3:
        raise NotImplementedError("Annual data not yet supported")
    else:
        raise ValueError(f"Unsupported date interval: {date_flag}")
    utils.assert_df_format_standards(df)
    return utils.TimeseriesDataframe.from_dataframe(df)


def write_idx_native(df: pd.DataFrame, filepath: str | Path, type: str = "None", units: str = "None") -> None:
    """Write IDX and OUT binary files using pure Python (no external tools).

    This function writes both an .IDX index file and a corresponding .OUT binary
    file using native Python, without requiring external tools like csvidx.exe.
    Currently only supports daily data (date_flag=0), matching the capabilities
    of :func:`read_idx`.

    The function assumes all columns in the DataFrame share the same units and
    data type (e.g., all Precipitation in mm, or all Flow in ML/d).

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with datetime index containing the data to write. Should
        follow the same format as output from :func:`read_idx`.
    filepath : str or Path
        Path to the IDX file to write (including .IDX extension). The
        corresponding .OUT file will be created with the same base name.
    type : str, default="None"
        Data type specifier for all columns in df, e.g., "Gauged Flow",
        "Precipitation", "Evaporation", etc.
    units : str, default="None"
        Units for all data in df, e.g., "mm", "ML/d", "mm/day", etc.

    Notes
    -----
    - The first line of the IDX file contains version/timestamp metadata
      (currently a placeholder copied from reference files).
    - Column names in the output are truncated/padded to fit the IDX format
      specifications (12 chars for source, 40 for description, etc.).
    - The .OUT file is written in binary format as 32-bit floats (float32).

    See Also
    --------
    read_idx : Read IDX and OUT files.
    write_idx : Write IDX files using the external csvidx.exe tool.
    """
    # Convert filepath to Path for consistent handling
    if not isinstance(filepath, Path):
        filepath = Path(filepath)

    date_flag = 0
    # TODO: When generalising to other frequencies, we may be able to simply
    # read the data type off the time delta in df.index values As is, I've
    # essentially copied what was done in the reader to flag that this should be
    # implemented at the "same time". Verify valid date_flag
    match date_flag:
        case 0:
            pass  # valid
        case 1:
            raise NotImplementedError("Monthly data not yet supported")
        case 3:
            raise NotImplementedError("Annual data not yet supported")
        case _:
            raise ValueError(f"Unsupported date interval: {date_flag}")

    utils.assert_df_format_standards(df)
    first_date = df.index[0]
    last_date = df.index[-1]
    col_names = df.columns

    # write index
    with open(filepath, 'w') as f:
        # TODO: check whether this "skipped" line has important info
        # For now I've just copied the data from ./tests/BUR_FLWX.IDX as it's likely just metadata.
        f.write('6.36.1 06/11/2006 10:48:30.64\n')
        f.write(f"{first_date} {last_date} {date_flag}\n")
        # data
        # inline fn to ensure padded string is exactly l characters long

        def ljust_or_truncate(s, l):
            return s.ljust(l)[0:l]
        for idx, col_name in enumerate(col_names):
            source_entry = ljust_or_truncate(f"df_col{idx+1}", 12)
            name_entry = ljust_or_truncate(f"{col_name}", 40)
            type_entry = ljust_or_truncate(f"{type}", 15)
            units_entry = ljust_or_truncate(f"{units}", 15)
            f.write(f"{source_entry} {name_entry}" +
                    f" {type_entry} {units_entry}\n")
    # write binary
    # Replace extension with .out (handles .idx, .IDX, etc.)
    out_filepath = filepath.with_suffix('.out')
    # Convert to structured array to match the format expected by read_idx
    # Each record should contain all columns for one row
    records = df.to_records(index=False, column_dtypes='f4')
    records.tofile(out_filepath)
    return
