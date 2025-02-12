import os
import pandas as pd
import numpy as np
from bulum import utils


def _detect_header_bytes(b_data: np.ndarray) -> bool:
    """
    Helper function for read_idx. Detects whether the OUT file was written with
    a version of IQQM with an old compiler with metadata/junk data as a header.
    Fails if (not necessarily only if) the run was undertaken with only one
    source of data, i.e. the .idx file has only one entry.

    Args:
        b_data (np.ndarray): 2d array of binary data filled with float32 data. 
    """
    b_data_slice: tuple[np.float32] = b_data[0]
    first_non_zero = b_data_slice[0] != 0.0
    rest_zeroes = not np.any(list(b_data_slice)[1:])
    return first_non_zero and rest_zeroes


def read_idx(filename, skip_header_bytes=None) -> utils.TimeseriesDataframe:
    """_summary_

    Args:
        filename (_type_): Name of the IDX file.
        skip_header_bytes (bool | None): Whether to skip header bytes in the IDX
          file (related to the compiler used for IQQM). If set to None, attempt
          to detect the presence of header bytes automatically.

    Returns:
        utils.TimeseriesDataframe: _description_
    """
    if not os.path.exists(filename):
        raise FileNotFoundError(f"File does not exist: {filename}")
    # Read ".idx" file
    with open(filename, 'r') as f:
        # Skip line
        stmp = f.readline()
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
    out_filename = filename.lower().replace('.idx', '.out')
    if not os.path.exists(out_filename):
        raise FileNotFoundError(f"File does not exist: {out_filename}")
    # 4-byte reals
    b_types = [(s, 'f4') for s in snames]
    # Read all data in, drop header bytes (first row) if necessary
    b_data = np.fromfile(out_filename, dtype=np.dtype(b_types))
    # Detection of header bytes
    if skip_header_bytes is None:
        skip_header_bytes = _detect_header_bytes(b_data)
    if skip_header_bytes:
        b_data = b_data[1:]  # skip header bytes
    # Read data
    if date_flag == 0:
        daily_date_values = utils.datetime_functions.get_dates(
            date_start, end_date=date_end, include_end_date=True)         
        df = pd.DataFrame.from_records(b_data, index=daily_date_values)
        df.columns = snames
        df.index.name = "Date"
        # Check data types. If not 'float64' or 'int64', convert to 'float64'
        x = df.select_dtypes(exclude=['int64','float64']).columns
        if x.__len__()>0:
            df=df.astype({i: 'float64' for i in x})
    elif date_flag == 1:
        raise NotImplementedError("Monthly data not yet supported")
    elif date_flag == 3:
        raise NotImplementedError("Annual data not yet supported")
    else:
        raise ValueError(f"Unsupported date interval: {date_flag}")
    utils.assert_df_format_standards(df)
    return utils.TimeseriesDataframe.from_dataframe(df)


def write_idx_native(df: pd.DataFrame, filepath, type="None", units="None") -> None:
    """Writer for .IDX and corresponding .OUT binary files written in native Python.
    Currently only supports daily data (date flag 0), as with the reader read_idx(...). 

    Assumes that data are homogeneous in units and type e.g. Precipitation & mm resp., or Flow & ML/d.

    Args:
        df (pd.Dataframe): DataFrame as per the output of read_idx(...).
        filepath (str)   : Path to the IDX file to be written to including .IDX extension.
        units (str, optional)      : Units for data in df. 
        type (str, optional)       : Data specifier for data in df, e.g. Gauged Flow, Precipitation, etc.
    """
    date_flag = 0
    # TODO: When generalising to other frequencies, we may be able to simply read the data type off the time delta in df.index values
    # As is, I've essentially copied what was done in the reader to flag that this should be implemented at the "same time".
    # Verify valid date_flag
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
        def ljust_or_truncate(s, l): return s.ljust(l)[0:l]
        for idx, col_name in enumerate(col_names):
            source_entry = ljust_or_truncate(f"df_col{idx+1}", 12)
            name_entry = ljust_or_truncate(f"{col_name}", 40)
            type_entry = ljust_or_truncate(f"{type}", 15)
            units_entry = ljust_or_truncate(f"{units}", 15)
            f.write(f"{source_entry} {name_entry}" +
                    f" {type_entry} {units_entry}\n")
    # write binary
    out_filepath = filepath.lower().replace('.idx', '.out')
    df.to_numpy().tofile(out_filepath)
    return
