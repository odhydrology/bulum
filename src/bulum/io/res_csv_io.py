""" 
IO functions for reading and writing .res.csv files.
"""

from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import os
from typing import Optional
from bulum import utils

_NA_VALUES: tuple[str, ...] = (
    '', ' ', 'null', 'NULL', 'NAN', 'NaN', 'nan',
    'NA', 'na', 'N/A', 'n/a', '#N/A', '#NA', '-NaN', '-nan',
)


def read_res_csv(filename: str | os.PathLike, custom_na_values=None,
                 df: Optional[pd.DataFrame] = None, colprefix=None,
                 allow_nonnumeric=False, use_field_name=False,
                 **kwargs) -> utils.TimeseriesDataframe:
    """Read a ``.res.csv`` data file into a DataFrame with a standardized date index.

    Parameters
    ----------
    filename : str or PathLike
        Path to the ``.res.csv`` file.
    custom_na_values : list of str, optional
        Overrides the automatically-determined missing values. If None, missing
        values include those declared in the file header plus the defaults::

            ['', ' ', 'null', 'NULL', 'NAN', 'NaN', 'nan', 'NA', 'na',
             'N/A', 'n/a', '#N/A', '#NA', '-NaN', '-nan']

    df : pd.DataFrame, optional
        If provided, the reader appends columns to this dataframe. Default is None.
    colprefix : str, optional
        If provided, prepended to each column name. Default is None.
    allow_nonnumeric : bool, optional
        If False, raises if any column is not numeric. Default is False.
    use_field_name : bool, optional
        If True, replaces column names with the field names from the file
        header. Default is False.

    Returns
    -------
    utils.TimeseriesDataframe

    Raises
    ------
    ValueError
        If a column is not numeric and ``allow_nonnumeric`` is False, or if
        the new date range does not overlap with ``df``.
    """
    if custom_na_values is None:
        na_values = list(_NA_VALUES)
    else:
        na_values = custom_na_values
    # If no df was supplied, instantiate a new one
    if df is None:
        df = pd.DataFrame()
    # Scrape through the header
    metadata_lines = []
    eoh_found = False
    with open(filename, encoding='UTF-8') as f:
        line = ""
        for line in f:
            metadata_lines.append(line)
            if line.strip().startswith("EOH"):
                eoh_found = True
                break
            if custom_na_values is None and line.strip().lower().startswith("missing data value,"):
                new_na_value = line.strip()[len("missing data value,"):]
                # e.g. "-9999"
                na_values.append(new_na_value)
    if not eoh_found:
        raise ValueError("File is not a valid .res.csv file")
    col_header_line_number = len(metadata_lines) - 2
    lines_to_skip = list(range(col_header_line_number)) + \
        [col_header_line_number + 1]
    # Read the data
    temp = pd.read_csv(filename, na_values=na_values, skiprows=lines_to_skip)
    # Date index
    temp.set_index(temp.columns[0], inplace=True)
    temp.index = utils.standardize_datestring_format(temp.index, as_index=True)
    temp.index.name = "Date"
    # Check values
    if not allow_nonnumeric:
        for col in temp.columns:
            if not np.issubdtype(temp[col].dtype, np.number):  # type: ignore
                raise ValueError(f"Column '{col}' is not numeric")
    # Replace column names with field name if required
    if use_field_name:
        field_count = -2                                         #i'm using this -2 value to mean do nothing
        for line in metadata_lines:
            line = line.strip()
            if line == "EOC":
                field_count = -1                                 #i'm using this -1 value to mean the field count will be defined on the next line
            elif field_count == -1:
                field_count = int(line.strip())                  #field count. Field properties will start on the next line
            elif field_count > 0:                                #this means we are in the 
                field_properties = line.split(",")               #all properties of the current field
                field_number = int(field_properties[0])          #field number should be at index 0
                field_name = field_properties[5]                 #field name should be at index 5
                c = temp.columns[field_number - 1]
                temp.rename(columns={c: field_name}, inplace=True)
                is_last_field = (field_number >= field_count)
                if is_last_field:
                    field_count = -2                             # reset field count to stop processing field names
            else:
                field_count = -2  # reset field count to stop processing field names
    # Add column prefix if required
    if colprefix is not None:
        for c in temp.columns:
            temp.rename(columns={c: f"{colprefix}{c}"}, inplace=True)
    # Join to existing dataframe if required
    if df is None:
        df = temp
    else:
        if len(df) > 0:
            # Check that the dates overlap
            newdf_ends_before_df_starts = temp.index[0] < df.index[-1]
            df_ends_before_newdf_starts = df.index[-1] < temp.index[0]
            if newdf_ends_before_df_starts or df_ends_before_newdf_starts:
                raise ValueError("The dates in the new dataframe do not overlap with the existing dataframe!")
        df = df.join(temp, how="outer")
    # utils.assert_df_format_standards(df)
    return utils.TimeseriesDataframe.from_dataframe(df)


def write_res_csv(df: pd.DataFrame, 
                  filepath: str | Path = "out.res.csv", 
                  file_version=3, 
                  missing_data_value="", 
                  project_name="", 
                  source_version="5.30.0.12728", 
                  datetime_format=r"%d/%m/%y") -> None:
    """Write a dataframe to a ``.res.csv`` file.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to write. Must have a ``"Date"`` index or a date column as
        the first column.
    filepath : str or Path, optional
        Path to the output file including extension. Default is ``"out.res.csv"``.
    file_version : int, optional
        File version number written to the header. Default is 3.
    missing_data_value : str, optional
        Value used to represent missing data. Default is ``""``.
    project_name : str, optional
        Project name written to the header. Default is ``""``.
    source_version : str, optional
        Source version string written to the header. Default is ``"5.30.0.12728"``.
    datetime_format : str, optional
        Format string for dates in the output. Default is ``"%d/%m/%y"``.
    """
    # Get metadata
    now = datetime.now().strftime(r"%d/%m/%Y %H:%M")
    num_fields = len(df.columns)

    # Date index
    df_date_index = df.index.name == "Date"
    if not df_date_index:
        df.set_index(df.columns[0], inplace=True)
        df.index = utils.standardize_datestring_format(df.index, as_index=True)
        df.index.name = "Date"

    utils.convert_index_to_string(df)
    first_date: datetime = df.index[0]
    last_date: datetime = df.index[-1]

    with open(filepath, 'w', encoding="UTF-8") as f:
        f.write(f"File version,{file_version}\n")
        f.write(f"Missing data value,{missing_data_value}\n")
        f.write("EOM\n")
        f.write(f"Project name,{project_name}\n")
        f.write(f"Source version,{source_version}\n")
        f.write(f"Latest result run time,{now}\n")
        f.write(f"Simulation time,{first_date} - {last_date}\n")
        f.write("Field,Units,RunName,ScenarioName,ScenarioInputSetName,Name,Site,ElementName,WaterFeatureType,ElementType,Structure,Custom\n")
        f.write("EOC\n")
        f.write(f"{num_fields}\n")
        for idx, column_name in enumerate(df.columns):
            # f.write(f"{idx+1},{units},{run_name},{scenario_name},{scenario_input_set_name},{name},{site},{element_name},{water_feature_type},{element_type},{structure},{custom}\n")
            f.write(f"{idx+1},,,,,{column_name},,,,,,\n")
        # HEADERS
        f.write("Date,")
        for i, c in enumerate(df.columns):
            f.write(c)
            if i != num_fields - 1:
                f.write(',')
            else:
                f.write('\n')
        f.write("EOH\n")
    df.to_csv(filepath, header=False, mode='a',
              date_format=datetime_format, na_rep=missing_data_value)

    # restore state of df index
    if not df_date_index:
        df.reset_index(inplace=True)
