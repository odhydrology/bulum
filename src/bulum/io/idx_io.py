import os
import pandas as pd
import uuid
import shutil
import subprocess
from bulum import utils
from .csv_io import *



def write_idx(df, filename, cleanup_tempfile=True):
    """_summary_

    Args:
        df (_type_): _description_
        filename (_type_): _description_
    """
    if shutil.which('csvidx') is None:
        raise Exception("This method relies on the external program 'csvidx.exe'. Please ensure it is in your path.")
    temp_filename = f"{uuid.uuid4().hex}.tempfile.csv"
    write_area_ts_csv(df, temp_filename)
    command = f"csvidx {temp_filename} {filename}"
    process = subprocess.Popen(command)
    process.wait()
    if cleanup_tempfile:
        os.remove(temp_filename)



def write_area_ts_csv(df, filename, units = "(mm.d^-1)"):
    """_summary_

    Args:
        df (_type_): _description_
        filename (_type_): _description_
        units (str, optional): _description_. Defaults to "(mm.d^-1)".

    Raises:
        Exception: If shortenned field names are going to clash in output file.
    """
    # ensures dataframe adheres to standards
    utils.assert_df_format_standards(df)
    # convert field names to 12 chars and check for collisions
    fields = {}
    for c in df.columns:
        c12 = f"{c[:12]:<12}"
        if c12 in fields.keys():
            raise Exception(f"Field names clash when shortenned to 12 chars: {c} and {fields[c12]}")
        fields[c12] = c
    # create the header text
    header = f"{units}"
    for k in fields.keys():
        header += f',"{k}"'
    header += os.linesep
    header += "Catchment area (km^2)"
    for k in fields.keys():
        header += f", 1.00000000"
    header += os.linesep
    # open a file and write the header and the csv body
    with open(filename, "w+", newline='', encoding='utf-8') as file:        
        file.write(header)
        df.to_csv(file, header=False, na_rep=' NaN')
        
        