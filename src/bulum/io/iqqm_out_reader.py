"""
Read .OUT files by calling on `lqmgui`.
"""

from math import floor
import os
import subprocess
from typing import Any, Optional

import pandas as pd

from bulum import utils


class IqqmOutReader:
    """
    Requires the external program `iqmgui` to be on the PATH.

    Examples
    --------
    .. code-block:: python
        reader = IqqmOutReader("abcd01.OUT")
        reader.require(node=1)
        reader.require(node=23)
        df = reader.read()
    """

    def __init__(self, iqqm_out_filepath) -> None:
        self.iqqm_out_filepath = iqqm_out_filepath
        self.iqqm_out_folder = os.path.dirname(self.iqqm_out_filepath)
        self.iqqm_out_basename = os.path.basename(self.iqqm_out_filepath)[:-4]
        self.required: dict = {}
        """A dictionary of all nodes marked as 'required' i.e. to be read."""
        self.available: dict = {}
        """A dictionary of all nodes that are available to be read based off the .OUT file."""

        self._lqn_filename = None
        self._lqn_filepath = None
        self._csv_filename = None
        self._csv_filepath = None
        self._files_requiring_cleanup: list[str] = []
        self._search_available_data()

    def require(self, node: Optional[int | str] = None,
                supertype: Optional[float] = None, type: Optional[float] = None,
                output: Any = None) -> bool:
        """Mark a node or multiple nodes as 'required' i.e. for reading.
        At least one argument must be non-null.

        Returns
        -------
        bool
            `True` if at least one node was marked, `False` otherwise, likely
            indicating failure or a bad node specification.
        """
        if node is None and supertype is None and type is None and output is None:
            raise ValueError("At least one argument to require() must be non-null")
        # Coerce node and output into string formats padded with zeros.
        node = None if node is None else f"000{node}"[-3:]
        output = None if output is None else f"00{output}"[-2:]
        pre_num_nodes = len(self.required)
        # Now loop over all available records and identify the ones required by the user.
        for k, v in self.available.items():
            if (
                ((node is None) or (node == v["node"])) and
                ((supertype is None) or (supertype == v["supertype"])) and
                ((type is None) or (type == v["type"])) and
                ((output is None) or (output == v["output"]))
            ):
                self.required[k] = v
        return pre_num_nodes > len(self.required)

    def read(self, remove_temp_files=True, read_all_availabe=False, *,
             use_iqqmgui=True, iqqmgui_path=None) -> pd.DataFrame:
        """
        Read data.

        Parameters
        ----------
        remove_temp_files : bool, default True
            Clean up after yourself (remove artifacts from running ``iqmgui``)
        read_all_available : bool, default False
            Read all nodes instead of just those previously marked by the user
            as required.
        use_iqqmgui : bool, default True
            Should this use ``iqqmgui`` to extract data?

            .. note::
                Native python implementation is not yet here, so this argument **must** be ``True``.

        iqqmgui_path : str, optional
            If ``use_iqqmgui == True``, you can specify the executable to use to
            extract data.

        Returns
        -------
        pandas.DataFrame
        """
        if read_all_availabe:
            required = self.required
            self.required = self.available
        if use_iqqmgui:
            self._write_iqqmgui_lqn_file()
            self._call_iqqmgui_lqn(iqqmgui_path=iqqmgui_path)
            answer = self._read_iqqmgui_csv()
        else:
            raise NotImplementedError("Native python reading of .OUT files not yet supported.")
        if read_all_availabe:
            # Remember previous settings
            self.required = required
        if remove_temp_files:
            self._clean_up()
        return answer

    def _search_available_data(self):
        with open(self.iqqm_out_filepath, mode="r", encoding="UTF-8") as file:
            ss = file.readlines()
        # Read the recorder-flag matrix
        ss2 = ss[2].split()  # line 3 in the file
        n_node_types = int(ss2[0])
        n_output_types = int(ss2[1])
        recorder_flags = []
        for i in range(n_node_types):
            temp = ss[3 + i].split()
            recorder_flags.append(temp[0:n_output_types])
        # Read the date range
        ssx = ss[n_node_types + 3].split()  # 01/01/1890 31/12/2008  0
        self.start_dt_str = ssx[0].replace('/', ' ')
        self.end_dt_str = ssx[1].replace('/', ' ')
        # Read all the nodes (loop over the nodes)
        for i in range(n_node_types + 4, len(ss)):
            temp = ss[i]
            if str.strip(temp) == "":
                break
            node_number = f"000{int(temp[0:3])}"[-3:]  # 053
            node_name = str.strip(temp[3:20])  # 'Unallocated Irr'
            node_type = float(temp[20:])  # 8.3
            node_supertype = floor(node_type)  # 8
            for j in range(n_output_types):
                if recorder_flags[node_supertype][j] == "0":
                    continue
                recorder_number = f"000{j + 1}"[-2:]  # 03; recorder numbers start at 1
                temp = [node_number, recorder_number, node_name, node_type, node_supertype]
                self.available[f"{node_number}_{recorder_number}.d"] = {
                    "node": node_number,
                    "supertype": node_supertype,
                    "type": node_type,
                    "output": recorder_number
                }

    def _write_iqqmgui_lqn_file(self) -> None:
        """Generates an iqqmgui lqn file so that we can use iqqm to extract data to csv."""
        if (
            self._lqn_filename is None
            or self._lqn_filepath is None
            or self._csv_filename is None
            or self._csv_filepath is None
        ):
            raise RuntimeError("Bad order of operations in IqqmOutReader; ", "Using filenames/paths before they are defined.")
        self._lqn_filename = "temp.run"
        self._lqn_filepath = f"{self.iqqm_out_folder}/{self._lqn_filename}"
        with open(self._lqn_filepath, "w+", encoding='utf-8') as file:
            file.write("Listing file generated by bulum\n")
            file.write(f"{self.start_dt_str} {self.end_dt_str} /\n")  # 01 01 1890 31 12 2008 / start date, end date
            file.write(f"'{self.iqqm_out_basename}' /\n")  # 'O02l' / Name of IQN File
            file.write(f"{len(self.required)} 0 1 /\n")  # 17 0 1 /no files, no eqns, (no csv ?)
            i = 0
            for k, v in self.required.items():
                i += 1
                iqqm_ts_filepath = f"{self.iqqm_out_folder}/{k}"
                self._files_requiring_cleanup.append(iqqm_ts_filepath)
                file.write(f"'{k}' 00 00 00 00 T / {[i]}\n")  # 'O02l#030.01d' 00 00 00 00 T / [1]
                file.write("1 0 0 /\n")  # 1 0 0 /
                file.write(f"{v['node']} {v['output']} /\n")  # 030 1 /
            self._csv_filename = "temp.csv"
            self._csv_filepath = f"{self.iqqm_out_folder}/{self._csv_filename}"
            file.write(f"{self._csv_filename} /\n")  # DW_Diversions.csv /
            file.write(f"1-{i} /\n")  # 1-17 /
        self._files_requiring_cleanup.append(self._lqn_filepath)

    def _call_iqqmgui_lqn(self, *, iqqmgui_path=None) -> None:
        """Uses iqqmgui to extract data to csv."""
        if self._csv_filepath is None:
            raise RuntimeError("Order of operations: method called before csv written.")

        if iqqmgui_path:
            process = subprocess.Popen(f"{iqqmgui_path} {self._lqn_filename}",
                                       cwd=f"{self.iqqm_out_folder}")
        else:
            process = subprocess.Popen(f"iqmgui {self._lqn_filename}",
                                       cwd=f"{self.iqqm_out_folder}")
        process.wait()
        self._files_requiring_cleanup.append(self._csv_filepath)
        self._files_requiring_cleanup.append(f"{self.iqqm_out_folder}/iqqmml.txt")
        # ^^^ Artifact from running iqmgui

    def _read_iqqmgui_csv(self) -> pd.DataFrame:
        """Reads the csv from iqqmgui into a dataframe."""
        if self._csv_filepath is None:
            raise RuntimeError("Order of operations: method called before csv written.")
        df = pd.read_csv(self._csv_filepath)
        df.columns = ["Date"] + [c.strip() for c in df.columns[1:]]
        # df = utils.set_index_dt(df)
        df.set_index("Date", inplace=True)
        utils.assert_df_format_standards(df)
        return df

    def _clean_up(self) -> None:
        """Removes any file-artifacts created by this class."""
        for f in self._files_requiring_cleanup:
            os.remove(f)
        self._files_requiring_cleanup.clear()


class iqqm_out_reader(IqqmOutReader):  # pylint: disable=invalid-name
    """
    For backwards compatibility. See :class:`IqqmOutReader`.

    .. deprecated:: 0.3.0
        Non-pythonic naming 
    """
