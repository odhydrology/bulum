# confer https://qldhyd.atlassian.net/wiki/spaces/MET/pages/524386/Negflo

"""
The input file contains the following information:

1. Start Date and End Date
2. File Type
   = 0 for old IQQM format
   = 1 for IQMM GUI format
   = 2 for SOURCE input file (with no header lines)
   = 3 for SOURCE Export file (with 6 header lines)
   No of Header Lines
3. observed Flow File name
4. File Type
   = 0 for old IQQM format
   = 1 for IQMM GUI format
   = 2 for SOURCE input file (with no header lines)
   = 3 for SOURCE Export file (with 6 header lines)
   No of Header Lines
5. Modelled Flow File name
6. File Type
   = 0 for old IQQM format
   = 1 for IQMM GUI format
   = 2 for SOURCE input file (with no header lines)
   = 3 for SOURCE Export file (with 6 header lines)
   No of Header Lines
7. Residual Flow File Name
8. Flow Limit
9. No of Segments (set to zero if want to smooth the whole period specified above)
10. Start and End Date of Segment.

A typical input file is given below.

14 09 1979 30 09 2008
Booncomb.sm9
s12b1.sim
residual.qqm
0
1
14 09 1979 30 09 2008
"""

import itertools
import logging
from enum import Enum
from typing import Optional
from collections.abc import MutableSequence

import numpy as np
import pandas as pd

import bulum.io as oio
import bulum.utils as utils
from bulum.utils import TimeseriesDataframe

logger = logging.getLogger(__name__)


class FileType(Enum):
    IQQM = 0
    IQQM_GUI = 1
    SOURCE_INPUT = 2
    SOURCE_OUTPUT = 3


class _AnalysisType(Enum):
    RAW = -1
    CLIPPED = 0
    SMOOTHED_ALL = 1  # sm1
    SMOOTHED_FORWARD = 2  # sm2
    SMOOTHED_FORWARD_NO_CARRY = 3  # sm3
    SMOOTHED_BACKWARD = 4  # sm4
    SMOOTHED_BACKWARD_NO_CARRY = 5  # sm5
    SMOOTHED_SPECIFIED = 6  # sm6
    SMOOTHED_NEG_LIM = 7  # sm7

    @staticmethod
    def to_file_extension(t) -> str:
        match t:
            case _AnalysisType.RAW:
                return ".rw1"
            case _AnalysisType.CLIPPED:
                return ".cl1"
            case _AnalysisType.SMOOTHED_ALL:
                return ".sm1"
            case _AnalysisType.SMOOTHED_FORWARD:
                return ".sm2"
            case _AnalysisType.SMOOTHED_FORWARD_NO_CARRY:
                return ".sm3"
            case _AnalysisType.SMOOTHED_BACKWARD:
                return ".sm4"
            case _AnalysisType.SMOOTHED_BACKWARD_NO_CARRY:
                return ".sm5"
            case _AnalysisType.SMOOTHED_SPECIFIED:
                return ".sm6"
            case _AnalysisType.SMOOTHED_NEG_LIM:
                return ".sm7"
            case default:
                raise ValueError(f"Invalid enum supplied, {t}")


class Negflo:
    """https://qldhyd.atlassian.net/wiki/spaces/MET/pages/524386/Negflo

    # Overview copied from the above page:
    NEGFLO6 can be used to calculate the difference between two IQQM format
    files. It is especially useful for calculating the residual catchment inflow
    by subtracting the upstream gauged flow routed through an IQQM reach model
    from the flow recorded at the downstream gauge.

    Because the routing in the IQQM may not match exactly the routing in the
    actual system, the subtraction can generate negative flows. If the flows are
    set to zero, the resultant modelled flows can significantly exceed the
    observed downstream flow. This can cause a particular problem if the reach
    flows into a dam, so the final model tends to predict higher dam levels than
    the recorded levels. Therefore, NEGFLO6 includes a number of methods for
    correcting the positive flows.

    If the residual flow contains a large number of negative flows, the
    following should be carried out.
      1. Review the routing and delays to see if the number of negative flows
         can be reduced.
      2. If the routed upstream flows are consistently greater than the observed
         downstream flows when there is no residual inflow, then review the
         ratings of the gauges.
      3. Consider including a waterhole and/or losses in the model before
         undertaking the residual inflow derivation.
    """

    def __init__(self,
                 # TODO: should these be tsdfs or should we just pass in one series? need to see functionality of negflo program.
                 df_residual: pd.DataFrame,
                 flow_limit: float,
                 #  num_segments: int,
                 #  segment_start_date: pd.DatetimeIndex, segment_end_date: pd.DatetimeIndex
                 ):
        super().__init__()
        # self.df_observed = df_observed
        # self.df_modelled = df_modelled
        # TODO how to determine which column to run this on? should we be
        #      enforcing *only* one column here? run it on all columns? maybe
        #      instead of passing a TSDF we pass a pd.Series (i.e. one column,
        #      which can just be obtained from indexing a TSDF)?

        # used to reset the residual df to speed up processes where we need to reset constantly
        self._df_residual_const = df_residual.copy()  # TODO is this req? might cause storage issues for sufficiently large DFs
        self.df_residual = df_residual

        self.flow_limit = flow_limit

        self._analysis_type = _AnalysisType.RAW

        # For now, these segment variables are dummy variables and not accounted
        # for in the program.
        # 21/1/25 commented out as the number of segments and the start/end
        #         dates are determined on the fly; does specifying them in the
        #         input files serve some purpose? Perhaps one solution is to
        #         allow them to be None in which case the behaviour is as
        #         present, but if they are provided then some special case is
        #         taken (perhaps via interceptor decorator?)
        # It appears that these are only useful for SM6
        # self.num_segments = num_segments
        # self.segment_dates = [(segment_start_date, segment_end_date)]

    @classmethod
    def from_file(cls, input_filename):  # TODO unfinished
        with open(input_filename, 'r') as file:
            # date line
            line = file.readline().strip()
            try:
                start_date, end_date = itertools.batched(line.split(), n=3)
            except ValueError:
                raise ValueError(
                    f"Unexpected format for dates (expected dd mm YYYY dd mm YYYY). Got {line}")
            start_date = pd.to_datetime(start_date, dayfirst=True)
            end_date = pd.to_datetime(end_date, dayfirst=True)
            if end_date < start_date:
                raise ValueError("End date before start date.")
            # TODO crop resulting df to these dates?

            # file names
            file1 = file.readline().strip()
            df_observed = TimeseriesDataframe()
            file2 = file.readline().strip()
            df_modelled = TimeseriesDataframe()
            df_residual = df_observed - df_modelled
            # TODO input verification; same column names? go via order?

            file_out = file.readline().strip()

            # file types
            # ! likely don't need to specify this for *this* implementation of negflo so long as the file extensions are correct
            # TODO dynamically determine whether type is supplied or just a file name
            line = file.readline().strip()
            file_type1 = FileType(int(line))
            # TODO err handling
            line = file.readline().strip()
            file_type2 = FileType(int(line))

            flow_limit = float(file.readline().strip())

            # segment
            num_segments = int(file.readline().strip())
            line = file.readline().strip()
            segment_start_date, segment_end_date = itertools.batched(
                line.split(), n=3)
            segment_start_date = pd.to_datetime(segment_start_date)
            segment_end_date = pd.to_datetime(segment_end_date)

        return cls.__init__(
            df_residual=df_residual,
            num_segments=num_segments,
            segment_start_date=segment_start_date,
            segment_end_date=segment_end_date
        )

    def _reset_residual(self):
        """Resets the residual to the initial state."""
        self.df_residual = self._df_residual_const.copy()

    def rw1(self) -> None:
        """This is the raw file created by subtracting the flows in the modelled
        file from the flows in the observed file. The file contains the negative
        flows."""
        self._analysis_type = _AnalysisType.RAW
        self._reset_residual()

    def cl1(self) -> None:
        """Clip all negative flows to zero."""
        self._analysis_type = _AnalysisType.CLIPPED
        self.df_residual[self.df_residual < 0] = 0

    @staticmethod
    def _rescaling_factor(sum_negative: float, sum_positive: float) -> float:
        return 1 - abs(sum_negative) / sum_positive

    def _smooth_flows(self, neg_flow_acc: float, pos_flow_period_l: MutableSequence[float]) -> tuple[float, MutableSequence[float]]:
        """Smooths the accumulated positive flows.

        This will mutate the provided input sequence.

        Returns a couple containing the remaining negative flows (for use in
        carry-over between flows), and a MutableSequence (of identical type) of
        the smoothed flows.
        """
        pos_flow_above_lim_l = list(map(lambda x: x - self.flow_limit,
                                        pos_flow_period_l))
        sum_pos_flow_above_lim = sum(pos_flow_above_lim_l)

        if sum_pos_flow_above_lim > abs(neg_flow_acc):
            rf = self._rescaling_factor(neg_flow_acc, sum_pos_flow_above_lim)
            for i in range(len(pos_flow_period_l)):
                pos_flow_period_l[i] = self.flow_limit + pos_flow_period_l[i] * rf
            neg_flow_acc = 0
        else:
            for i in range(len(pos_flow_period_l)):
                delta = pos_flow_period_l[i] - self.flow_limit
                # INVARIANT: delta > 0
                pos_flow_period_l[i] = self.flow_limit
                neg_flow_acc += delta  # reduces the absolute val
        return neg_flow_acc, pos_flow_period_l

    def _sm_global_helper(self, residual: pd.Series) -> pd.Series:
        neg_sum = sum(residual[residual < 0])
        residual[residual < 0] = 0
        _, res = self._smooth_flows(neg_sum, residual)
        for i in range(len(res)):
            assert len(residual) == len(res)
            residual[i] = res[i]
        return residual

    # TODO is there a way to refactor the following three helpers into one method with additional options? Look at the order of execution and boundary conditionals.
    def _sm_forward_helper(self, residual: pd.Series, *, carry_negative=True) -> pd.Series:
        """SM2 & SM3 helper, which operates on pd.Series aka columns of the dataframe."""
        pos_flow_tracker = ContiguousTracker()
        neg_flow_acc = 0
        for residual_idx, residual_val in enumerate(residual):
            if residual_val >= self.flow_limit:
                pos_flow_tracker.add(residual_idx, residual_val)

            is_below_flow_limit = (residual_val < self.flow_limit)
            is_final_value = residual_idx == (len(residual) - 1)
            if ((is_below_flow_limit or is_final_value)
                    and pos_flow_tracker.is_tracking()):
                # Reached the end of the positive flow period.
                neg_flow_acc, smoothed_pos_flows = self._smooth_flows(neg_flow_acc, pos_flow_tracker.get())
                for list_idx, df_idx in enumerate(pos_flow_tracker.indices()):
                    residual[df_idx] = smoothed_pos_flows[list_idx]
                pos_flow_tracker.reset()
                if not carry_negative:
                    neg_flow_acc = 0

            if residual_val < 0:
                neg_flow_acc += residual_val
                residual[residual_idx] = 0

        if neg_flow_acc < 0:
            logger.error(f"Smoothing function was unable to fully factor out negative flows, remainder {neg_flow_acc}.")
        return residual

    def _sm_backward_helper(self, residual: pd.Series, *, carry_negative=True) -> pd.Series:
        """SM4 & SM5 helper, which operates on pd.Series aka columns of the dataframe."""
        pos_flow_tracker = ContiguousTracker()
        neg_flow_tracker = ContiguousTracker()
        neg_flow_acc = 0
        for residual_idx, residual_val in enumerate(residual):
            if residual_val < 0:
                neg_flow_tracker.add(residual_idx, residual_val)
                residual[residual_idx] = 0

            is_nonneg = residual_val >= 0
            is_final_value = residual_idx == (len(residual) - 1)
            if ((is_nonneg or is_final_value)
                    and neg_flow_tracker.is_tracking()  # TODO should I also check if `neg_flow_acc` is non-zero i.e. < 0? This will only matter when carry_negative is True
                    and pos_flow_tracker.is_tracking()):
                # Reached the end of the negative flow period AND there was previously a positive flow period.
                neg_flow_acc += neg_flow_tracker.sum_and_reset()

                neg_flow_acc, smoothed_pos_flows = self._smooth_flows(neg_flow_acc, pos_flow_tracker.get())
                for list_idx, df_idx in enumerate(pos_flow_tracker.indices()):
                    residual[df_idx] = smoothed_pos_flows[list_idx]
                if not carry_negative:
                    neg_flow_acc = 0

            if residual_val >= self.flow_limit:
                pos_flow_tracker.add(residual_idx, residual_val)

        if sum(neg_flow_tracker) < 0:
            logger.error(f"Smoothing function was unable to fully factor out negative flows, remainder {sum(neg_flow_tracker)}.")
        return residual

    def _sm_bidirectional_helper(self, residual: pd.Series, *, carry_negative=True) -> pd.Series:
        left_tracker = ContiguousTracker()
        right_tracker = ContiguousTracker()
        neg_tracker = ContiguousTracker()
        neg_acc = 0

        def greater_tracker(left, right):
            """Returns the greater of the two trackers"""
            left_sum = sum(left)
            right_sum = sum(right)
            return left if left_sum > right_sum else right

        for residual_idx, residual_val in enumerate(residual):
            is_final_value = residual_idx == (len(residual) - 1)
            # if we've hit the end or if we've dropped out of RHS tracker and
            # need to distribute negative flow
            if ((is_final_value or (residual_val < self.flow_limit
                                    and right_tracker.is_member_of_block(residual_idx)))
                and neg_tracker.is_tracking()  # TODO see note in _sm_backward_helper
                    and (left_tracker.is_tracking() or right_tracker.is_tracking())):
                if is_final_value:
                    if residual_val >= self.flow_limit:
                        right_tracker.add(residual_idx, residual_val)
                    elif residual_val < 0:
                        neg_tracker.add(residual_idx, residual_val)
                neg_acc += neg_tracker.sum_and_reset()
                pos_flow_tracker = greater_tracker(left_tracker, right_tracker)

                # TODO these five lines appear multiple times; pull this functionality out?
                neg_acc, smoothed_pos_flows = self._smooth_flows(neg_acc, pos_flow_tracker.get())
                for list_idx, df_idx in enumerate(left_tracker.indices()):
                    residual[df_idx] = smoothed_pos_flows[list_idx]
                if not carry_negative:
                    neg_acc = 0

            if residual_val >= self.flow_limit:
                right_tracker.add(residual_idx, residual_val)

            elif residual_val < 0:
                neg_tracker.add(residual_idx, residual_val)
                residual[residual_idx] = 0

                if right_tracker.is_tracking():
                    left_tracker = right_tracker
                    right_tracker = ContiguousTracker()
                # TODO if we've hit a second negative flow event in a row and need to accumulate negative flow

            raise NotImplementedError()  # TODO
            """if residual_val < 0:
                neg_tracker.add(residual_idx, residual_val)
                residual[residual_idx] = 0

            is_nonneg = residual_val >= 0
            if ((is_nonneg or is_final_value)
                    and neg_tracker.is_tracking()
                    and left_tracker.is_tracking()):
                # Reached the end of the negative flow period AND there was previously a positive flow period.
                neg_acc += neg_tracker.get_sum_and_reset()

                neg_acc, smoothed_pos_flows = self._smooth_flows(neg_acc, left_tracker.get())
                for list_idx, df_idx in enumerate(left_tracker.indices()):
                    residual[df_idx] = smoothed_pos_flows[list_idx]
                if not carry_negative:
                    neg_acc = 0

            if residual_val >= self.flow_limit:
                left_tracker.add(residual_idx, residual_val)"""

        raise NotImplementedError()
        if sum(neg_tracker) < 0:
            logger.error(f"Smoothing function was unable to fully factor out negative flows, remainder {sum(neg_tracker)}.")
        return residual

    def sm1(self) -> None:
        """This file has been smoothed over the whole period. The negative flows
        have been set to zero and the excess positive flows have been adjusted
        by a factor of
            1 - abs(Total of the negative flows)/(Total of the positive flows)

        This file will be preserve the variability of the flows and maintain the
        mean annual flow at the downstream gauge. This method is most useful for
        preserving storage behaviour if the reach empties into a dam. (This is
        similar to NEGFLO4).

        IMPORTANT: this *may* differ from the NEGFLO implementation in that this
        does not multiply the *positive* flows but the *excess* flows (i.e.
        those above the flow limit) by the scaling factor. This behaviour can be
        recovered by setting the flow limit to zero.
        """
        self._analysis_type = _AnalysisType.SMOOTHED_ALL
        assert self.flow_limit >= 0, f"Expected non-negative flow limit, got {self.flow_limit}."
        self.df_residual = self.df_residual.apply(self._sm_global_helper)

    def sm2(self) -> None:
        """This method breaks the raw residual flows into periods. The period
        starts when the flow exceeds the specified flow limit. It accumulates
        the negative flow and the positive flow when the flow exceeds the flow
        limit. It then factors the flows exceeding the flow limit according to
        the formula above using the total of the negative flows from the period
        preceding the period of positive flows. This method is similar to
        NEGFLO3, except that it will not reduce the flow below the specified
        flow limit.

        This method accumulates the negative flows, so that if the positive
        flows above the flow limit in a period are not enough to balance all the
        preceding negative flows, the remaining negative flow is loaded into the
        next period.

        If the flow limit is set to zero flow, the flows will give modelled
        flows with a mean that is close to the mean of the measure flows.
        However, it can eliminate small flow peaks if there are a lot of
        negative flows.

        Setting the flow limit to a high flow preserves these peaks, but can
        severely reduce the high flows. It can give a ranked flow plot with a
        notch at the flow limit.
        """
        self._analysis_type = _AnalysisType.SMOOTHED_FORWARD
        assert self.flow_limit >= 0, f"Expected non-negative flow limit, got {self.flow_limit}."
        self.df_residual = self.df_residual.apply(self._sm_forward_helper)

    def sm3(self) -> None:
        """This file is produced using the same methodology as SM2 except that
        the negative flow total is not carried over to the next period of
        smoothing."""
        self._analysis_type = _AnalysisType.SMOOTHED_FORWARD_NO_CARRY
        assert self.flow_limit >= 0, f"Expected non-negative flow limit, got {self.flow_limit}."
        self.df_residual = self.df_residual.apply(self._sm_forward_helper, carry_negative=False)

    def sm4(self) -> None:
        """This method is similar to the method used to produce residual.sm2
        except that the negatives are spread over the preceding positive flows
        that exceed the flow limit. The first method is particularly useful if
        the negative flows mainly occur on the rising limb of the hydrograph.
        This method is more useful if the negative flows are generated on the
        falling limb of the hydrograph.

        If the difference between the positive flows and the flow limit does not
        exceed the sum of the negative flows, the remaining negative flow is
        carried over to the next period of positive flows exceeding the flow
        limit."""
        self._analysis_type = _AnalysisType.SMOOTHED_BACKWARD
        assert self.flow_limit >= 0, f"Expected non-negative flow limit, got {self.flow_limit}."
        self.df_residual = self.df_residual.apply(self._sm_backward_helper)

    def sm5(self) -> None:
        """
        This method is the same as that described above where the negative flows
        are spread over the preceding positive flows that exceed the flow limit.
        However, excess negative flows are not carried over to the next period.
        """
        self._analysis_type = _AnalysisType.SMOOTHED_BACKWARD_NO_CARRY
        assert self.flow_limit >= 0, f"Expected non-negative flow limit, got {self.flow_limit}."
        self.df_residual = self.df_residual.apply(self._sm_backward_helper, carry_negative=False)

    def sm6(self) -> None:
        """
        This is the output file for averaging over the specified segments. The
        method in each specified segment is the same as described for
        residual.SM1, where negatives are averaged over the positive flows only
        within the specified segment.

        IMPORTANT: Unlike the original implementation, this version of SM6 does
        not set the flow limit to zero while averaging.
        """
        self._analysis_type = _AnalysisType.SMOOTHED_SPECIFIED
        raise NotImplementedError()  # TODO

        # prototype
        # TODO periods var should be passed in or a class variable
        periods: list[tuple[pd.DatetimeIndex, pd.DatetimeIndex]]

        for start_date, end_date in periods:
            pass
            df: pd.DataFrame
            df = df.loc[start_date:end_date]
            # filter between these dates
            # apply global smoothing helper fn to all columns
            df.apply(self._sm_global_helper)
            update_df: pd.DataFrame
            self.df_residual.update(update_df)
        raise NotImplementedError()

    def sm7(self) -> None:
        """
        If the flow_limit is less than zero, the program uses the negative flows
        to determine the periods for spreading the negatives. The program checks
        the positive flows either side of the negative flows and distributes the
        negative flows over the larger positive flow. If the flow_limit is -0.5,
        the program will use a flow_limit of zero, but work from the negative
        flows. The smoothed residual flows are saved in a file called
        residual.SM7. The segment option does not work for a negative flow
        limit.

        # TODO edit this documentation; this is not how this particular implementation of NEGFLO works, and instead we expect a non-negative flow limit and instead simply call this method.
        """
        self._analysis_type = _AnalysisType.SMOOTHED_NEG_LIM
        assert self.flow_limit >= 0, f"Expected non-negative flow limit, got {self.flow_limit}."
        self.df_residual = self.df_residual.apply(self._sm_bidirectional_helper)

    def log(self) -> None:
        raise NotImplementedError()  # TODO

    def run_all(self, filename="./residual"):
        """Runs all types of """
        self.rw1()
        self.df_residual.to_csv(f"{filename}.cl1")
        self._reset_residual()

        self.sm1()
        self.df_residual.to_csv(f"{filename}.sm1")
        self._reset_residual()

        self.sm2()
        self.df_residual.to_csv(f"{filename}.sm2")
        self._reset_residual()

        self.sm3()
        self.df_residual.to_csv(f"{filename}.sm3")
        self._reset_residual()

        self.sm4()
        self.df_residual.to_csv(f"{filename}.sm4")
        self._reset_residual()

        self.sm5()
        self.df_residual.to_csv(f"{filename}.sm5")
        self._reset_residual()

        # TODO sm6 may not be runnable without periods specified
        self.sm6()
        self.df_residual.to_csv(f"{filename}.sm6")
        self._reset_residual()

        self.sm7()
        self.df_residual.to_csv(f"{filename}.sm7")
        self._reset_residual()

        self.log()

    def to_file(self, *, out_filename=None):
        """Saves the result dataframe to the output file."""
        # TODO: better control over file location etc.?
        if out_filename is None:
            out_filename = "result"
        out_filename += _AnalysisType.to_file_extension(self._analysis_type)
        self.df_residual.to_csv(out_filename)


class ContiguousTracker:
    """Convenience class to track contiguous blocks of data as determined by index."""

    def __init__(self):
        # tracks start pt of positive period
        self.start_idx = None
        # tracks most recent position of positive period
        self.last_idx = None
        self.acc = list()

    def __len__(self):
        return len(self.acc)

    def __iter__(self):
        # This is mostly here to allow sum() to act on this class.
        return iter(self.acc)

    def indices(self):
        """Returns a range of (contiguous) indices of the associated collection for which values were tracked."""
        return range(self.start_idx, self.start_idx + len(self.acc))

    def force_add(self, idx: int, v) -> None:
        if self.start_idx is None:
            self.start_idx = idx
        self.last_idx = idx
        self.acc.append(v)

    def add(self, idx: int, v) -> None:
        """Adds current index/val pair to tracker."""
        if not self.is_tracking():          # initialisation case
            self.reset(idx, [v])
        elif self.is_member_of_block(idx):  # contiguous case
            self.last_idx = idx
            self.acc.append(v)
        else:                               # non-contiguous case
            self.reset(idx, [v])

    def get(self):
        if not self.is_tracking():
            raise RuntimeError("ContiguousTracker is not tracking anything but get() was called.")
        return self.acc

    def sum_and_reset(self):
        """Returns the sum of the underlying accumulator and resets the trackers.

        This appeared a few times so threw it in a util function."""
        x = sum(self)
        self.reset()
        return x

    def is_tracking(self):
        """Checks if the tracker is active.

        If start_idx is not null then it is required that last_idx is also not
        null."""
        return self.start_idx is not None

    def is_member_of_block(self, idx):
        """Asks whether the incoming index is part of the current tracked block."""
        return self.is_tracking() and idx == self.last_idx + 1

    def reset(self, /, idx: Optional[int] = None, val: Optional[list] = None):
        """Resets to default or resets to current index/value (list)."""
        if val is None:
            val = []
        self.start_idx = self.last_idx = idx
        self.acc = val
