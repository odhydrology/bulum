"""Bulum implementation of negflo and supporting classes.

C.f. https://qldhyd.atlassian.net/wiki/spaces/MET/pages/524386/Negflo
"""

import itertools
import logging
import re
from collections.abc import MutableSequence
from typing import Any, Optional

import pandas as pd

from bulum.utils import TimeseriesDataframe

from .negflo_helpers import (AnalysisType, ContiguousTracker, FileType,
                               dec_sm_helpers_log_neg_rem)

logger = logging.getLogger(__name__)


class Negflo:
    """Bulum implementation of NEGFLO, **NOT FINISHED**.

    Experimental implementation of NEGFLO. Should be mostly ok to use, but not
    guaranteed to have a stable API at present, especially regarding
    configuration files, pending confirmation of expected outputs etc.

    Spec obtained from:
        https://qldhyd.atlassian.net/wiki/spaces/MET/pages/524386/Negflo

    """

    def __init__(self,
                 # TODO: should these be tsdfs or should we just pass in one series? need to see functionality of negflo program.
                 df_residual: pd.DataFrame | TimeseriesDataframe,
                 flow_limit: float = 0.0,
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
        self._df_residual_const = df_residual.copy()
        # TODO is this ^^^ req? might cause storage issues for sufficiently large DFs
        self.df_residual = df_residual
        self.neg_overflow: dict[Any, float] = {}
        self.df_name: Optional[str]
        if isinstance(df_residual, TimeseriesDataframe):
            self.df_name = df_residual.name
        else:
            self.df_name = None

        self.flow_limit = flow_limit

        self._analysis_type = AnalysisType.RAW

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
    def from_config_file(cls, input_filename):
        # TODO unfinished
        with open(input_filename, 'r') as file:
            # date line
            line = file.readline().strip()
            try:
                start_date, end_date = itertools.batched(line.split(), n=3)
            except ValueError as exc:
                raise ValueError(
                    "Unexpected format for dates (expected dd mm YYYY dd mm YYYY). " +
                    f"Got {line}") from exc
            start_date = pd.to_datetime(start_date, dayfirst=True)
            end_date = pd.to_datetime(end_date, dayfirst=True)
            if end_date < start_date:
                raise ValueError("End date before start date.")
            # TODO crop resulting df to these dates?

            # file names
            file1 = file.readline().strip()
            df_observed = TimeseriesDataframe()  # TODO
            file2 = file.readline().strip()
            df_modelled = TimeseriesDataframe()  # TODO
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

        return cls(
            df_residual=df_residual,
            # num_segments=num_segments,
            # segment_start_date=segment_start_date,
            # segment_end_date=segment_end_date
        )

    def _reset_residual(self):
        """Resets the residual to the initial state."""
        self.neg_overflow = {}
        self.df_residual = self._df_residual_const.copy()

    def rw1(self) -> None:
        """This is the raw file created by subtracting the flows in the modelled
        file from the flows in the observed file. The file contains the negative
        flows."""
        self._analysis_type = AnalysisType.RAW
        self._reset_residual()

    def cl1(self) -> None:
        """Clip all negative flows to zero."""
        self._analysis_type = AnalysisType.CLIPPED
        self.df_residual[self.df_residual < 0] = 0

    @staticmethod
    def _has_neg_flow_to_redistribute(neg_acc: float,
                                      neg_tracker: Optional[ContiguousTracker] = None) -> bool:
        if neg_tracker is None:
            return neg_acc != 0
        else:
            return neg_tracker.is_tracking() or neg_acc != 0

    @staticmethod
    def _rescaling_factor(sum_negative, sum_positive):
        return 1 - abs(sum_negative) / sum_positive

    def _smooth_flows(self, neg_acc,
                      pos_flow_period_l: MutableSequence[Any]) -> tuple[Any, MutableSequence[Any]]:
        """Smooths the accumulated positive flows.

        This will mutate the provided input sequence.

        Returns a couple containing the remaining negative flows (for use in
        carry-over between flows), and a MutableSequence (of identical type) of
        the smoothed flows.
        """
        pos_flow_above_lim_l = list(map(lambda x: x - self.flow_limit,
                                        pos_flow_period_l))
        sum_pos_flow_above_lim = sum(pos_flow_above_lim_l)

        if sum_pos_flow_above_lim > abs(neg_acc):
            rf = self._rescaling_factor(neg_acc, sum_pos_flow_above_lim)
            for i, _ in enumerate(pos_flow_period_l):
                pos_flow_period_l[i] = self.flow_limit + pos_flow_above_lim_l[i] * rf
            neg_acc = 0
        else:
            for i, _ in enumerate(pos_flow_period_l):
                delta = pos_flow_period_l[i] - self.flow_limit
                # INVARIANT: delta > 0
                pos_flow_period_l[i] = self.flow_limit
                neg_acc += delta  # reduces the absolute val
        return neg_acc, pos_flow_period_l

    @dec_sm_helpers_log_neg_rem
    def _sm_global_series(self, residual: pd.Series) -> pd.Series:
        neg_sum = sum(residual[residual < 0])
        residual[residual < 0] = 0
        neg_sum, res = self._smooth_flows(neg_sum, residual)
        assert len(residual) == len(res)
        for i, _ in enumerate(res):
            residual[i] = res[i]
        return residual, neg_sum

    # TODO is there a way to refactor the following three helpers into one
    #      method with additional options? Look at the order of execution and
    #      boundary conditionals.

    @dec_sm_helpers_log_neg_rem
    def sm_forward_series(self, residual: pd.Series, *, carry_negative=True) -> pd.Series:
        """SM2 & SM3 helper, which operates on pd.Series aka columns of the dataframe."""
        pos_tracker = ContiguousTracker()
        neg_acc = 0
        for residual_idx, residual_val in enumerate(residual):
            if residual_val >= self.flow_limit:
                pos_tracker.add(residual_idx, residual_val)

            is_below_flow_limit = residual_val < self.flow_limit
            is_final_value = residual_idx == (len(residual) - 1)
            if ((is_below_flow_limit or is_final_value)
                    and self._has_neg_flow_to_redistribute(neg_acc)
                    and pos_tracker.is_tracking()):
                # Reached the end of the positive flow period.
                neg_acc, smoothed_pos_flows = self._smooth_flows(neg_acc, pos_tracker.get())
                for list_idx, df_idx in enumerate(pos_tracker.indices()):
                    residual[df_idx] = smoothed_pos_flows[list_idx]
                pos_tracker.reset()
                if not carry_negative:
                    neg_acc = 0

            if residual_val < 0:
                neg_acc += residual_val
                residual[residual_idx] = 0

        return residual, neg_acc

    @dec_sm_helpers_log_neg_rem
    def _sm_backward_series(self, residual: pd.Series, *, carry_negative=True) -> pd.Series:
        """SM4 & SM5 helper, which operates on pd.Series aka columns of the dataframe."""
        pos_tracker = ContiguousTracker()
        neg_acc = 0
        for residual_idx, residual_val in enumerate(residual):
            if residual_val < 0:
                neg_acc += residual_val
                residual[residual_idx] = 0

            is_nonneg = residual_val >= 0
            is_final_value = residual_idx == (len(residual) - 1)
            if ((is_nonneg or is_final_value)
                    and self._has_neg_flow_to_redistribute(neg_acc)
                    and pos_tracker.is_tracking()):
                # Reached the end of the negative flow period AND there was
                # previously a positive flow period.
                neg_acc, smoothed_pos_flows = self._smooth_flows(neg_acc, pos_tracker.get())
                for list_idx, df_idx in enumerate(pos_tracker.indices()):
                    residual[df_idx] = smoothed_pos_flows[list_idx]
                if not carry_negative:
                    neg_acc = 0

            if residual_val >= self.flow_limit:
                pos_tracker.add(residual_idx, residual_val)

        return residual, neg_acc

    @dec_sm_helpers_log_neg_rem
    def _sm_bidirectional_series(self, residual: pd.Series, *, carry_negative=True) -> pd.Series:
        """SM7 helper.

        Current implementation only distributes flows at the conclusion of the
        RHS tracker (or at the end of all flow). 

        """
        # TODO Check whether this is the expected behaviour or if it should be
        #      distributed at every negative flow event.
        # TODO Distribute over other positive flow event if it flattens the
        #      larger? Or if it would flatten one, then flatten both
        #      simultaneously?
        # TODO This might be easier if I changed the algorithm - identify
        #      periods first and then do the smoothing.
        left_tracker = ContiguousTracker()
        right_tracker = ContiguousTracker()
        neg_acc = 0

        def greater_tracker(left: ContiguousTracker, right: ContiguousTracker) -> ContiguousTracker:
            """Returns the tracker that has the greater total flow above the
            flow limit."""
            left_sum = sum(left.offset(-self.flow_limit))
            right_sum = sum(right.offset(- self.flow_limit))
            return left if left_sum > right_sum else right

        for residual_idx, residual_val in enumerate(residual):
            is_final_value = residual_idx == (len(residual) - 1)
            # if we've hit the end or if we've dropped out of RHS tracker and
            # need to distribute negative flow

            if is_final_value:
                if residual_val >= self.flow_limit:
                    right_tracker.add(residual_idx, residual_val)
                elif residual_val < 0:
                    neg_acc += residual_val
            if ((is_final_value or (residual_val < self.flow_limit
                                    and right_tracker.is_member_of_block(residual_idx)))
                    and self._has_neg_flow_to_redistribute(neg_acc)
                    and (left_tracker.is_tracking() or right_tracker.is_tracking())):
                larger_pos_tracker = greater_tracker(left_tracker, right_tracker)

                neg_acc, smoothed_pos_flows = self._smooth_flows(neg_acc, larger_pos_tracker.get())
                for list_idx, df_idx in enumerate(larger_pos_tracker.indices()):
                    residual[df_idx] = smoothed_pos_flows[list_idx]
                if not carry_negative:
                    neg_acc = 0

            if residual_val >= self.flow_limit:
                right_tracker.add(residual_idx, residual_val)
            elif right_tracker.is_tracking():
                left_tracker = right_tracker
                right_tracker = ContiguousTracker()

            if residual_val < 0:
                neg_acc += residual_val
                residual[residual_idx] = 0

        return residual, neg_acc

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
        self._analysis_type = AnalysisType.SMOOTHED_ALL
        assert self.flow_limit >= 0, f"Expected non-negative flow limit, got {self.flow_limit}."
        self.df_residual = self.df_residual.apply(self._sm_global_series)

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
        self._analysis_type = AnalysisType.SMOOTHED_FORWARD
        assert self.flow_limit >= 0, f"Expected non-negative flow limit, got {self.flow_limit}."
        self.df_residual = self.df_residual.apply(self.sm_forward_series)

    def sm3(self) -> None:
        """This file is produced using the same methodology as SM2 except that
        the negative flow total is not carried over to the next period of
        smoothing."""
        self._analysis_type = AnalysisType.SMOOTHED_FORWARD_NO_CARRY
        assert self.flow_limit >= 0, f"Expected non-negative flow limit, got {self.flow_limit}."
        self.df_residual = self.df_residual.apply(self.sm_forward_series, carry_negative=False)

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
        self._analysis_type = AnalysisType.SMOOTHED_BACKWARD
        assert self.flow_limit >= 0, f"Expected non-negative flow limit, got {self.flow_limit}."
        self.df_residual = self.df_residual.apply(self._sm_backward_series)

    def sm5(self) -> None:
        """
        This method is the same as that described above where the negative flows
        are spread over the preceding positive flows that exceed the flow limit.
        However, excess negative flows are not carried over to the next period.
        """
        self._analysis_type = AnalysisType.SMOOTHED_BACKWARD_NO_CARRY
        assert self.flow_limit >= 0, f"Expected non-negative flow limit, got {self.flow_limit}."
        self.df_residual = self.df_residual.apply(self._sm_backward_series, carry_negative=False)

    def sm6(self) -> None:
        """
        This is the output file for averaging over the specified segments. The
        method in each specified segment is the same as described for
        residual.SM1, where negatives are averaged over the positive flows only
        within the specified segment.

        IMPORTANT: Unlike the original implementation, this version of SM6 does
        not set the flow limit to zero while averaging.
        """
        self._analysis_type = AnalysisType.SMOOTHED_SPECIFIED
        raise NotImplementedError()  # TODO

        # prototype
        # TODO periods var should be passed in or a class variable
        # periods: list[tuple[pd.DatetimeIndex, pd.DatetimeIndex]]
        # for start_date, end_date in periods:
        #     pass
        #     df: pd.DataFrame
        #     df = df.loc[start_date:end_date]
        #     # filter between these dates
        #     # apply global smoothing helper fn to all columns
        #     df.apply(self._sm_global_series)
        #     update_df: pd.DataFrame
        #     self.df_residual.update(update_df)
        # raise NotImplementedError()

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
        self._analysis_type = AnalysisType.SMOOTHED_NEG_LIM
        assert self.flow_limit >= 0, f"Expected non-negative flow limit, got {self.flow_limit}."
        self.df_residual = self.df_residual.apply(self._sm_bidirectional_series)

    def log(self) -> None:
        """
        Input_file_name.LOG

        A file is also created which gives the total of the positive and negative
        flows, the total of the positive flows above the flow limit. It also gives the
        start and end of each period of flows above the flow limit, the total of the
        preceding negatives and the total of the positive flow above the flow limit.
        """
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

        # TODO sm6 will not be runnable without periods specified
        logger.error("SM6 not implemented.")
        # self.sm6()
        # self.df_residual.to_csv(f"{filename}.sm6")
        # self._reset_residual()

        self.sm7()
        self.df_residual.to_csv(f"{filename}.sm7")
        self._reset_residual()

        self.log()

    def to_file(self, *, out_filename: Optional[str] = None):
        """Saves the result dataframe to the output file.

        This function will attempt to automatically 
        """
        if out_filename is None:  # Automatically determine file name.
            out_filename = (
                ("result" if self.df_name is None else self.df_name)
                + self._analysis_type.to_file_extension())
        # if extension is not already specified
        elif not re.match(r"\.\w+$", out_filename):
            out_filename += self._analysis_type.to_file_extension()
        self.df_residual.to_csv(out_filename)
