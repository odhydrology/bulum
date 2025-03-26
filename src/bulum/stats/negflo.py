"""
Bulum implementation of Negflo and supporting classes.

C.f. https://qldhyd.atlassian.net/wiki/spaces/MET/pages/524386/Negflo
"""

import itertools
import logging
import re
from collections.abc import MutableSequence
from typing import Any, Optional
import datetime

import pandas as pd

from bulum.utils import TimeseriesDataframe

from . import negflo_helpers as helpers

logger = logging.getLogger(__name__)


class Negflo:
    """Bulum implementation of NEGFLO. Incomplete.

    Experimental implementation of NEGFLO. Should be mostly ok to use, but not
    guaranteed to have a stable API at present, especially regarding
    configuration files, pending confirmation of expected outputs etc.

    Spec obtained from: https://qldhyd.atlassian.net/wiki/spaces/MET/pages/524386/Negflo
    """

    def __init__(self,
                 df_residual: pd.DataFrame | TimeseriesDataframe,
                 flow_limit: float = 0.0,
                 num_segments: int = 0,
                 segments: Optional[MutableSequence[tuple[pd.Timestamp, pd.Timestamp]]] = None
                 ):
        super().__init__()

        # used to reset the residual df to speed up processes where we need to reset constantly
        self._df_residual_raw = df_residual.copy()

        self.df_residual = df_residual
        self.neg_overflows: dict[Any, float] = {}

        self.df_name: Optional[str]
        if isinstance(df_residual, TimeseriesDataframe):
            self.df_name = df_residual.name
        else:
            self.df_name = None

        self.flow_limit = flow_limit

        self._analysis_type = helpers.NegfloAnalysisType.RAW

        self.sm6_num_segments = num_segments
        self.sm6_segment_boundaries: Optional[MutableSequence[tuple[pd.Timestamp, pd.Timestamp]]] = segments

    @classmethod
    def from_config_file(cls, filepath, *, execute=False):
        """Construct a Negflo analysis from a config file. Unfinished.

        Args
        ----
            filepath : path-like 
                Relative or absolute filepath to the input file
            execute : bool, default=False
                Flag to execute all possible analyses and save to file.

        Returns
        -------
        An instance of the Negflo class.
        """
        # TODO unfinished
        with open(filepath, 'r', encoding="ascii") as file:
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
            # file_type1 = helpers.NegfloFileType(int(line))
            # TODO err handling
            line = file.readline().strip()
            # file_type2 = helpers.NegfloFileType(int(line))

            flow_limit = float(file.readline().strip())

            # segments
            # TODO flexibility to only specify periods, or only specify num_segments
            # num_segments = int(file.readline().strip())
            # line = file.readline().strip()
            # segment_start_date, segment_end_date = itertools.batched(
            #     line.split(), n=3)
            # segment_start_date = pd.to_datetime(segment_start_date)
            # segment_end_date = pd.to_datetime(segment_end_date)

            # validation of the boundaries
            # if self.sm6_segment_boundaries is not None:
            #     last_segment = None
            #     for segment in self.sm6_segment_boundaries:
            #         if segment[1] < segment[0]:
            #             raise ValueError(f"Segment must be in chronological order: {segment}")

            #         if last_segment is None:
            #             last_segment = segment
            #             continue
            #         elif segment[0] < last_segment[1]:
            #             raise ValueError(f"Malordered segments, {last_segment} is not before {segment}")
            #             # TODO better error message

        return cls(
            df_residual=df_residual,
            flow_limit=flow_limit
        )

    def _reset_residual(self) -> None:
        """Reset the residual to the initial state."""
        self.neg_overflows = {}
        self.df_residual = self._df_residual_raw.copy()

    def rw1(self) -> None:
        """Compute the raw residual i.e. downstream-upstream flows.

        Internally, resets the residual to that stored on initialisation."""
        self._analysis_type = helpers.NegfloAnalysisType.RAW
        self._reset_residual()

    def cl1(self) -> None:
        """Clip all negative flows to zero."""
        self._analysis_type = helpers.NegfloAnalysisType.CLIPPED
        self.df_residual[self.df_residual < 0] = 0

    @staticmethod
    def _has_neg_flow_to_redistribute(
            neg_acc: float, neg_tracker: Optional[helpers.ContiguousIndexTracker] = None) -> bool:
        if neg_tracker is None:
            return neg_acc != 0
        else:
            return neg_tracker.is_tracking() or neg_acc != 0

    @staticmethod
    def _rescaling_factor(sum_negative, sum_positive):
        return 1 - abs(sum_negative) / sum_positive

    def _smooth_flows(self, neg_acc,
                      pos_flow_period_l: MutableSequence | pd.Series
                      ) -> tuple[Any, MutableSequence | pd.Series]:
        """Smooth the accumulated positive flows.

        Return
        ------
        A couple containing:
        - the remaining negative flows (number)
        - the smoothed sequence
        """
        pos_flow_period_l = list(pos_flow_period_l)
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

    @helpers.dec_sm_helpers_log_neg_rem
    def _sm_global_series(self, residual: pd.Series) -> tuple[pd.Series, float]:
        """Smooth the entire input series on aggregate.

        Return
        ------
        pd.Series
            The smoothed series
        float
            The remaining negative flow after smoothing
        """
        neg_sum = sum(residual[residual < 0])
        residual[residual < 0] = 0
        neg_sum, smoothed_residual = self._smooth_flows(neg_sum, residual)
        assert len(residual) == len(smoothed_residual)
        for i, _ in enumerate(smoothed_residual):
            residual.iloc[i] = smoothed_residual[i]
        return residual, neg_sum

    # TODO is there a way to refactor the following three helpers into one
    #      method with additional options? Look at the order of execution and
    #      boundary conditionals.

    @helpers.dec_sm_helpers_log_neg_rem
    def _sm_forward_series(self, residual: pd.Series, *,
                           carry_negative=True) -> tuple[pd.Series, float]:
        """SM2 & SM3 helper, which operates on pd.Series aka columns of the dataframe."""
        pos_tracker = helpers.ContiguousIndexTracker()
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

    @helpers.dec_sm_helpers_log_neg_rem
    def _sm_backward_series(self, residual: pd.Series, *,
                            carry_negative=True) -> tuple[pd.Series, float]:
        """SM4 & SM5 helper, which operates on pd.Series aka columns of the dataframe."""
        pos_tracker = helpers.ContiguousIndexTracker()
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

    @helpers.dec_sm_helpers_log_neg_rem
    def _sm_bidirectional_series(self, residual: pd.Series, *,
                                 carry_negative=True) -> tuple[pd.Series, float]:
        """SM7 (bidirectional, negative flow event based) helper.

        Note
        ----
        Current implementation only distributes flows at the conclusion of the
        RHS tracker (or at the end of all flow). 
        """
        # TODO Check whether this is the expected behaviour or if it should be
        #      distributed at every negative flow event.
        # TODO Distribute over other positive flow event if it flattens the
        #      larger? Or if it would flatten one, then flatten both
        #      simultaneously?
        left_tracker = helpers.ContiguousIndexTracker()
        right_tracker = helpers.ContiguousIndexTracker()
        neg_acc = 0

        def greater_tracker(left: helpers.ContiguousIndexTracker, right: helpers.ContiguousIndexTracker) -> helpers.ContiguousIndexTracker:
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
                right_tracker = helpers.ContiguousIndexTracker()

            if residual_val < 0:
                neg_acc += residual_val
                residual[residual_idx] = 0

        return residual, neg_acc

    def sm1(self) -> None:
        """Redistribute negative flows across all positive flow events.

        The negative flows are set to zero and the excess positive flows have
        been adjusted by a factor of
            1 - abs(Total of the negative flows)/(Total of the positive flows)

        This will preserve the variability of the flows and maintain the mean
        annual flow at the downstream gauge. This method is most useful for
        preserving storage behaviour if the reach empties into a dam.

        Return
        ------
        None. The results are written to `self.df_residual`.
        """
        self._analysis_type = helpers.NegfloAnalysisType.SMOOTHED_ALL
        assert self.flow_limit >= 0, f"Expected non-negative flow limit, got {self.flow_limit}."
        self.df_residual = self.df_residual.apply(self._sm_global_series)

    def sm2(self) -> None:
        """Redistribute negative flows into future positive flow events, with
        carry-over.

        ===== 

        This method breaks the raw residual flows into periods. The period
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
        self._analysis_type = helpers.NegfloAnalysisType.SMOOTHED_FORWARD
        assert self.flow_limit >= 0, f"Expected non-negative flow limit, got {self.flow_limit}."
        self.df_residual = self.df_residual.apply(self._sm_forward_series)

    def sm3(self) -> None:
        """Redistribute negative flows into future positive flow events, without
        carry-over. Refer to SM2.
        """
        self._analysis_type = helpers.NegfloAnalysisType.SMOOTHED_FORWARD_NO_CARRY
        assert self.flow_limit >= 0, f"Expected non-negative flow limit, got {self.flow_limit}."
        self.df_residual = self.df_residual.apply(self._sm_forward_series, carry_negative=False)

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
        self._analysis_type = helpers.NegfloAnalysisType.SMOOTHED_BACKWARD
        assert self.flow_limit >= 0, f"Expected non-negative flow limit, got {self.flow_limit}."
        self.df_residual = self.df_residual.apply(self._sm_backward_series)

    def sm5(self) -> None:
        """
        This method is the same as that described above where the negative flows
        are spread over the preceding positive flows that exceed the flow limit.
        However, excess negative flows are not carried over to the next period.
        """
        self._analysis_type = helpers.NegfloAnalysisType.SMOOTHED_BACKWARD_NO_CARRY
        assert self.flow_limit >= 0, f"Expected non-negative flow limit, got {self.flow_limit}."
        self.df_residual = self.df_residual.apply(self._sm_backward_series, carry_negative=False)

    def sm6(self, *, use_predefined_segments=True,
            sampling_frequency=None, sampling_start_date=None) -> None:
        """Smooths over the specified segments.

        Applies the SM1 smoothing algorithm (ie global smoothing) for flows
        across the specified periods. If no segments are defined or `method` is
        set to `sample`, then it will partition the full period on an annual
        (default) basis.

        Unlike the reference documentation, this function does not set the flow
        limit to zero while smoothing.

        Assumes the indices of the underlying dataframe are datetimes.

        Parameters
        ----------
            use_predefined_segments : bool, default True
                Use the stored segments (`self.sm6_segment_boundaries`) if they
                exist. Otherwise the segments will be computed when this method
                is called.
            sampling_frequency : pd.DateOffset, optional
                Specifies the time interval for smoothing periods. Defaults to
                one year.
            sampling_start_date : pd.Timestamp, optional
                Specifies the start of the first period for sampling. Defaults
                to the start of the data period.

        Returns
        -------
        Writes the result to `self.df_residual`.
        """
        self._analysis_type = helpers.NegfloAnalysisType.SMOOTHED_SEGMENTS
        # pre-processing to determine segments if non-existent
        if use_predefined_segments == "default" and self.sm6_segment_boundaries is not None:
            pass
        else:
            if sampling_start_date is None:
                sampling_start_date = self.df_residual.index[0]
            end = self.df_residual.index[-1]
            date = sampling_start_date

            if sampling_frequency is None:
                sampling_frequency = pd.DateOffset(years=1)
            one_day = pd.DateOffset(days=1)

            self.sm6_segment_boundaries = []
            while date < end:
                last_date = date
                date = date + sampling_frequency
                self.sm6_segment_boundaries.append((last_date, date-one_day))

        # main logic
        for start, end in self.sm6_segment_boundaries:
            start = pd.to_datetime(start)
            end = pd.to_datetime(end)
            mask = (self.df_residual.index >= start) & (self.df_residual.index <= end)
            negflo = Negflo(self.df_residual[mask])
            negflo.sm1()
            self.df_residual.update(negflo.df_residual)
            # update overflows
            for k, v in negflo.neg_overflows.items():
                self.neg_overflows[k] = self.neg_overflows.get(k, 0) + v

    def sm7(self) -> None:
        """Smooths negative flows over the largest adjacent positive flow event.

        The program checks the positive flows either side of the negative flows
        and distributes the negative flows over the larger positive flow.

        If the flow_limit is less than zero, the program uses the negative flows
        to determine the periods for spreading the negatives.  If the flow_limit is -0.5,
        the program will use a flow_limit of zero, but work from the negative
        flows. The smoothed residual flows are saved in a file called
        residual.SM7. The segment option does not work for a negative flow
        limit.

        """
        # TODO edit this documentation; this is not how this particular
        #      implementation of NEGFLO works, and instead we expect a
        #      non-negative flow limit and instead simply call this method.
        self._analysis_type = helpers.NegfloAnalysisType.SMOOTHED_NEG_LIM
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
        # TODO
        raise NotImplementedError()

    def run_all(self, filename="./residual"):
        """Runs all analyses on the residual."""
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

        self.sm6()
        self.df_residual.to_csv(f"{filename}.sm6")
        self._reset_residual()

        self.sm7()
        self.df_residual.to_csv(f"{filename}.sm7")
        self._reset_residual()

        self.log()

    def to_file(self, *, out_filename: Optional[str] = None):
        """Saves the result dataframe to the output file."""
        if out_filename is None:  # Automatically determine file name.
            out_filename = (
                ("result" if self.df_name is None else self.df_name)
                + self._analysis_type.to_file_extension())
        # if extension is not already specified
        elif not re.match(r"\.\w+$", out_filename):
            out_filename += self._analysis_type.to_file_extension()
        self.df_residual.to_csv(out_filename)
