"""
Bulum implementation of NEGFLO.

NEGFLO redistributes the negative flows in a residual flow series (e.g.
downstream gauged flow minus routed upstream flow) by reducing positive flows,
so that the negatives can be set to zero without inflating the total volume.

Concepts
--------
Flow event
    A maximal run of consecutive days with flow above the flow limit. Days with
    missing data (NaN) break events.
Negative volume
    The total of the negative flows between two consecutive events (or before
    the first event, or after the last).
Absorbing
    An event absorbs a negative volume by reducing each flow's excess above the
    flow limit in proportion, i.e. by a factor of
    ``1 - negative volume / total excess``. Flows are never reduced below the
    flow limit. If the event's total excess is too small, all its flows are set
    to the flow limit and the remainder is left over.
Overflow
    Negative volume that was not absorbed by any event. It is reported (as a
    negative number) per column in :attr:`Negflo.neg_overflows`, so that for
    every method::

        sum(result) == sum(positive residuals) + sum(negative residuals) - overflow

Missing data
    Missing days (NaN) contribute nothing to any total, break flow events,
    and remain NaN in every result. Negative volumes are carried across gaps
    exactly as they are across days of zero flow.

After smoothing, all remaining negative flows are set to zero. Flows between
zero and the flow limit that are not part of an event are left unchanged.

Differences from NEGFLO6
------------------------
This implementation follows the NEGFLO6 documentation and C source, except:

* Missing data is excluded rather than treated as a measured flow of zero
  (which in NEGFLO6 created spurious negative residuals equal to the modelled
  flow).
* The flow limit is applied. NEGFLO6 silently ignored positive flow limits.
* Results are not rounded to whole numbers.
* SM4 carries negatives with no preceding event (i.e. before the first event)
  forward into the first event. NEGFLO6 discarded them, and also had a typo
  in its carry-over calculation.
* SM7 is a method in its own right using a non-negative flow limit, rather than
  being triggered by a negative flow limit. Negatives after the last event are
  absorbed by the last event, and an event may absorb negative volumes from both
  sides (NEGFLO6 discarded the former and overwrote one of the latter).

See Also
--------
Spec obtained from: https://qldhyd.atlassian.net/wiki/spaces/MET/pages/524386/Negflo
"""

import logging
import os
from collections.abc import Callable, Sequence
from enum import Enum
from typing import Optional

import numpy as np
import pandas as pd

from bulum.utils import TimeseriesDataframe

logger = logging.getLogger(__name__)


class NegfloAnalysisType(Enum):
    """Negflo class keeps track of most recent analysis performed."""
    RAW = -1
    CLIPPED = 0
    SMOOTHED_ALL = 1  # sm1
    SMOOTHED_FORWARD = 2  # sm2
    SMOOTHED_FORWARD_NO_CARRY = 3  # sm3
    SMOOTHED_BACKWARD = 4  # sm4
    SMOOTHED_BACKWARD_NO_CARRY = 5  # sm5
    SMOOTHED_SEGMENTS = 6  # sm6
    SMOOTHED_NEG_LIM = 7  # sm7

    def to_file_extension(self) -> str:
        """Gives the corresponding file extension for the analysis type."""
        match self:
            case NegfloAnalysisType.RAW:
                return ".rw1"
            case NegfloAnalysisType.CLIPPED:
                return ".cl1"
            case NegfloAnalysisType.SMOOTHED_ALL:
                return ".sm1"
            case NegfloAnalysisType.SMOOTHED_FORWARD:
                return ".sm2"
            case NegfloAnalysisType.SMOOTHED_FORWARD_NO_CARRY:
                return ".sm3"
            case NegfloAnalysisType.SMOOTHED_BACKWARD:
                return ".sm4"
            case NegfloAnalysisType.SMOOTHED_BACKWARD_NO_CARRY:
                return ".sm5"
            case NegfloAnalysisType.SMOOTHED_SEGMENTS:
                return ".sm6"
            case NegfloAnalysisType.SMOOTHED_NEG_LIM:
                return ".sm7"
            case _:
                raise ValueError(f"Unhandled/invalid enum, {self}")


# ---------------------------------------------------------------------------
# Series-level algorithms.
#
# Each smoothing function takes a 1-D float array (NaN = missing), modifies it
# in place, and returns the negative volume (as a positive number) it could not
# absorb. Negative flows are left in place; the caller sets them to zero.
# ---------------------------------------------------------------------------

def _negative_total(x: np.ndarray) -> float:
    """Total magnitude of the negative flows in `x`."""
    return float(-x[x < 0].sum())


def _find_events(x: np.ndarray, flow_limit: float) -> list[np.ndarray]:
    """Indices of each flow event, i.e. maximal run of consecutive days with
    flow above `flow_limit`. Missing days are never part of an event."""
    above = np.concatenate(([0], (x > flow_limit).astype(np.int8), [0]))
    edges = np.diff(above)
    starts = np.flatnonzero(edges == 1)
    ends = np.flatnonzero(edges == -1)
    return [np.arange(start, end) for start, end in zip(starts, ends)]


def _negative_volumes(x: np.ndarray, events: list[np.ndarray]) -> np.ndarray:
    """Negative volume preceding each event, followed by the volume after the
    last event (so ``len(events) + 1`` values).

    Requires a non-negative flow limit, so that events contain no negatives.
    """
    cumulative = np.concatenate(([0.0], np.cumsum(np.where(x < 0, -x, 0.0))))
    cuts = [0, *(event[0] for event in events), len(x)]
    return np.diff(cumulative[cuts])


def _absorb(x: np.ndarray, idx: np.ndarray, volume: float, flow_limit: float) -> float:
    """Reduce the flows at `idx` (all above `flow_limit`) by a total of
    `volume`, in proportion to their excess above `flow_limit`.

    Returns the part of `volume` that could not be absorbed.
    """
    if volume <= 0 or len(idx) == 0:
        return volume
    excess = x[idx] - flow_limit
    total_excess = excess.sum()
    if volume >= total_excess:
        x[idx] = flow_limit
        return volume - total_excess
    x[idx] = flow_limit + excess * (1 - volume / total_excess)
    return 0.0


def _smooth_all(x: np.ndarray) -> float:
    """SM1: all negatives absorbed by all positive flows (flow limit of zero)."""
    return _absorb(x, np.flatnonzero(x > 0), _negative_total(x), 0.0)


def _smooth_forward(x: np.ndarray, flow_limit: float, carry: bool) -> float:
    """SM2/SM3: each event absorbs the negative volume preceding it."""
    events = _find_events(x, flow_limit)
    volumes = _negative_volumes(x, events)
    carried = lost = 0.0
    for event, volume in zip(events, volumes[:-1]):
        leftover = _absorb(x, event, carried + volume, flow_limit)
        if carry:
            carried = leftover
        else:
            lost += leftover
    return carried + lost + volumes[-1]


def _smooth_backward(x: np.ndarray, flow_limit: float, carry: bool) -> float:
    """SM4/SM5: each event absorbs the negative volume following it.

    With `carry`, whatever an event cannot absorb (and any negatives before the
    first event) is carried forward to the next event.
    """
    events = _find_events(x, flow_limit)
    volumes = _negative_volumes(x, events)
    carried, lost = (volumes[0], 0.0) if carry else (0.0, volumes[0])
    for event, volume in zip(events, volumes[1:]):
        leftover = _absorb(x, event, carried + volume, flow_limit)
        if carry:
            carried = leftover
        else:
            lost += leftover
    return carried + lost


def _smooth_adjacent(x: np.ndarray, flow_limit: float) -> float:
    """SM7: each negative volume is absorbed by the larger (by excess volume)
    of the events either side of it; the preceding event wins ties."""
    events = _find_events(x, flow_limit)
    volumes = _negative_volumes(x, events)
    if not events:
        return volumes[0]
    excess = [(x[event] - flow_limit).sum() for event in events]
    to_absorb = np.zeros(len(events))
    for k, volume in enumerate(volumes):
        # volumes[k] lies between events k-1 and k
        neighbours = [j for j in (k - 1, k) if 0 <= j < len(events)]
        to_absorb[max(neighbours, key=lambda j: excess[j])] += volume
    return sum(_absorb(x, event, volume, flow_limit)
               for event, volume in zip(events, to_absorb))


def _smooth_segments(x: np.ndarray, segment_masks: Sequence[np.ndarray]) -> float:
    """SM6: SM1 applied within each segment. Days outside every segment are
    not smoothed, so their negatives are lost."""
    covered = np.zeros(len(x), dtype=bool)
    lost = 0.0
    for mask in segment_masks:
        segment = x[mask]
        lost += _smooth_all(segment)
        x[mask] = segment
        covered |= mask
    return lost + _negative_total(x[~covered])


def _generate_segments(start: pd.Timestamp, last_date: pd.Timestamp,
                       frequency: pd.DateOffset) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    """Consecutive segments of length `frequency` from `start`, covering
    `last_date`."""
    segments = []
    while start <= last_date:
        next_start = start + frequency
        segments.append((start, next_start - pd.Timedelta(days=1)))
        start = next_start
    return segments


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

class Negflo:
    """Bulum implementation of NEGFLO.

    Each analysis method (:meth:`rw1`, :meth:`cl1`, :meth:`sm1` ... :meth:`sm7`)
    works from the residual supplied on construction, independently of any
    previous call, and returns the result as a new dataframe. Columns are
    processed independently. See the module documentation for the concepts
    and the treatment of missing data.

    Attributes
    ----------
    flow_limit : float
        Flows at or below this limit never absorb negative flow (SM2-SM5, SM7).
    segments : list of (start, end) tuples, optional
        Inclusive date ranges used by :meth:`sm6`.
    neg_overflows : dict
        Negative volume (as a negative number) that the most recent analysis
        could not redistribute, per column.
    df_name : str or None
        Name of the input TimeseriesDataframe, used to name output files.
    """

    def __init__(self,
                 df_residual: pd.DataFrame | TimeseriesDataframe,
                 flow_limit: float = 0.0,
                 *,
                 segments: Optional[Sequence[tuple[pd.Timestamp, pd.Timestamp]]] = None):
        """
        Parameters
        ----------
        df_residual : pd.DataFrame or TimeseriesDataframe
            Residual flows, one series per column. Missing data should be NaN.
        flow_limit : float, default 0.0
            Non-negative flow limit.
        segments : sequence of (start, end), optional
            Inclusive date ranges for :meth:`sm6`.
        """
        if flow_limit < 0:
            raise ValueError(f"Expected a non-negative flow limit, got {flow_limit}.")
        self.df_name: Optional[str] = (
            df_residual.name if isinstance(df_residual, TimeseriesDataframe) else None)
        self.flow_limit = flow_limit
        self.segments = segments
        self.neg_overflows: dict = {}
        self._df_residual_raw = df_residual.astype(float)
        self._df_residual = self._df_residual_raw.copy()
        self._analysis_type = NegfloAnalysisType.RAW

    @classmethod
    def from_flows(cls,
                   df_measured: pd.DataFrame,
                   df_modelled: pd.DataFrame,
                   flow_limit: float = 0.0,
                   *,
                   segments: Optional[Sequence[tuple[pd.Timestamp, pd.Timestamp]]] = None
                   ) -> "Negflo":
        """Construct from measured (e.g. downstream gauged) and modelled (e.g.
        routed upstream) flows, computing the residual as measured - modelled.

        The dataframes are aligned on their index; a day is missing in the
        residual if it is missing (or absent) in either input.

        Parameters
        ----------
        df_measured, df_modelled : pd.DataFrame
            Flows with matching column names.
        flow_limit, segments
            See :class:`Negflo`.
        """
        if set(df_measured.columns) != set(df_modelled.columns):
            raise ValueError("Measured and modelled flows must have the same columns. Got "
                             f"{list(df_measured.columns)} and {list(df_modelled.columns)}.")
        return cls(df_measured - df_modelled[df_measured.columns], flow_limit, segments=segments)

    @property
    def df_residual(self) -> pd.DataFrame:
        """The result of the most recent analysis (read-only copy)."""
        return self._df_residual.copy()

    def _run(self, analysis_type: NegfloAnalysisType,
             smooth: Callable[[np.ndarray], float]) -> pd.DataFrame:
        """Apply `smooth` to each column of the raw residual, set remaining
        negatives to zero, and record overflows."""
        self._analysis_type = analysis_type
        self.neg_overflows = {}
        result = self._df_residual_raw.copy()
        for i, column in enumerate(result.columns):
            x = result.iloc[:, i].to_numpy(dtype=float, copy=True)
            lost = smooth(x)
            x[x < 0] = 0.0
            result.iloc[:, i] = x
            self.neg_overflows[column] = -lost if lost else 0.0
            if lost:
                logger.info("%s: %s of negative flow could not be redistributed in %s.",
                            column, lost, analysis_type.name)
        self._df_residual = result
        return result.copy()

    def rw1(self) -> pd.DataFrame:
        """The raw residual, unchanged.

        Returns
        -------
        pd.DataFrame
            The raw residual dataframe.
        """
        self._analysis_type = NegfloAnalysisType.RAW
        self.neg_overflows = dict.fromkeys(self._df_residual_raw.columns, 0.0)
        self._df_residual = self._df_residual_raw.copy()
        return self._df_residual.copy()

    def cl1(self) -> pd.DataFrame:
        """Set all negative flows to zero.

        Returns
        -------
        pd.DataFrame
            The clipped residual dataframe.
        """
        return self._run(NegfloAnalysisType.CLIPPED, _negative_total)

    def sm1(self) -> pd.DataFrame:
        """Redistribute negative flows across the whole period.

        All negative flows are set to zero and all positive flows are factored
        by::

            1 - abs(Total of the negative flows)/(Total of the positive flows)

        This preserves the variability of the flows and the total volume, and
        is most useful for preserving storage behaviour if the reach flows into
        a dam. The flow limit is not used (i.e. it is zero).

        Returns
        -------
        pd.DataFrame
            The smoothed residual dataframe.
        """
        return self._run(NegfloAnalysisType.SMOOTHED_ALL, _smooth_all)

    def sm2(self) -> pd.DataFrame:
        """Redistribute negative flows into the following flow event, with
        carry-over.

        Each flow event (a run of flows above the flow limit) absorbs the
        negative flows preceding it, reducing the flows' excess above the flow
        limit by the factor::

            1 - abs(Total of the preceding negative flows)/(Total excess of the event)

        If an event is not large enough, its flows are set to the flow limit
        and the remaining negative flow is carried over to the next event.

        This is most useful if the negative flows mainly occur on the rising
        limb of the hydrograph. With a flow limit of zero, the mean flow is
        close to the measured mean, but small flow peaks can be eliminated.
        A higher flow limit preserves these peaks but can severely reduce high
        flows, giving a ranked flow plot with a notch at the flow limit.

        Returns
        -------
        pd.DataFrame
            The smoothed residual dataframe.
        """
        return self._run(NegfloAnalysisType.SMOOTHED_FORWARD,
                         lambda x: _smooth_forward(x, self.flow_limit, carry=True))

    def sm3(self) -> pd.DataFrame:
        """As :meth:`sm2`, but negative flow an event cannot absorb is not
        carried over to the next event.

        Returns
        -------
        pd.DataFrame
            The smoothed residual dataframe.
        """
        return self._run(NegfloAnalysisType.SMOOTHED_FORWARD_NO_CARRY,
                         lambda x: _smooth_forward(x, self.flow_limit, carry=False))

    def sm4(self) -> pd.DataFrame:
        """Redistribute negative flows into the preceding flow event, with
        carry-over.

        As :meth:`sm2`, except each event absorbs the negative flows following
        it. Negative flow an event cannot absorb, and negative flow before the
        first event, is carried over to the next event. This is most useful if
        the negative flows mainly occur on the falling limb of the hydrograph.

        Returns
        -------
        pd.DataFrame
            The smoothed residual dataframe.
        """
        return self._run(NegfloAnalysisType.SMOOTHED_BACKWARD,
                         lambda x: _smooth_backward(x, self.flow_limit, carry=True))

    def sm5(self) -> pd.DataFrame:
        """As :meth:`sm4`, but negative flow an event cannot absorb (or that
        precedes the first event) is not carried over.

        Returns
        -------
        pd.DataFrame
            The smoothed residual dataframe.
        """
        return self._run(NegfloAnalysisType.SMOOTHED_BACKWARD_NO_CARRY,
                         lambda x: _smooth_backward(x, self.flow_limit, carry=False))

    def sm6(self, *, use_predefined_segments: bool = True,
            sampling_frequency: Optional[pd.DateOffset] = None,
            sampling_start_date: Optional[pd.Timestamp] = None) -> pd.DataFrame:
        """Redistribute negative flows within each segment, as per :meth:`sm1`.

        Negative flows outside every segment are set to zero. As with
        :meth:`sm1`, the flow limit is not used.

        Assumes the index of the residual can be interpreted as dates.

        Parameters
        ----------
        use_predefined_segments : bool, default True
            Use :attr:`segments` if they exist. Otherwise segments are
            generated from `sampling_frequency` and `sampling_start_date`.
        sampling_frequency : pd.DateOffset, optional
            Length of generated segments. Defaults to one year.
        sampling_start_date : pd.Timestamp, optional
            Start of the first generated segment. Defaults to the start of the
            data.

        Returns
        -------
        pd.DataFrame
            The smoothed residual dataframe.

        See Also
        --------
        * :meth:`sm1`
        """
        dates = pd.DatetimeIndex(pd.to_datetime(self._df_residual_raw.index))
        if use_predefined_segments and self.segments is not None:
            segments = [(pd.Timestamp(start), pd.Timestamp(end)) for start, end in self.segments]
        else:
            segments = _generate_segments(
                pd.Timestamp(dates[0] if sampling_start_date is None else sampling_start_date),
                dates[-1],
                pd.DateOffset(years=1) if sampling_frequency is None else sampling_frequency)

        masks = []
        for start, end in segments:
            if end < start:
                raise ValueError(f"Segment ends before it starts: {(start, end)}")
            mask = np.asarray((dates >= start) & (dates <= end))
            if any((mask & other).any() for other in masks):
                raise ValueError(f"Segment overlaps another segment: {(start, end)}")
            masks.append(mask)

        return self._run(NegfloAnalysisType.SMOOTHED_SEGMENTS,
                         lambda x: _smooth_segments(x, masks))

    def sm7(self) -> pd.DataFrame:
        """Redistribute negative flows into the larger adjacent flow event.

        The negative flows between two flow events are absorbed (as per
        :meth:`sm2`) by whichever of the two events has the larger total
        excess above the flow limit. Negative flows before the first event or
        after the last are absorbed by that event. Negative flow an event
        cannot absorb is not carried over.

        Returns
        -------
        pd.DataFrame
            The smoothed residual dataframe.
        """
        return self._run(NegfloAnalysisType.SMOOTHED_NEG_LIM,
                         lambda x: _smooth_adjacent(x, self.flow_limit))

    def run_all(self, folder: str = ".") -> None:
        """Runs all analyses on the residual, saving each to ``folder``."""
        for analysis in (self.rw1, self.cl1, self.sm1, self.sm2, self.sm3,
                         self.sm4, self.sm5, self.sm6, self.sm7):
            analysis()
            self.to_file(folder=folder)

    def to_file(self, *, out_filename: Optional[str] = None, folder: Optional[str] = None) -> None:
        """Saves the result of the most recent analysis to a csv file. Missing
        data is written as an empty value.

        Parameters
        ----------
        out_filename : str, optional
            Explicit output path. If it has no extension, the extension of the
            current analysis type is added. If omitted, the filename is derived
            from ``df_name`` and the current analysis type extension.
        folder : str, optional
            Directory in which to place the auto-named file. Ignored when
            ``out_filename`` is supplied.
        """
        if out_filename is None:
            out_filename = (
                ("result" if self.df_name is None else self.df_name)
                + self._analysis_type.to_file_extension())
            if folder is not None:
                out_filename = os.path.join(folder, out_filename)
        elif not os.path.splitext(out_filename)[1]:
            out_filename += self._analysis_type.to_file_extension()
        self.df_residual.to_csv(out_filename)
