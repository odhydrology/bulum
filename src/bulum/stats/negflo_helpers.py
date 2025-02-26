"""Bulum, negflo supporting classes.

C.f. https://qldhyd.atlassian.net/wiki/spaces/MET/pages/524386/Negflo
"""

import logging
from enum import Enum
from typing import Any, Callable, Optional
import functools

import pandas as pd

logger = logging.getLogger(__name__)


class FileType(Enum):
    """Negflo file types. For use in config file setup."""
    IQQM = 0
    IQQM_GUI = 1
    SOURCE_INPUT = 2
    SOURCE_OUTPUT = 3


class AnalysisType(Enum):
    """Negflo class keeps track of most recent analysis performed."""
    RAW = -1
    CLIPPED = 0
    SMOOTHED_ALL = 1  # sm1
    SMOOTHED_FORWARD = 2  # sm2
    SMOOTHED_FORWARD_NO_CARRY = 3  # sm3
    SMOOTHED_BACKWARD = 4  # sm4
    SMOOTHED_BACKWARD_NO_CARRY = 5  # sm5
    SMOOTHED_SPECIFIED = 6  # sm6
    SMOOTHED_NEG_LIM = 7  # sm7

    def to_file_extension(self) -> str:
        """Gives the corresponding file extension for """
        match self:
            case AnalysisType.RAW:
                return ".rw1"
            case AnalysisType.CLIPPED:
                return ".cl1"
            case AnalysisType.SMOOTHED_ALL:
                return ".sm1"
            case AnalysisType.SMOOTHED_FORWARD:
                return ".sm2"
            case AnalysisType.SMOOTHED_FORWARD_NO_CARRY:
                return ".sm3"
            case AnalysisType.SMOOTHED_BACKWARD:
                return ".sm4"
            case AnalysisType.SMOOTHED_BACKWARD_NO_CARRY:
                return ".sm5"
            case AnalysisType.SMOOTHED_SPECIFIED:
                return ".sm6"
            case AnalysisType.SMOOTHED_NEG_LIM:
                return ".sm7"
            case _:
                raise ValueError(f"Unhandled/invalid enum, {self}")


class ContiguousTracker:
    """Convenience class to track contiguous blocks of data as determined by
    index. Essentially a wrapper over a list with convenience methods."""

    def __init__(self):
        # tracks start pt of positive period
        self.start_idx = None
        # tracks most recent position of positive period
        self.last_idx = None
        self.acc = []

    def __len__(self):
        return len(self.acc)

    def __iter__(self):
        # This is mostly here to allow sum() to act on this class.
        return iter(self.acc)

    def indices(self):
        """Returns a range of indices of the associated collection
        for which values were tracked."""
        return range(self.start_idx, self.start_idx + len(self.acc))

    def force_add(self, idx: int, v) -> None:
        """Be careful with this, as it may invalidate any computations to do
        with the indices of the values."""
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

    def offset(self, offset_val: Any | Callable[[Any], Any]) -> list[Any]:
        if callable(offset_val):
            return list(map(offset_val, self.acc))
        else:
            # This performs a shallow copy
            return list(map(lambda x: x + offset_val, self.acc))

    def sum_and_reset(self):
        """Returns the sum of the underlying accumulator and resets the trackers."""
        x = sum(self)
        self.reset()
        return x

    def is_tracking(self):
        """Checks if the tracker is active.

        If start_idx is not null then it is required that last_idx is also not
        null."""
        return self.start_idx is not None

    def is_member_of_block(self, idx):
        """Asks whether idx is adjacent to the currently tracked block of integers."""
        return self.is_tracking() and (self.start_idx - 1 <= idx <= self.last_idx + 1)

    def reset(self, /, idx: Optional[int] = None, val: Optional[list] = None):
        """Resets to default or resets to current index/value (list)."""
        if val is None:
            val = []
        self.start_idx = self.last_idx = idx
        self.acc = val


def dec_sm_helpers_log_neg_rem(func):
    """Decorator to standardise treatment of remaining negative flow after
    execution. Internal use only."""
    @functools.wraps(func)
    def _impl(self, residual: pd.Series, *args, **kwargs):
        series, neg_overflow = func(self, residual, *args, **kwargs)
        self.neg_overflow[residual.name] = neg_overflow
        if neg_overflow < 0:
            logger.warning(f"Negative flow remaining after execution: {neg_overflow}")
        return series
    return _impl
