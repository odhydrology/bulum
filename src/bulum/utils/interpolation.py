"""
This module provides interpolation utilities.

.. deprecated:: 0.1.0
    The :func:`interp` function is deprecated. Use :func:`numpy.interp` directly instead.
"""
import warnings
from typing import Any

import numpy as np


def interp(*args: Any, **kwargs: Any) -> np.ndarray:
    """
    Linear interpolation function.

    .. deprecated:: 0.1.0
        This function is deprecated and will be removed in a future version.
        Use :func:`numpy.interp` directly instead.

    This is a simple wrapper around numpy.interp() provided for convenience.
    All arguments are passed directly to the numpy function.

    Parameters
    ----------
    *args
        Arguments passed directly to numpy.interp().
    **kwargs
        Keyword arguments passed directly to numpy.interp().

    Returns
    -------
    np.ndarray
        Interpolated values as returned by numpy.interp().

    See Also
    --------
    numpy.interp : The underlying numpy interpolation function.

    Notes
    -----
    This wrapper exists for historical reasons but direct use of numpy.interp()
    is recommended for new code.

    Examples
    --------
    >>> # Deprecated usage
    >>> x = [1, 2, 3]
    >>> y = [10, 20, 30]
    >>> xi = [1.5, 2.5]
    >>> result = interp(xi, x, y)  # Will show deprecation warning

    >>> # Recommended usage
    >>> result = np.interp(xi, x, y)
    """
    warnings.warn(
        "bulum.utils.interp() is deprecated and will be removed in a future version. "
        "Use numpy.interp() directly instead.",
        DeprecationWarning,
        stacklevel=2
    )
    return np.interp(*args, **kwargs)
