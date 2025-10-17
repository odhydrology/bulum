""" 
This is a straight wrapper for :func:`numpy.interp`.
"""
import numpy as np


def interp(*args):
    """
    Linear interpolation function.

    .. deprecated::
        This function is deprecated. Use :func:`numpy.interp` directly instead.

    This is a simple wrapper around numpy.interp() provided for convenience.
    All arguments are passed directly to the numpy function.

    Parameters
    ----------
    *args
        Arguments passed directly to numpy.interp().

    Returns
    -------
    :class:`numpy.ndarray`
        Interpolated values as returned by numpy.interp().

    See Also
    --------
    :func:`numpy.interp` : The underlying numpy interpolation function.

    Notes
    -----
    This wrapper exists for historical reasons but direct use of numpy.interp()
    is recommended for new code.
    """
    return np.interp(*args)
