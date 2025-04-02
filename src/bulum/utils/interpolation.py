""" 
This is a straight wrapper for :py:func:`numpy.interp`.
"""
import numpy as np


def interp(*args):
    """
    .. deprecated:: Use :py:func:`numpy.interp`

    This is just a straight wrapper for `np.interp()`. The only reason I've
    added this is because I knew you'd look here for an interpolation function.
    You probably should use `np.interp()`.
    """
    return np.interp(*args)
