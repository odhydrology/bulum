import numpy as np


def interp(p, xp, fp):
    """This is just a straight wrapper for np.interp(). The only reason I've added this is 
    because I knew you'd look here for an interpolation function. You probably should use 
    np.interp().

    Args:
        p (array_like): The x-coordinates at which to evaluate the interpolated values.
        xp (1-D sequence of floats): The x-coordinates of the data points, must be increasing if argument period is not specified. Otherwise, xp is internally sorted after normalizing the periodic boundaries with xp = xp % period.
        fp (1-D sequence of float or complex): The y-coordinates of the data points, same length as xp.
    """
    answer = np.interp(p, xp, fp)
    return answer
