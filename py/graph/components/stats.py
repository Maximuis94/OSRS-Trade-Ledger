"""
This module contains namedtuple classes with a variety of specific statistics.
Class instances are to be created via CLASS.compute()

ValueRanges: Basic statistics that describe the distribution of values across both axes.


"""
from collections.abc import Sequence
from typing import NamedTuple, Tuple
import numpy as np


class ValueRanges(NamedTuple):
    """
    Immutable class with various basic statistics derived from a plot. The stats listed here describe the range of the
    values within the plot;
    n: amount of values in x and y
    xmin, ymin: minimum value of x and y
    xq1, yq1: q1 (25%) value of x and y
    xmdn, ymdn: median (50%) value of x and y
    xq3, yq3: q3 (75%) value of x and y
    xmax, ymax: maximum value of x and y
    xstd, ystd: standard deviation of x and y
    """
    
    n: int
    ymin: int or float
    yq1: int or float
    ymdn: int or float
    yq3: int or float
    ymax: int or float
    ystd: float
    
    xmin: int or float
    xq1: int or float
    xmdn: int or float
    xq3: int or float
    xmax: int or float
    xstd: float
    
    @staticmethod
    def compute(x: Sequence[int or float], y: Sequence[int or float]):
        """
        Compute various statistics of the value ranges of x- and y-values in the plot. Either pass a plot (variable with
         attributes x and y) OR pass x and y as kwargs
        
        Parameters
        ----------
        x : Sequence[any]
            A sequence of x-values of the plot.
        y : Sequence[any]
            A sequence of y-values of the plot.

        Returns
        -------
        PlotStats
            A set of statistics derived from plot or x and y
        
        Raises
        ------
        ValueError
            If the length of x is not equal to the length of y, a ValueError is raised
        """
        n = len(x)
        if n != len(y):
            raise ValueError(f"Mismatch between the length of `x` ({len(x)}) and `y` ({len(y)}) -- they are supposed "
                             f"to be identical...")
        
        _x, _y = np.sort(x), np.sort(y)
        return ValueRanges(
            n=n,
            ymin=_y[0], ymax=_y[-1], ystd=float(np.std(_y)), yq1=_y[int(round(n*.25, 0))], ymdn=_y[int(round(n*.5, 0))],
            yq3=_y[int(round(n*.75, 0))], xmin=_x[0], xmax=_x[-1], xstd=float(np.std(_x)),
            xq1=_x[int(round(n*.25, 0))], xmdn=_x[int(round(n*.5, 0))], xq3=_x[int(round(n*.75, 0))]
        )


class Stats(NamedTuple):
    """ NamedTuple with all statistics. Attribute names are identical to the kwargs of its compute method. """
    
    value_ranges: ValueRanges = None
    value_ranges.__doc__ = "N, minimum, q1 (25%), median (50%), q3 (75%), maximum and std values for x and y"
    
    @staticmethod
    def compute(plot: Tuple[Sequence[int or float], Sequence[int or float]], value_ranges: bool = True):
        """
        Compute statistics for `plot`, specified via the boolean args.
        
        Parameters
        ----------
        plot : Tuple[Sequence[int or float], Sequence[int or float]]
            A tuple with the x-values and y-values of the plot.
        value_ranges : bool, optional, True by default
            If True, compute the five-number summary of the x-axis and the y-axis.
    
        Returns
        -------
        Stats
            A NamedTuple is returned with an entry for each of the statistics that were computed. Entries can be
            accessed via index (preserving the function signature order), or by accessing the stats as an attribute. The
             attribute name is identical to the name of the keyword arg, i.e. if `value_ranges` is True, the resulting
            tuple will have a value_ranges attribute
    
        """
        _x = np.sort(plot[0])
        _y = np.sort(plot[1])
        
        if len(_x) != len(_y):
            raise ValueError(f"Mismatch between the length of `x` ({len(_x)}) and `y` ({len(_y)}) -- they are supposed "
                             f"to be identical...")
        
        stats = {}
        
        if value_ranges:
            stats['value_ranges'] = ValueRanges.compute(_x, _y)
        
        if len(stats) == 0:
            return None
        
        return Stats(**stats)
