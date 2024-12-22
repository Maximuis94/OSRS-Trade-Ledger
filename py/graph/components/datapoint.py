"""
This module contains template classes for values that can be plotted.
In these templates, basic logic is defined to enforce certain behaviours/constraints.

PlotValue classes are defined as immutable data classes. Additional methods/attributes are added to make them deployable
 as graph plot values.

The interface class, IPlotValue, covers a detailed description, as well as common features for datapoint values.



References
----------
https://noklam-data.medium.com/how-to-achieve-partial-immutability-with-python-dataclasses-or-attrs-0baa0d818898
"""
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime
from email.policy import default
from math import floor
from typing import Any, Tuple, NamedTuple
import matplotlib as plt
from matplotlib.axis import XAxis as _XAxis, YAxis as _YAxis

from attr.setters import frozen
from overrides import override

import graph.components.string_formats as fmt
from graph.components.color import Color
from graph.components.axis import IAxis


class Line(NamedTuple):
    color: Color
    linewidth: int or float = 1.
    style: str = '-'


########################################################################################################################
#   datapoints
########################################################################################################################
class PriceGraph:
    X: Callable = XAxisUnixTimestamp
    Y: Callable = YAxisPrice


@dataclass(slots=True, frozen=True)
class Datapoint2D(ABC):
    """
    Representation of a single, 2-dimensional datapoint.
    In its most basic form, it is a tuple that consists of 2 two values.
    
    
    """
    x: IAxis
    y: IAxis


class TimeseriesDatapoint(Datapoint2D):
    """
    Datapoint for a Timeseries plot. Its x-value is a unix timestamp, whereas its y-value can vary.

    """
    
    def __new__(cls, x: int or float, y: int or float):
        return cls(x, y)


class HODDatapoint(Datapoint2D):
    """
    Datapoint for a Timeseries plot. Its x-value is a unix timestamp, whereas its y-value can vary.
    
    """
    def __new__(cls, x: int or float, y: int or float):
        return cls(x//3600, y)


class DOWDatapoint(Datapoint2D):
    """
    Datapoint for a Timeseries plot. Its x-value is a unix timestamp, whereas its y-value can vary.
    
    """
    def __new__(cls, x: int or float, y: int or float):
        return cls(x//86400, y)


def format_datetime(v: int) -> str:
    """  """
    return datetime.fromtimestamp(v).strftime('%d-%m-%Y %H:%M:%S')


if __name__ == '__main__':
    t = time.perf_counter()
    print(issubclass(YAxisPrice, IAxis))
    print(isinstance(YAxisPrice, IAxis))
    dp = None
    print(time.time(), XAxisUnixTimestamp(time.time()))
    
    t1 = time.perf_counter()
    # print(f"{1000*(t1-t):.0f}ms", dp, time.time())
    