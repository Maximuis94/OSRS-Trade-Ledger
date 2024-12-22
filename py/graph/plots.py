"""
This module contains various classes that represent plots of specific types of data. Various components are combined
into functional classes.
"""
from collections.abc import Sequence

from pandas.errors import SettingWithCopyError

from graph.components.axis import Axis
from graph.components.color import *
from graph.components.datapoint import *
import graph.components.string_formats as fmt
from axes import *


class IPlot(ABC):
    """
    Class representation of a plot. A plot is a series of datapoints plotted somewhere.
    
    """
    
    datapoints: Sequence[Datapoint2D]
    
    x_axis: Axis
    y_axis: Axis
    
    @abstractmethod
    def convert_datapoint(self, datapoint: Datapoint2D) -> Datapoint2D:
        return datapoint


class TimeseriesGraph(IPlot):
        x_axis: XUnixTimestamp
        y_axis: YAxis


class DOWGraph(IPlot):
    x_axis: XAxisDayOfWeek
    y_axis: YAxis
    
    
    