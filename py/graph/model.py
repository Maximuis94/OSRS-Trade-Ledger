"""
In this module the Graph model is defined.


A Datapoint is defined as;
- A tuple of x, y coordinates
- It has a label, which is used to identify its Graph

A Graph is defined as;
- A set of Datapoints projected on a 2-dimensional space
- The set of datapoints is clustered via a label
- Graphs can be distinguished via color and label, among others.

A Canvas is defined as;
- A 2-dimensional space onto which datapoints are plotted
- Datapoints are introduced via Graphs
- A Canvas has a set of (labelled) axes (x-, y-), which dictates the exact position of its datapoints


This model serves as an interface for the rather extensive matplotlib package.

"""
from collections import namedtuple
from collections.abc import Callable, Iterable
from abc import ABC, abstractmethod
from typing import List, Tuple, Type

from matplotlib import pyplot as plt, patches as mpatches

from graph.components.color import Color
from graph.components.datapoint import IAxis
from graph.components.line import Line

Datapoint = namedtuple('Datapoint', ['label', 'x', 'y'])
Axis = namedtuple('Axis', ['label', 'format_tick', 'format_value', ])


class Graph:
    label: str
    datapoints: List[Datapoint]
    line: Line
    
    def __init__(self, label: str, datapoints: List[Datapoint], XAxis: Type[IAxis], yAxis: Type[IAxis], color: Color,
                 **kwargs):
        self.label = label
        self.datapoints = datapoints
        
        self.line = Line(color=color, **{k: kwargs[k] for k in frozenset(Line.__match_args__).intersection(kwargs)})
        self.y_axis = Axis()
        self.x_axis = Axis()
        

class Canvas(ABC):
    """
    A Canvas is a 2-dimensional space used to visualize a set of datapoints
    """
    graphs: List[Graph]
    axes: plt.Axes
    
    x_range: Tuple[float, float]
    y_range: Tuple[float, float]
    
    vplot_small: Iterable[Graph]
    vplot_large: Iterable[Graph]
    
    def __init__(self):
        ...
    
    @abstractmethod
    def plot(self):
        """ Plot all the graphs loaded in this Canvas, using the config attributes """
        ...
    
    
    
    