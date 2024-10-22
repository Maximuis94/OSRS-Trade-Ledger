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
from typing import List, Tuple

from matplotlib import pyplot as plt, patches as mpatches


Datapoint = namedtuple('Datapoint', ['label', 'x', 'y'])
Axis = namedtuple('Axis', ['label', 'format_tick', 'format_value', ])


class RGB:
    """"""
    def __init__(self, r: int, b: int, g: int, a: float = 1.0):
        self.red, self.blue, self.green, self.alpha = r, g, b, a
        self.c = self.get_color_code((r, g, b))
        self.rgb = (r, g, b, a)
    
    @staticmethod
    def get_color_code(rgb: Tuple[int, int, int]) -> str:
        """ Converts an r, g, b tuple into a color code """
        return "#%02x%02x%02x" % rgb


class Patch(mpatches.Patch):
    """
    A patch dictates the color assigned to a graph and it is set within the legend of the canvas.
    
    """
    
    def __init__(self, color: RGB, label: str, **kwargs):
        super().__init__(color=color.rgb, label=label, **kwargs)
        self.rgb = color


class Graph(ABC):
    label: str
    datapoints: List[Datapoint]
    patch: Patch
    x_range: Tuple[float, float]
    y_range: Tuple[float, float]
    
    def __init__(self, label: str, datapoints: List[Datapoint], color: RGB, line_width: float = None,
                 line_marker: str = None, line_style: str = None):
        self.label = label
        self.datapoints = datapoints
        
        self.patch = Patch(color=color, label=label)
        self.line_width = line_width
        self.line_marker = line_marker
        self.line_style = line_style
        

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
    
    
    
    