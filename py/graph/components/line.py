"""
Module for line stylings

"""
from dataclasses import dataclass, field

from matplotlib import patches

from graph.components.color import Color, Rgba


class Patch(patches.Patch):
    """ A patch dictates the color assigned to a graph and it is set within the legend of the canvas. """
    def __init__(self, color: Rgba, label: str, **kwargs):
        super().__init__(color=color.tuple, label=label, **kwargs)
        self.rgba: Rgba = color


@dataclass(slots=True, match_args=True)
class Line:
    """Line dataclass; defines basic styling properties of this Line"""
    
