"""
Module with various evaluation functions that
"""
from collections.abc import Callable
from typing import Optional

from gui.component.interface.row import IRow
from gui.util.colors import Rgba
from gui.util.generic import Number


EvaluationFunctionReturn = Optional[Rgba] | Rgba
""""""


EvaluationFunction = Callable[[IRow], Optional[Rgba] | Rgba]
"""A function that accepts an IRow and returns"""


def red_yellow_green(attribute: str, lower_bound: Number, upper_bound: Number) -> EvaluationFunction:
    """
    Return a function that accepts a value and converts it into an Rgba instance that slides from red to yellow to green
    If the given value exceeds either `lower_bound` or `upper_bound` the function the same color as if the upper bound
    was reached.
    
    Parameters
    ----------
    attribute : str
        The name of the attribute in the given Row that is being evaluated
    lower_bound : Number
        The lower bound value of the color scale. As the value approaches the lower bound, it will converge towards red
    upper_bound : Number
        The lower bound value of the color scale. As the value approaches the lower bound, it will converge towards green

    Returns
    -------

    """
    d_lower, d_upper = lower_bound / 240, upper_bound / 240
    
    def evaluation_function(value: Number) -> Rgba:
        """Converts `value` to a color that scales from red->green, given its position between lower- and upper-bound"""
        return Rgba(min(240, value/d_lower), 240, 85) if value < (upper_bound-lower_bound)/2 \
            else Rgba(240, min(240, value/d_upper), 85)
    return evaluation_function


def step_function(threshold_value: Number, color_lower: Optional[Rgba] = None, color_upper: Optional[Rgba] = None)\
        -> EvaluationFunctionReturn:
    """
    Generate a step function, that evaluates a particular value and
    
    Parameters
    ----------
    threshold_value
    color_lower
    color_upper

    Returns
    -------

    """
