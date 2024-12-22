"""
Module with the class representation of an Axis.
Is mostly derived from the matplotlib Axis class, although it is more tailored towards specific use cases


"""
from abc import abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Tuple, NamedTuple


class AxisOrientation(Enum):
    X=0
    Y=1
    
    
class AxisFormat:
    minor: Callable[[any], str]
    major: Callable[[any], str]
    

@dataclass(frozen=True, slots=True, match_args=True, order=True)
class CoordinatesXY:
    """ A pair of x- and y- coordinates """
    x: int or float
    y: int or float
    

@dataclass(slots=True, frozen=True, match_args=True, order=True)
class Tick:
    """ A single tick that can be displayed on an Axis """
    position: CoordinatesXY
    is_large: bool = False
    label: str = ""
    

@dataclass(slots=True, )
class Ticks:
    """
    Class representation of the Ticks that are to be placed on an axis
    """
    tick_position: int or float or Tuple[int, int] or Tuple[float, float] = 0, 1
    n_ticks: int = 10
    tick_spacing: int or float = None
    is_relative_position: bool = True
    
    def __init__(self, axis_lim: Tuple[int or float, int or float], tick_positions=(0., 1.),
                 n_ticks=None, is_relative_position=True, is_labelled_tick=None):
        """
        Ticks configuration to apply to an axis. By default, place 11 evenly spaced ticks at 0%, 10%, 20%, ..., 90%,
        100%
        
        Parameters
        ----------
        axis_lim: Tuple[int or float, int or float]
            The limits of the underlying axis (min axis value, max axis value)
        tick_positions: Tuple[int or float, ...], optional, (0., 1.) by default
            Tick positions on the axis. If more than 2 positions are passed, this variable describes all tick positions.
            If `is_relative_position` is True and values in this tuple are floats, values are interpreted relative to
            the axis_lim, where 0.0 and 1.0 are respectively 0% and 100%.
        n_ticks: int, optional, None by default
            Amount of ticks to place on the axis; overrides `tick_spacing`. If `is_relative_position` is True and
            `n_ticks`/`tick_spacing` are undefined, `n_ticks` is set to 10.
        is_relative_position: bool, optional, True by default
            If True, tick positions are expressed as a value between 0 and 1, where 0 is the minimum value and 1 the max
        is_labelled_tick: Callable, optional, None by default
            Function that accepts at least an index i, that returns True if the tick at `i` should be labelled
        """
        self.ticks = []
        
        min_tick, max_tick = min(tick_positions), max(tick_positions)
        
        if n_ticks is None and is_relative_position and tick_positions == (0., 1.):
            self.ticks = 0, .1, .2, .3, .4, .5, .6, .7, .8, .9, 1.0
        
        elif n_ticks is not None:
            self.ticks = tuple(range(min_tick, max_tick, (max_tick - min_tick) // n_ticks))
        
        self.n_ticks = len(self.ticks)
        
        if is_labelled_tick is not None:
            ...
        else:
            self.labelled = 0, -1


class TwinAxis(NamedTuple):
    minor_formatter: Callable = None
    major_formatter: Callable = None
    tick_labels: Iterable[str] = ()


class IAxis:
    """
    Base class for an axis. Subclasses are either x- or y-axes; their naming follows a specific format that emphasizes
    this.
    """
    __slots__ = ('small_ticks', 'large_ticks', 'axis_span', 'twin_axis', 'grid_format_small', 'grid_format_large')
    small_ticks: Iterable[int or float]
    large_ticks: Iterable[int or float]
    twin_axis: TwinAxis
    grid_format_small: tuple
    grid_format_large: tuple
    
    @abstractmethod
    def set_axis_span(self, values: Iterable):
        """ Derive+set the value span for this axis, given `values`. By default, use min/max values """
        raise NotImplementedError
    
    @abstractmethod
    def set_gridlines(self, small: Iterable[int or float, ...] = None, large: Iterable[int or float, ...] = None):
        """ Generate small and/or large gridlines using the small and large grid formats """
        raise NotImplementedError
    
    @abstractmethod
    def set_twin_axis(self, value_formatter: Callable = None, tick_labels: Iterable[str] = None):
        """
        Set a twin axis. The twin axis is identical up to a certain degree or even completely. It shares the same ticks,
        although its tick labels and formatter may differ

        Parameters
        ----------

        Returns
        -------

        """
        raise NotImplementedError
    
    def convert_values(self, values: Iterable[any], func: Callable = None):
        """ Preprocess the raw values from `values` using `func` or `self.convert_raw_value` """
        try:
            if func is None:
                func = self.convert_raw_value
            return [func(v) for v in values]
        except NotImplementedError:
            return values
    
    @abstractmethod
    def convert_raw_value(self, value: any, *args, **kwargs) -> any:
        """ Converts a raw value into a preprocessed value; e.g. coverts unix into datetime. Apply if _convert_data  """
        raise NotImplementedError("Convert data is True, while convert_raw_value is not overridden...")
    
    def _major_format(self, value) -> str:
        """ Method to apply for major plot format. This is the value that is displayed within plots. """
        return str(value)
    
    def _minor_format(self, value) -> str:
        """ Method to apply for minor plot format of the preprocessed value. By default, use str(self). """
        return str(value)
    
    def tick_label(self, tick_value, *args, **kwargs) -> str:
        """ Given `tick_value`, return the corresponding tick label """
        return self._major_format(tick_value)


class Axis:
    """
    Class representation of an axis. The Axis class describe how the axis should be realized; ticks, labels,
    minor/major value formats, value types, grid_lines, ...

    The Axis is used as reference by specific graphs, NOT the other way around!

    TODO: Integrate with existing matplotlib Axis, use this object as a precursor for generating a matplotlib.Axis
    """
    label: str
    formatter: Callable
    span: Tuple[int, int] or Tuple[float, float]
    ticks: Tuple[Tick, ...] = (0., .1, .2, .3, .4, .5, .6, .7, .8, .9, 1.0)
    labelled_ticks: Tuple[int, ...]
    relative_ticks: bool = True
    value_type: IAxis
    grid_lines: Iterable
    
    def __init__(self, label: str, span: Tuple[int, int] or Tuple[float, float], value_type: IAxis,
                 formatter: Callable or Tuple[Callable, Callable] = None, ticks: Tuple[int or float, ...] = None,
                 labelled_ticks: Iterable[int] = None, relative_ticks: bool = True):
        self.label = label
        self.value_type = value_type
        
        if ticks is None:
            ticks = self.ticks
            relative_ticks = True
        if relative_ticks:
            v0, v1 = min(span), max(span)
            delta = v1 - v0
            ticks = [min(span) + delta * p for p in ticks]
        self.ticks = ticks
        
        if labelled_ticks is None:
            self.labelled_ticks = (self.ticks[0], self.ticks[len(self.ticks) // 2], self.ticks[-1])
        
        if formatter is None:
            def formatter(value):
                return str(value)
        if isinstance(formatter, Callable):
            self.major_formatter = formatter
            self.minor_formatter = formatter
        elif len(formatter) == 2:
            self.major_formatter, self.minor_formatter = formatter
        else:
            raise ValueError(f"Unable to extract minor/major formatters from formatter={formatter}")
    
    def add_twin_axis(self):
        ...


