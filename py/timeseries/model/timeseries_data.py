"""
Module with TimeseriesData class, which represents a timeseries data from a particular timespan for a particular item



"""
from typing import NamedTuple, Optional


class TimeseriesDatapoint(NamedTuple):
    item: int
    src: int
    timestamp: int
    price: int
    volume: Optional[int]
    