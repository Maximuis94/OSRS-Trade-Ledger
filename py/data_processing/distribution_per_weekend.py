"""
Module with an implementation to aggregate timeseries data on a per-weekend basis.
Though nearly identical to the stats per week, it is restricted to the friday, saturday and sundays, UTC time.
"""

import datetime
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal, Tuple

import numpy as np
from overrides import override

import global_variables.path as gp
from common.classes.database import Database
from data_processing.distribution_stats import DistributionStats
from item.itemdb import itemdb, Item

timeseries = Database(gp.f_db_timeseries)

_window_size: int = 604800
"""The size of the window in which data is aggregated"""


@dataclass(frozen=True, slots=True)
class DistributionPerWeekend(DistributionStats):
    """
    Basic distribution stats for a set of numbers that span a week
    """
    
    @property
    def window_size(self) -> int:
        return 604800
    
    @override
    @property
    def timespan(self) -> Tuple[int, int]:
        """Return a t0 that starts at friday 12am and spans until monday 8am"""
        t0 = self.timestamp - self.timestamp % self.window_size
        return t0 + 86400, t0 + int((4+1/3)*86400)
    
    @property
    def timestamp_label(self) -> str:
        """Return the label, defined as YYYY wWW, which Y being the year and W being the week number"""
        return datetime.datetime.fromtimestamp(self.timestamp).strftime("Weekend %Y-w%V")


def get_distribution_metrics(values: Sequence[int | float], item: Item, column: str, timestamp: int) -> DistributionStats:
    """Generate a DistributionStats object with a specific ID format that is consistent across the module"""
    return DistributionStats(values, column, )
