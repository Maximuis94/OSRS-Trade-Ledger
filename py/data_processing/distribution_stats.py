from collections.abc import Sequence

import numpy as np
from typing import Tuple

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class DistributionStats:
    """
    Basic distribution stats for a set of numbers
    
    
    """
    values: Sequence[int | float] = field(repr=False)
    
    v_min: int | float= field(default=None, init=False)
    v_05: int | float= field(default=None, init=False)
    v_q1: int | float= field(default=None, init=False)
    v_median: int | float= field(default=None, init=False)
    v_q3: int | float= field(default=None, init=False)
    v_95: int | float= field(default=None, init=False)
    v_max: int | float= field(default=None, init=False)
    
    def __post_init__(self):
        values = np.array(self.values)
        object.__setattr__(self, "v_min", float(round(min(values), 2)))
        object.__setattr__(self, "v_05", float(round(np.percentile(values, 0.05), 2)))
        object.__setattr__(self, "v_q1", float(round(np.percentile(values, 0.25), 2)))
        object.__setattr__(self, "v_median", float(round(np.percentile(values, 0.5), 2)))
        object.__setattr__(self, "v_q3", float(round(np.percentile(values, 0.75), 2)))
        object.__setattr__(self, "v_95", float(round(np.percentile(values, 0.95), 2)))
        object.__setattr__(self, "v_max", float(round(max(values), 2)))
    
    