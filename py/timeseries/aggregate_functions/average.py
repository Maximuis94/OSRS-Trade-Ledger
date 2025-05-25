"""
SQLite aggregate function definition.
Serves mostly as a working example, given that it is already builtin.
"""
from timeseries.aggregate_functions._base_aggregate_function import AggregateFunction


class Avg(AggregateFunction):
    def __init__(self):
        self.count = 0
        self.total = 0.0

    def step(self, value):
        # Skip NULLs
        if value is not None:
            self.count += 1
            self.total += value

    def finalize(self):
        if self.count == 0:
            return None
        return self.total / self.count
    