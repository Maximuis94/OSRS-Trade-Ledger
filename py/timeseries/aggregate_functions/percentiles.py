"""
Module with aggregate functions for computing the Nth percentile


"""
from timeseries.aggregate_functions._base_aggregate_function import AggregateFunction

import sqlite3
import math

class Percentile01(AggregateFunction):
    def __init__(self):
        self.values = []

    def step(self, value):
        if value is not None:
            self.values.append(value)

    def finalize(self):
        n = len(self.values)
        if n == 0:
            return None
        sorted_vals = sorted(self.values)
        k = math.ceil(0.1 * n)
        return sorted_vals[k-1]

# 2) Open a connection and register the aggregate
conn = sqlite3.connect("example.db")
conn.create_aggregate("p01", 1, Percentile01)

# 3) Example usage
cur = conn.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS scores(val REAL);")
cur.executemany("INSERT INTO scores(val) VALUES(?)", [
    (1.0,), (2.0,), (3.0,), (4.0,), (5.0,),
    (6.0,), (7.0,), (8.0,), (9.0,), (10.0,),
])
# p01 returns the value at rank ceil(0.1*10)=1 → the smallest element
cur.execute("SELECT p01(val) FROM scores;")
print("0.1‐quantile:", cur.fetchone()[0])  # → 1.0
