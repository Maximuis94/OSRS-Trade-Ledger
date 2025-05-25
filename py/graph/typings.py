"""
Module with various parameter type definitions

"""
from typing import Literal


HourOfDay = Literal[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]
"""Hour of day as an int"""

DayOfWeek = Literal[0, 1, 2, 3, 4, 5, 6]
"""Day of week as an int"""