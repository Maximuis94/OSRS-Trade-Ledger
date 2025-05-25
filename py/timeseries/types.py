"""
Module with type and metaclass definitions in the context of the TimeseriesDatabase.
Types are named

"""
from enum import Enum

from typing import Iterable, Literal, Tuple

SrcLike = Literal[0, 1, 2, 3, 4] | Iterable[Literal[0, 1, 2, 3, 4]]
"""Parameter type for src. It contains all the integers that occur as src, and the definition for passing multiple"""


OrderBy = str | Tuple[str, bool]
"""OrderBy values. The str denotes the column, the bool the sorting order ASC (True; default) or DESC (False)"""


Orderable = OrderBy | Iterable[OrderBy]
"""One or more OrderBy elements that specify in which sequence the requested data is to be ordered"""



