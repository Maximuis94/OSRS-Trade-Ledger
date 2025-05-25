"""
Module with an abstract template class for an aggregate function.
Since this approach involves lots of Python -> C conversions, an SQL statement seems preferable.
As such, an executable sql can be implemented in the subclass as well.


"""
from typing import get_type_hints, Optional

from dataclasses import dataclass

import sqlite3

from abc import ABC, abstractmethod

SQLiteDataType = None | int | float | str | bytes
"""The Python datatypes that may be returned by finalize()"""


@dataclass(slots=True, init=False, repr=False)
class AggregateFunction(ABC):
    name: str
    """The name of the aggregate function in SQL statements"""
    
    n_args: int
    """The amount of arguments accepted by this aggregate function"""
    
    # Verify if the subclass is properly implemented
    def __subclasshook__(cls, __subclass):
        if __debug__:
            cls_name = __subclass.__name__
            msg = None
            type_hints = get_type_hints(cls)
            annots = getattr(__subclass, '__annotations__', {})
            for a in ("name", "n_args"):
                if not hasattr(__subclass, a):
                    if msg is None:
                        msg = f"While verifying subclass {cls_name}, the following errors were encountered;\n"
                    msg += f"Subclass {cls_name} does not have attribute {a} defined on class level\n"
                    
                expected_type = type_hints.get(a)
                actual_type = annots.get(a, None)
                if actual_type is None:
                    attr = getattr(__subclass, a)
                    if isinstance(attr, property):
                        actual_type = getattr(attr.fget, '__annotations__', {}).get('return')
                
                if not actual_type is expected_type:
                    if msg is None:
                        msg = f"While verifying subclass {cls_name}, the following errors were encountered;\n"
                    msg += f"Mismatch between expected type (={expected_type}) and actual type (={actual_type}) for attribute '{a}'\n"
            
            if msg is not None:
                raise RuntimeError(msg)
    
    @abstractmethod
    def step(self, *args):
        """Step function that is applied to every value encountered in the given set of values"""
        raise NotImplementedError

    @abstractmethod
    def finalize(self) -> SQLiteDataType:
        """Called after the entire set of values has been traversed. Prepares and returns the final value"""
        raise NotImplementedError
    
    def sql(self, *args, **kwargs) -> str:
        """Executable SQL with pre-defined values"""
        raise NotImplementedError
    
    @classmethod
    def register(cls, con: sqlite3.Connection) -> sqlite3.Connection:
        """Register this aggregate function on the given connection and return it"""
        con.create_aggregate(cls.name, cls.n_args, cls)
        return con
    
    def verify_sql(self, con: sqlite3.Connection, sql_registered_function: str, sql_executable: str) -> Optional[bool]:
        """Evaluate the SQL by comparing its output with the function in a connection in which this AggregateFunction
        was registered.
        
        Parameters
        ----------
        con : sqlite3.Connection
            Database connection to use for benchmarking
        sql_registered_function: str
            Executable SQL that uses the function registered by this AggregateFunction
        sql_executable : str
            Executable SQL that is supposed to produce the same output as the step and finalize methods combined

        Returns
        -------
        bool
            True if the SQL produces the same output, False if not.
        None
            Returned value if the method is called outside a debugging environment
            
        Raises
        ------
        RuntimeError
            If there is a mismatch between outputs, a RuntimeError is raised.

        """
        if __debug__:
            con = self.register(con)
            a = tuple(con.execute(sql_registered_function).fetchall())
            b = tuple(con.execute(sql_executable).fetchall())
            if a != b:
                msg = (f"Mismatch between outputs! Output from registered sql is {a}, "
                       f"whereas the output from the executable sql is {b}")
                raise RuntimeError(msg)
            return True
        