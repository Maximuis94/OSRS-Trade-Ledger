"""
This module lists almost all the pragmas that are available (acquired via PRAGMA pragma_list;).
The interface defined in this module, IPragma, can be implemented by an SQLite database connection, which provides
methods for pragmas, as well as specific args, documentation, annotations and references.

Pragmas listed below are native to sqlite and therefore to sqlite3. This module was written to define function calls for
specific PRAGMAs with documentation.

References
----------
https://www.sqlite.org/pragma.html
    Official SQLite documentation on implemented PRAGMAs. Documentation found on this webpage *always* overrules
    documentation on the same PRAGMA listed here, should it contradict each other.
"""

import sqlite3
import time
from abc import ABC, abstractmethod
from collections import namedtuple
from collections.abc import Callable
from typing import List, Tuple

from multipledispatch import dispatch

# Placeholder class needed for dispatch 
# class IPragma:
#     ...

# _UTFEncodings = namedtuple('UTFEncodings', ('UTF-8', 'UTF-16', 'UTF-16le', 'UTF-16be'))
# encodings = _UTFEncodings('UTF-8', 'UTF-16', 'UTF-16le', 'UTF-16be')


class IPragma(ABC):
    """
    This interface extends the sqlite3 database with various methods to execute PRAGMAs. Additionally, each PRAGMA
    method is extended with documentations from the official website and in some cases personal experience.
    
    The default parameter values listed in docstrings (if applicable), correspond to the default SQLite configurations.
    Because of overloading, this does not mean the default value is passed, as in many cases there is a method for
    querying the Connection's current value that takes no args, which is called instead.
    E.g. sqlite3.Connection.auto_vacuum() returns the current value of auto_vacuum, while
    sqlite3.Connection.auto_vacuum(1) sets the current value of auto_vacuum to 1, even though the docstring would
    suggest that not passing args implies passing default value 0.
    the item that is passed by default, as often an overloaded function that accepts no args will be called instead.
    
    Almost all PRAGMAs have been included (and commented out if not implemented), some exceptions are;
        - PRAGMAs not returned by the pragma_list PRAGMA of an in-memory db connection
        - PRAGMAs that are deprecated
        - PRAGMAs designed for testing
        - PRAGMAs not deemed useful
        - PRAGMAs that compromise data safety
    
    In some cases, mostly the latter, the PRAGMA is still listed as a method, but it will be commented out.
    Note that this interface was designed to make PRAGMAS more accessible from within a Python environment, as it
    provides annotations, args and an integrated PRAGMA description. Other than a (hopefully) improved developer
    experience, this interface does not provide any benefits.
    
    References
    ----------
    https://www.sqlite.org/pragma.html
        Official SQLite documentation on implemented PRAGMAs. Documentation on this webpage *always* overrules
        documentation listed here, should it contradict each other.
    """
    _c_idx0: sqlite3.Cursor
    
    @abstractmethod
    def __init__(self):
        ...
    
    @dispatch(Callable)
    def analysis_limit(self) -> int:
        """ Query the limit on the approximate number of rows examined in each index by ANALYZE """
        return self._c_idx0.execute("PRAGMA analysis_limit").fetchone()
    
    @dispatch(Callable, int)
    def analysis_limit(self, n: int):
        """ Set the limit on the approximate number of rows examined in each index by ANALYZE to `n` """
        self._c_idx0.execute("PRAGMA analysis_limit = ?", (n,))

#     @dispatch(Callable)
#     def application_id(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_application_id
#             Official SQLite docs for PRAGMA application_id
#         
#         """
#         return self._c_idx0.execute("PRAGMA application_id").fetchone()

    @dispatch(Callable)
    def auto_vacuum(self) -> int:
        """ Query the auto-vacuum status in the database """
        return self._c_idx0.execute("PRAGMA main.auto_vacuum").fetchone()

    @dispatch(Callable, int)
    def auto_vacuum(self, status: int):
        """
        Query or update the auto-vacuum status of the database. `status` should be either 0 (=None), 1 (=FULL)
        or 2 (=INCREMENTAL).

        Parameters
        ----------
        status : int, optional, 0 by default
            If given, the auto-vacuum status of the database will be set to this value.

        References
        ----------
        https://www.sqlite.org/pragma.html#pragma_auto_vacuum
        Official SQLite docs for PRAGMA auto_vacuum

        """
        self._c_idx0.execute("PRAGMA auto_vacuum = ?", (status,))

    @dispatch(Callable)
    def automatic_index(self) -> bool:
        """ Query the automatic-indexing status of the database """
        return bool(self._c_idx0.execute("PRAGMA automatic_index").fetchone())

    @dispatch(Callable, bool)
    def automatic_index(self, is_enabled: bool):
        """
        Query or set the status of automatic-indexing.

        Parameters
        ----------
        is_enabled : bool, optional, None by default
            If given, the automatic-indexing status of the database will be set to `is_enabled`

        Returns
        -------
        bool, optional, True by default
            If the status is queried (i.e. no arg passed), return a bool that represents the current status
        None
            If the status is updated, return None

        References
        ----------
            https://www.sqlite.org/pragma.html#pragma_automatic_index
            Official SQLite docs for PRAGMA automatic_index

        """
        self._c_idx0.execute("PRAGMA automatic_index = ?", (int(is_enabled),))

#     @dispatch(Callable)
#     def busy_timeout(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_busy_timeout
#             Official SQLite docs for PRAGMA busy_timeout
#         
#         """
#         return self._c_idx0.execute("PRAGMA busy_timeout").fetchone()

#     @dispatch(Callable)
#     def cache_size(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_cache_size
#             Official SQLite docs for PRAGMA cache_size
#         
#         """
#         return self._c_idx0.execute("PRAGMA cache_size").fetchone()

#     @dispatch(Callable)
#     def cache_spill(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_cache_spill
#             Official SQLite docs for PRAGMA cache_spill
#         
#         """
#         return self._c_idx0.execute("PRAGMA cache_spill").fetchone()

#     @dispatch(Callable)
#     def case_sensitive_like(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_case_sensitive_like
#             Official SQLite docs for PRAGMA case_sensitive_like
#         
#         """
#         return self._c_idx0.execute("PRAGMA case_sensitive_like").fetchone()

    @dispatch(Callable)
    def cell_size_check(self):
        """ Query the cell_size_check status """
        return self._c_idx0.execute("PRAGMA cell_size_check").fetchone()

    @dispatch(Callable, bool)
    def cell_size_check(self, is_enabled: bool):
        """
        Query or set the cell_size_check status.

        Parameters
        ----------
        is_enabled : bool, optional, False by default
            If given, the cell_size_check status of the database will be set to `is_enabled`

        Returns
        -------
        bool, optional, True by default
            If the status is queried (i.e. no arg passed), return a bool that represents the current status
        None
            If an arg was passed, return None

        References
        ----------
            https://www.sqlite.org/pragma.html#pragma_cell_size_check
            Official SQLite docs for PRAGMA cell_size_check

        """
        self._c_idx0.execute("PRAGMA cell_size_check = ?", (int(is_enabled),))

#     @dispatch(Callable)
#     def checkpoint_fullfsync(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_checkpoint_fullfsync
#             Official SQLite docs for PRAGMA checkpoint_fullfsync
#         
#         """
#         return self._c_idx0.execute("PRAGMA checkpoint_fullfsync").fetchone()

    @dispatch(Callable)
    def collation_list(self) -> List[str]:
        """
        Query the list of collation sequences set for this database

        Returns
        -------
        List[str]
            A list of collation sequences that apply to this database

        References
        ----------
            https://www.sqlite.org/pragma.html#pragma_collation_list
            Official SQLite docs for PRAGMA collation_list

        """
        return self._c_idx0.execute("PRAGMA collation_list").fetchall()

#     @dispatch(Callable)
#     def compile_options(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_compile_options
#             Official SQLite docs for PRAGMA compile_options
#         
#         """
#         return self._c_idx0.execute("PRAGMA compile_options").fetchone()

#     @dispatch(Callable)
#     def data_version(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_data_version
#             Official SQLite docs for PRAGMA data_version
#         
#         """
#         return self._c_idx0.execute("PRAGMA data_version").fetchone()

    @dispatch(Callable)
    def database_list(self) -> List[Tuple[int, str, str]]:
        """
        Query all databases associated with this connection and return them as a list. The second column is 'main' for
        the main database file, 'temp' for the TEMP objects, or the name of the attached database. The third column is
        the name of the database file of that database, if applicable.

        Returns
        -------
        List[Tuple[int, str, str]]
            A list of size N, with one row for each db associated with this connection. Second and third columns
            are the db name and db file.

        References
        ----------
            https://www.sqlite.org/pragma.html#pragma_database_list
            Official SQLite docs for PRAGMA database_list

        """
        return self._c_idx0.execute("PRAGMA database_list").fetchone()

    @dispatch(Callable)
    def defer_foreign_keys(self) -> bool:
        """ Query the current status of defer_foreign_keys """
        return bool(self._c_idx0.execute("PRAGMA defer_foreign_keys").fetchone())

    @dispatch(Callable, bool)
    def defer_foreign_keys(self, is_enabled: bool):
        """
        Query or update the current status of defer_foreign_keys for this database.
        
        Parameters
        ----------
        is_enabled : bool, optional, False by default
            If passed, the status will be set equal to `is_enabled`

        Returns
        -------
        bool
            If no arg is passed, the current status of defer_foreign_keys will be returned
        None
            If an arg was passed, nothing will be returned

        References
        ----------
            https://www.sqlite.org/pragma.html#pragma_defer_foreign_keys
            Official SQLite docs for PRAGMA defer_foreign_keys

        """
        return self._c_idx0.execute("PRAGMA defer_foreign_keys = ?", (is_enabled,)).fetchone()

    @dispatch(Callable)
    def encoding(self):
        """ Queries encoding applied to this connection """
        return self._c_idx0.execute("PRAGMA encoding").fetchone()
    
    @dispatch(Callable, str)
    def encoding(self, encoding: str):
        """
        Set the character encoding for this connection to `encoding`
        
        Parameters
        ----------
        encoding: str, optional, "UTF-8" by default
            The UTF encoding that should be configured

        Returns
        -------
        str
            If no arg is passed, the active encoding is returned
        None
            If an encoding is passed, nothing is returned

        References
        ----------
            https://www.sqlite.org/pragma.html#pragma_encoding
            Official SQLite docs for PRAGMA encoding

        """
        return self._c_idx0.execute("PRAGMA encoding = ?", (encoding,)).fetchone()

    @dispatch(Callable)
    def foreign_key_check(self) -> bool:
        """ Query the status of foreign_key_check """
        return bool(self._c_idx0.execute("PRAGMA foreign_key_check").fetchone())

    @dispatch(Callable, bool)
    def foreign_key_check(self, is_enabled: bool):
        """
        Query or update the current status of foreign_key_check for this database.

        Returns
        -------
        bool
            If no arg is passed, the current status of foreign_key_check will be returned
        None
            If an arg was passed, nothing will be returned

        References
        ----------
            https://www.sqlite.org/pragma.html#foreign_key_check
            Official SQLite docs for PRAGMA foreign_key_check

        """
        return self._c_idx0.execute("PRAGMA foreign_key_check = ?", (int(is_enabled),))

    def foreign_key_list(self, table_name: str) -> List[str]:
        """
        Query the list of foreign_keys in table `table_name`
        
        Parameters
        ----------
        table_name : str
            The name of the table that is to be checked for foreign keys

        Returns
        -------
        List[str]
            A list of foreign keys found in table `table_name`

        References
        ----------
            https://www.sqlite.org/pragma.html#pragma_foreign_key_list
            Official SQLite docs for PRAGMA foreign_key_list

        """
        return self._c_idx0.execute("PRAGMA foreign_key_list(?)", (table_name,)).fetchall()

    @dispatch(Callable)
    def foreign_keys(self) -> bool:
        """ Query the status of foreign_keys """
        return bool(self._c_idx0.execute("PRAGMA foreign_keys").fetchone())

    @dispatch(Callable, bool)
    def foreign_keys(self, is_enabled: bool):
        """
        Query or update the current status of foreign_keys for this database.
        
        Parameters
        ----------
        is_enabled : bool, optional, False by default
            If passed, the status will be set equal to `is_enabled`

        Returns
        -------
        bool
            If no arg is passed, the current status of foreign_keys will be returned
        None
            If an arg was passed, nothing will be returned

        References
        ----------
            https://www.sqlite.org/pragma.html#foreign_keys
            Official SQLite docs for PRAGMA foreign_keys

        """
        return self._c_idx0.execute("PRAGMA foreign_keys = ?", (int(is_enabled),))

    @dispatch(Callable)
    def freelist_count(self) -> int:
        """
        Return the number of free pages in the database file associated with the connection

        Returns
        -------
        int
            The number of free pages in the database file

        References
        ----------
            https://www.sqlite.org/pragma.html#pragma_freelist_count
            Official SQLite docs for PRAGMA freelist_count

        """
        return self._c_idx0.execute("PRAGMA freelist_count").fetchone()

    @dispatch(Callable)
    def function_list(self) -> List[str]:
        """
        Query the list functions known to this database

        Returns
        -------
        List[str]
            A list of functions that can be executed via connections established with this database

        References
        ----------
            https://www.sqlite.org/pragma.html#pragma_function_list
            Official SQLite docs for PRAGMA function_list

        """
        return self._c_idx0.execute("PRAGMA function_list").fetchall()

#     @dispatch(Callable)
#     def hard_heap_limit(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_hard_heap_limit
#             Official SQLite docs for PRAGMA hard_heap_limit
#         
#         """
#         return self._c_idx0.execute("PRAGMA hard_heap_limit").fetchone()

    def ignore_check_constraints(self, ignore_check: bool):
        """
        Instruct the db engine to either ignore check constraints or not
        
        Parameters
        ----------
        ignore_check : bool, optional, False by default
            If set to True, the checks will no longer be enforced.

        References
        ----------
            https://www.sqlite.org/pragma.html#pragma_ignore_check_constraints
            Official SQLite docs for PRAGMA ignore_check_constraints

        """
        return self._c_idx0.execute("PRAGMA ignore_check_constraints = ?", (ignore_check,)).fetchone()

#     @dispatch(Callable)
#     def incremental_vacuum(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_incremental_vacuum
#             Official SQLite docs for PRAGMA incremental_vacuum
#         
#         """
#         return self._c_idx0.execute("PRAGMA incremental_vacuum").fetchone()

    @dispatch(Callable)
    def index_info(self) -> list:
        """
        This pragma returns one row for each key column in the named index. A key column is a column that is actually
        named in the CREATE INDEX index statement or UNIQUE constraint or PRIMARY KEY constraint that created the index.
        Index entries also usually contain auxiliary columns that point back to the table row being indexed. The
        auxiliary index-columns are not shown by the index_info pragma, but they are listed by the index_xinfo pragma.

        Output columns from the index_info pragma are as follows:
        1. The rank of the column within the index. (0 means left-most.)
        2. The rank of the column within the table being indexed. A value of -1 means rowid and a value of -2 means that
            an expression is being used.
        3. The name of the column being indexed. This columns is NULL if the column is the rowid or an expression.

        If there is no index named index-name but there is a WITHOUT ROWID table with that name, then (as of SQLite
        version 3.30.0 on 2019-10-04) this pragma returns the PRIMARY KEY columns of the WITHOUT ROWID table as they
        are used in the records of the underlying b-tree, which is to say with duplicate columns removed.


        Returns
        -------
        None


        References
        ----------
            https://www.sqlite.org/pragma.html#pragma_index_info
            Official SQLite docs for PRAGMA index_info

        """
        return self._c_idx0.execute("PRAGMA index_info").fetchone()

    # @dispatch(Callable)
    # def index_list(self):
    #     """
    #
    #
    #     Returns
    #     -------
    #     None
    #
    #
    #     References
    #     ----------
    #         https://www.sqlite.org/pragma.html#pragma_index_list
    #         Official SQLite docs for PRAGMA index_list
    #
    #     """
    #     return self._c_idx0.execute("PRAGMA index_list").fetchone()

#     @dispatch(Callable)
#     def index_xinfo(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_index_xinfo
#             Official SQLite docs for PRAGMA index_xinfo
#         
#         """
#         return self._c_idx0.execute("PRAGMA index_xinfo").fetchone()

#     @dispatch(Callable)
#     def integrity_check(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_integrity_check
#             Official SQLite docs for PRAGMA integrity_check
#         
#         """
#         return self._c_idx0.execute("PRAGMA integrity_check").fetchone()

#     @dispatch(Callable)
#     def journal_mode(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_journal_mode
#             Official SQLite docs for PRAGMA journal_mode
#         
#         """
#         return self._c_idx0.execute("PRAGMA journal_mode").fetchone()

#     @dispatch(Callable)
#     def journal_size_limit(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_journal_size_limit
#             Official SQLite docs for PRAGMA journal_size_limit
#         
#         """
#         return self._c_idx0.execute("PRAGMA journal_size_limit").fetchone()

#     @dispatch(Callable)
#     def legacy_alter_table(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_legacy_alter_table
#             Official SQLite docs for PRAGMA legacy_alter_table
#         
#         """
#         return self._c_idx0.execute("PRAGMA legacy_alter_table").fetchone()

#     @dispatch(Callable)
#     def locking_mode(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_locking_mode
#             Official SQLite docs for PRAGMA locking_mode
#         
#         """
#         return self._c_idx0.execute("PRAGMA locking_mode").fetchone()

#     @dispatch(Callable)
#     def max_page_count(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_max_page_count
#             Official SQLite docs for PRAGMA max_page_count
#         
#         """
#         return self._c_idx0.execute("PRAGMA max_page_count").fetchone()

#     @dispatch(Callable)
#     def mmap_size(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_mmap_size
#             Official SQLite docs for PRAGMA mmap_size
#         
#         """
#         return self._c_idx0.execute("PRAGMA mmap_size").fetchone()

#     @dispatch(Callable)
#     def module_list(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_module_list
#             Official SQLite docs for PRAGMA module_list
#         
#         """
#         return self._c_idx0.execute("PRAGMA module_list").fetchone()

#     @dispatch(Callable)
#     def optimize(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_optimize
#             Official SQLite docs for PRAGMA optimize
#         
#         """
#         return self._c_idx0.execute("PRAGMA optimize").fetchone()

#     @dispatch(Callable)
#     def page_count(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_page_count
#             Official SQLite docs for PRAGMA page_count
#         
#         """
#         return self._c_idx0.execute("PRAGMA page_count").fetchone()

#     @dispatch(Callable)
#     def page_size(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_page_size
#             Official SQLite docs for PRAGMA page_size
#         
#         """
#         return self._c_idx0.execute("PRAGMA page_size").fetchone()

#     @dispatch(Callable)
#     def pragma_list(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_pragma_list
#             Official SQLite docs for PRAGMA pragma_list
#         
#         """
#         return self._c_idx0.execute("PRAGMA pragma_list").fetchone()

#     @dispatch(Callable)
#     def query_only(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_query_only
#             Official SQLite docs for PRAGMA query_only
#         
#         """
#         return self._c_idx0.execute("PRAGMA query_only").fetchone()

#     @dispatch(Callable)
#     def quick_check(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_quick_check
#             Official SQLite docs for PRAGMA quick_check
#         
#         """
#         return self._c_idx0.execute("PRAGMA quick_check").fetchone()

#     @dispatch(Callable)
#     def read_uncommitted(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_read_uncommitted
#             Official SQLite docs for PRAGMA read_uncommitted
#         
#         """
#         return self._c_idx0.execute("PRAGMA read_uncommitted").fetchone()

#     @dispatch(Callable)
#     def recursive_triggers(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_recursive_triggers
#             Official SQLite docs for PRAGMA recursive_triggers
#         
#         """
#         return self._c_idx0.execute("PRAGMA recursive_triggers").fetchone()

#     @dispatch(Callable)
#     def reverse_unordered_selects(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_reverse_unordered_selects
#             Official SQLite docs for PRAGMA reverse_unordered_selects
#         
#         """
#         return self._c_idx0.execute("PRAGMA reverse_unordered_selects").fetchone()

#     @dispatch(Callable)
#     def schema_version(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_schema_version
#             Official SQLite docs for PRAGMA schema_version
#         
#         """
#         return self._c_idx0.execute("PRAGMA schema_version").fetchone()

#     @dispatch(Callable)
#     def secure_delete(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_secure_delete
#             Official SQLite docs for PRAGMA secure_delete
#         
#         """
#         return self._c_idx0.execute("PRAGMA secure_delete").fetchone()

#     @dispatch(Callable)
#     def short_column_names(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_short_column_names
#             Official SQLite docs for PRAGMA short_column_names
#         
#         """
#         return self._c_idx0.execute("PRAGMA short_column_names").fetchone()

#     @dispatch(Callable)
#     def shrink_memory(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_shrink_memory
#             Official SQLite docs for PRAGMA shrink_memory
#         
#         """
#         return self._c_idx0.execute("PRAGMA shrink_memory").fetchone()

#     @dispatch(Callable)
#     def soft_heap_limit(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_soft_heap_limit
#             Official SQLite docs for PRAGMA soft_heap_limit
#         
#         """
#         return self._c_idx0.execute("PRAGMA soft_heap_limit").fetchone()

#     @dispatch(Callable)
#     def synchronous(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_synchronous
#             Official SQLite docs for PRAGMA synchronous
#         
#         """
#         return self._c_idx0.execute("PRAGMA synchronous").fetchone()

#     @dispatch(Callable)
#     def table_info(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_table_info
#             Official SQLite docs for PRAGMA table_info
#         
#         """
#         return self._c_idx0.execute("PRAGMA table_info").fetchone()

#     @dispatch(Callable)
#     def table_list(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_table_list
#             Official SQLite docs for PRAGMA table_list
#         
#         """
#         return self._c_idx0.execute("PRAGMA table_list").fetchone()

#     @dispatch(Callable)
#     def table_xinfo(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_table_xinfo
#             Official SQLite docs for PRAGMA table_xinfo
#         
#         """
#         return self._c_idx0.execute("PRAGMA table_xinfo").fetchone()

#     @dispatch(Callable)
#     def temp_store(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_temp_store
#             Official SQLite docs for PRAGMA temp_store
#         
#         """
#         return self._c_idx0.execute("PRAGMA temp_store").fetchone()

    @dispatch(Callable)
    def threads(self) -> int:
        """ Query the amount of auxiliary threads this connection is allowed to use """
        return self._c_idx0.execute("PRAGMA threads").fetchone()

    @dispatch(Callable, int)
    def threads(self, n: int):
        """
        Query or set the amount of auxiliary threads the database connection is allowed to use.
        
        Parameters
        ----------
        n : int, optional, 0 by default
            Amount of auxiliary threads the database connection is allowed to use.

        Returns
        -------
        int
            If no arg is passed, the current value of threads is returned
        None
            If n is passed, nothing will be returned
            
        References
        ----------
            https://www.sqlite.org/pragma.html#pragma_threads
            Official SQLite docs for PRAGMA threads

        """
        self._c_idx0.execute("PRAGMA threads = ?", (n,))

#     @dispatch(Callable)
#     def trusted_schema(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_trusted_schema
#             Official SQLite docs for PRAGMA trusted_schema
#         
#         """
#         return self._c_idx0.execute("PRAGMA trusted_schema").fetchone()

#     @dispatch(Callable)
#     def user_version(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_user_version
#             Official SQLite docs for PRAGMA user_version
#         
#         """
#         return self._c_idx0.execute("PRAGMA user_version").fetchone()

#     @dispatch(Callable)
#     def wal_autocheckpoint(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_wal_autocheckpoint
#             Official SQLite docs for PRAGMA wal_autocheckpoint
#         
#         """
#         return self._c_idx0.execute("PRAGMA wal_autocheckpoint").fetchone()

#     @dispatch(Callable)
#     def wal_checkpoint(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_wal_checkpoint
#             Official SQLite docs for PRAGMA wal_checkpoint
#         
#         """
#         return self._c_idx0.execute("PRAGMA wal_checkpoint").fetchone()

#     @dispatch(Callable)
#     def writable_schema(self):
#         """
#         
#         
#         Returns    
#         -------
#         None
#             
#         
#         References
#         ----------
#             https://www.sqlite.org/pragma.html#pragma_writable_schema
#             Official SQLite docs for PRAGMA writable_schema
#         
#         """
#         return self._c_idx0.execute("PRAGMA writable_schema").fetchone()

# t1 = time.perf_counter()
# print(_mem.execute('PRAGMA main.index_list').fetchall())
# exit(1)
#
#
# _mem.row_factory = lambda c, row: row[0]
# pragma_list: tuple = tuple([el for el in _mem.execute("PRAGMA pragma_list").fetchall()])
# function_list: tuple = tuple([el for el in _mem.execute("PRAGMA function_list").fetchall()])
# print(f"{1000*(time.perf_counter()-t1):.0f}ms")
# print('\n\n\n')
# max_len = 0
# previous = ''
# for f in function_list:
#     if f == previous:
#         continue
#     previous = f
#     max_len = max(len(f), max_len)
#     print(f'{f.upper(): >25}: ')
# exit(max_len)
# def locking_mode(schema: str = 'main', exclusive: bool = None):
#     """
#     This pragma sets or queries the database connection locking-mode.
#
#     Parameters
#     ----------
#     schema : str, optional, 'main' by default
#         Database schema the PRAGMA should apply to
#     exclusive : bool, optional,
#
#     Returns
#     -------
#
#     """
#     ...
#
# # namedtuple('PRAGMA')
# del _mem
#
# if __name__ == '__main__':
#     for p in pragma_list[1:]:
#         print(f'''# \t@dispatch(Callable)\n# \tdef {p}(self):\n# \t\t"""\n# \t\t\n# \t\t\n# \t\tReturns\t\n# \t\t-------\n# \t\tNone\n# \t\t\t\n# \t\t\n# \t\tReferences\n# \t\t----------\n# \t\t\thttps://www.sqlite.org/pragma.html#pragma_{p}\n# \t\t\tOfficial SQLite docs for PRAGMA {p}\n# \t\t\n# \t\t"""\n# \t\treturn self._c_idx0.execute("PRAGMA {p}").fetchone()\n\n''')
