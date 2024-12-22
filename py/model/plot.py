"""
Model class for a plot

"""
import sqlite3
from collections.abc import Callable

from venv_auto_loader.active_venv import *
from global_variables.data_classes import PlotStats
from sqlite.row_factories import timeseries_row_factory
__t0__ = time.perf_counter()


class Plot:
    """
    Model for a Plot.
    A plot refers to a plot of timestamps, y-values for one specific item within a specified time frame.
    
    Multiple plots can be added to a canvas.
    """
    
    def __init__(self, item_id: int, t0: int, t1: int, y_value: str, db_path: str, process_datapoint: Callable = None,
                 sqlite_where: str = None, parameters: list = None, **kwargs):
        """
        Constructor for the plot
        
        Parameters
        ----------
        item_id : int
            Item_id of the item the data represents
        t0 : int
            Lower bound timestamp (inclusive)
        t1 : int
            Upper bound timestamp (inclusive)
        y_value : str
            Name of the y-value in the database the data is extracted from
        db_path : str
            Path to the database the data is to be extracted from
        process_datapoint : Callable, optional, None by default
            Method for processing the datapoints. Its signature should look like;
            <CALLABLE(xy: List[TimeseriesDatapoint], dp: TimeseriesDatapoint) -> List[TimeseriesDatapoint]>,
            where xy is a list of TimeseriesDatapoints and dp is a candidate TimeseriesDatapoint.
            Note that it has to return the TimeseriesDatapoint list with or without the datapoint given.
        sqlite_where : str, optional, None by default
            Where clause that will be appended to the sql statement, can be used instead of/along with
            `process_datapoint`. Additional parameters should be supplied via `parameters` as tuple/list
        parameters : list, optional, None by default
            If `sqlite_where` is passed and it requires parameters, those parameters should be supplied here. Note that
            they should NOT be passed as dict.
            
        Notes
        -----
        If passing a method for process_datapoint, make sure the args are ordered identically in the signature and as
        in the return statement. Since the x, y tuple represents a single datapoint, make sure to add both or neither.
        
        Examples
        --------
        process_datapoint: lambda ar, xy: ar + [xy]
            The method shown above is the bare minimum of what should be passed, i.e. Datapoint `xy` is added to `ar`
        """
        db = sqlite3.connect(database=f'file:{db_path}?mode=ro', uri=True)
        db.row_factory = timeseries_row_factory
        self.db_path = db_path
        
        sql = f'SELECT timestamp, {y_value} FROM item{item_id:0>5} WHERE timestamp BETWEEN ? AND ? '
        if sqlite_where is not None:
            sql += sqlite_where.replace('WHERE', '')
        self.xy = db.execute(sql, (t0, t1) if parameters is None else tuple([t0, t1] + list(parameters))).fetchall()
        
        if process_datapoint is not None:
            xy = []
            for dp in self.xy:
                xy = process_datapoint(xy, dp)
            self.xy = xy
        
        self.stats = PlotStats.get(self.x, self.y)
        self.x, self.y = tuple(self.x), tuple(self.y)
    
    def __repr__(self):
        # todo
        raise NotImplementedError
    
    def __eq__(self, other):
        try:
            return self.x == other.x and self.y == other.y
        except AttributeError:
            return False
    
    