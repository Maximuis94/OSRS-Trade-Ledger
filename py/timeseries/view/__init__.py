from enum import Enum

from timeseries.view.timeseries_extended_1d import extended_timeseries_view_1d as _1d
from timeseries.view.timeseries_extended_5m import extended_timeseries_view_5m as _5m


class TimeseriesView(Enum):
    FIVE_MINUTES = (_5m, "5m")
    ONE_DAY = (_1d, "1d")
    
    def __str__(self):
        return self.value[0]
    
    @property
    def row_timespan(self) -> str:
        return self.value[1]


def sql_create_timeseries_extension_view(item_id: int, sql: TimeseriesView) -> str:
    """Inject the table name into the extended view SQL based on `item_id` and return it"""
    return (str(sql)
            .replace('__TIMESERIES_TABLE__', f'"item{item_id:0>5}"')
            .replace('__OUT_TABLE__', f'"item{item_id:0>5}_{sql.row_timespan}_extended"'))


__all__ = ['sql_create_timeseries_extension_view', "TimeseriesView"]
