"""
This module contains variables that do not really fit in any other category

"""
import datetime
import time
from collections import namedtuple


##########################################################################
# Time-related values
##########################################################################


months_dict = {'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5, 'June': 6,
               'July': 7, 'August': 8, 'September': 9, 'October': 10, 'November': 11, 'December': 12}

months_tuple = ('', 'January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September',
                'October', 'November', 'December')

months_short = [m[:3] for m in months_tuple]

# days_of_week; index corresponds to datetime.datetime.weekday()
dow = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

# Modified to scale w/ datetime isoweekday range
dow_iso = ['', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

# time unit lengths in seconds
t_unit_sec = {'minute': 60, 'hour': 3600, 'day': 86400, 'week': 604800}


# Max values of n-bits (unsigned) integers. Values are inclusive.
int8_max = 127
uint8_max = 255

int16_max = 32768
uint16_max = 65535

int32_max = 2147483647
uint32_max = 4294967295

# Smallest timestamp of wiki entries
min_ts_wiki = 1427500800

# Timestamp threshold used for computing average daily wiki volume
min_avg_wiki_volume_ts = int(time.time()-8*86400)

# Smallest timestamp for which a wiki entry can have a volume
min_ts_wiki_volume = 1537833600

# Lowest avg5m timestamp in db
min_avg5m_ts = 1616688900

# Lowest avg5m timestamp that can be queried
min_avg5m_ts_query_online = 1680001200


# datetime/timestamp of ge tax implementation
ge_tax_min_dt = datetime.datetime(2021, 12, 9, 12, 0, 0)
ge_tax_min_ts = 1639047600

# Update frequency of the sqlite database in seconds
db_update_frequency = 14400
