from __future__ import print_function

"""Process date columns.

This script takes the NYC TripData file for one month.

Then it computes statistics:
    - Unique year, month, day, down to seconds (INPUT.year.csv, etc.)
    - List of strings that don't look like dates (INPUT.nondate.csv, etc.)
    Each output is comma-separated with counts of rows that follow the criteria.
"""

import sys
import re
import csv
from operator import add
from pyspark import SparkContext

raw = sys.argv[1]
min_partitions = int(sys.argv[2])
column = sys.argv[3]

date_str = re.compile('(\d\d\d\d)-(\d\d)-(\d\d) (\d\d):(\d\d):(\d\d)')

icol = 0
if column == 'tpep_pickup_datetime':
    icol = 1
elif column == 'tpep_dropoff_datetime':
    icol = 2
else:
    raise ValueError('column must be pickup/dropoff time')

def matches_date(s):
    """Input: (string, count)
    Output: either
        (string, count) or
        ({year: '2015', month: '03', ...}, count)"""

    # Sample date: 2015-01-15 19:23:42
    string, count = s
    match = date_str.match(string)

    if not match:
        return s

    date = {
        'year': match.group(1),
        'month': match.group(2),
        'day': match.group(3),
        'hour': match.group(4),
        'minute': match.group(5),
        'second': match.group(6)
    }
    return (date, count)

def isstr(s):
    return isinstance(s, str) or isinstance(s, unicode)

def getprop(key):
    def do_fn(tup):
        o, count = tup
        try:
            return (o[key], count)
        except TypeError:
            raise ValueError(str(tup))
    return do_fn

def to_csv(s):
    return '{0},{1}'.format(s[0], s[1])

def process(sc, strs):
    """Input:
        SparkContext,
        RDD [ string, string, string ] , each string a date
    Output: None, but saves text files"""

    strs = lines.map(lambda l: (l, 1))
    counts = strs.reduceByKey(add)

    dates = counts.map(matches_date)
    invalids = dates.filter(lambda x: isstr(x[0]))
    valids = dates.filter(lambda x: not isstr(x[0]))

    dims = ['year', 'month', 'day', 'hour', 'minute', 'second']
    for d in dims:
        dimcount = valids.map(getprop(d))
        sums = dimcount.reduceByKey(add)
        sums.map(to_csv).saveAsTextFile('{0}.{1}.{2}.csv'.format(raw, column, d))

    invalids.map(to_csv).saveAsTextFile('{0}.{1}.nondate.csv'.format(raw, column))

if __name__ == '__main__':
    sc = SparkContext()
    lines = sc.textFile(raw, minPartitions=min_partitions)
    print(lines.getNumPartitions())
    lines = lines.map(lambda x: next(csv.reader([x]))[icol])

    process(sc, lines)

