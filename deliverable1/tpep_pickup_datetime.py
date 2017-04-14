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
import random
import argparse
from operator import add
from pyspark import SparkContext


parser = argparse.ArgumentParser(description='Process date time.')
parser.add_argument('--path', help='what file in HDFS to load')
parser.add_argument('--min_partitions', type=int, help='how many partitions.')
parser.add_argument('--keep_prob', type=float, help='for valid values, only ' \
    'keep this much. Invalid values are always printed..')

args = parser.parse_args()

date_str = re.compile('(\d\d\d\d)-(\d\d)-(\d\d) (\d\d):(\d\d):(\d\d)')


def matches_date(string):
    '''Detects if a string is a date, and extract the date values.

    Output: 
        {year: '2015', month: '03', ...}, or
        None, if the regex failed to match.'''

    # Sample date: 2015-01-15 19:23:42
    match = date_str.match(string)

    if not match:
        return None

    date = {
        'year': match.group(1),
        'month': match.group(2),
        'day': match.group(3),
        'hour': match.group(4),
        'minute': match.group(5),
        'second': match.group(6)
    }
    return date

def to_csv(l):
    return ','.join(l)

def drop_rows(row):
    # For analysis, so we don't print too much.
    if rows[3] == 'VALID':
        if random.random() < args.keep_prob:
            return True
        return False
    return True

def process(pair):
    '''Processes a date string.

    Args:
        pair: (date_string, occurrence_count)
    Returns:
        row: a list/tuple of values to be csv'ed. Columns:
             (value, base_type, semantic_data_type, label)'''

    date_string, occurrence_count = pair
    if date_string is None:
        return (None, 'NULL', 'missing value', 'INVALID')

    date = matches_date(date_string)
    if not date:
        return (date_string, 'STRING', 'unknown value', 'INVALID')

    return (date_string, 'STRING', 'date and time value', 'VALID')

if __name__ == '__main__':
    icol = 1 # tpep_pickup_datetime
    colname = 'tpep_pickup_datetime'

    sc = SparkContext()
    lines = sc.textFile(args.path, minPartitions=args.min_partitions)
    print(lines.getNumPartitions())
    lines = lines.map(lambda x: (next(csv.reader([x]))[icol], 1))
    values = lines.reduceByKey(add)
    values = values.map(process)

    values = values.filter(drop_rows)
    values.map(to_csv).saveAsTextFile(args.path + '_tpep_pickup_datetime.csv')

