from __future__ import print_function

import csv
import re
import argparse
import os
from cStringIO import StringIO
from operator import add
import datetimes
from pyspark import SparkContext, SparkConf
from half_trip import HalfTrip

pu_location_id_index = 0
do_location_id_index = 1
pu_datetime_index = 6
do_datetime_index = 7

parser = argparse.ArgumentParser(description='Taxi net traffic.')
parser.add_argument('--input_dir', type=str,
        default='s3://cipta-bigdata1004/',
        help='location of csv files in HDFS.')
parser.add_argument('--daily_path', type=str,
        default='s3://cipta-bigdata1004/daily.csv',
        help='directory in HDFS to save files to.')
parser.add_argument('--minutely_path', type=str,
        default='s3://cipta-bigdata1004/minutely.csv',
        help='directory in HDFS to save files to.')
parser.add_argument('--loglevel', type=str, default='WARN',
                    help='log verbosity.')
args = parser.parse_args()


def csv_row_read(x):
    '''Turns a CSV string (x) into a list of columns.'''
    return next(csv.reader([x]))


def to_csv(l):
    '''Turns a tuple into a CSV row.
    Args:
        l: list/tuple to turn into a CSV
    Returns:
        (str) input encoded as CSV'''
    f = StringIO()
    writer = csv.writer(f)
    writer.writerow(l)
    return f.getvalue().strip()

# skip payment type

def get_daily_keyval(half_trip):
    return (half_trip.get_daily_key(), half_trip.get_val())

def get_time_keyval(half_trip):
    return (half_trip.get_time_key(), half_trip.get_val())

def get_half_trip(columns, mode='PU'):
    '''Processes either input date/pickup, or output date/pickup'''
    if mode == 'PU':
        time_index, loc_id_index = pu_datetime_index, pu_location_id_index
    else:
        mode = 'DO'
        time_index, loc_id_index = do_datetime_index, do_location_id_index
    date_str = columns[time_index]
    loc_str = columns[loc_id_index]
    d = datetimes.matches_date(date_str)
    if d is None:
        return None
    try:
        loc = int(loc_str)
    except:
        return None
    if loc < 1 or loc > 263:
        return None
    return HalfTrip(mode, columns, d, loc)


def process(string):
    '''Args:
        string: Raw input from the textFile(s)
    Returns:
        [None], or [HalfTrip, HalfTrip]
        If 'string' is invalid, returns [None].'''
    columns = csv_row_read(string)
    pu = get_half_trip(columns, mode='PU')
    do = get_half_trip(columns, mode='DO')
    # Feed to a flatMap.
    return [pu, do]


def tuple_add(x, y):
    return tuple([x[i] + y[i] for i in xrange(len(x))])


def format_add_result(pair):
    key, value = pair
    row = list(key)
    row.extend(value)
    return to_csv(row)

filepaths = [
    os.path.join(args.input_dir,
        'yellow_extract_{}-{:02d}.csv'.format(year, month))
    for year in range(2015, 2016)
    for month in range(1, 3)
]

def main():
    conf = SparkConf().setAppName('summarize')
    sc = SparkContext()
    sc.setLogLevel(args.loglevel)
    sc.addPyFile('datetimes.py')
    sc.addPyFile('half_trip.py')

    print('-'*80 + '\n' + 'net traffic counter' + '\n' + '-'*80)
    for filepath in filepaths:
        print(filepath)

    print('Save to:', args.daily_path)
    print('Save to:', args.minutely_path)

    not_null = lambda x: x is not None
    half_trips = sc.union([sc.textFile(x) for x in filepaths])\
        .flatMap(process).filter(not_null).cache()
    
    daily = half_trips.map(get_daily_keyval).reduceByKey(tuple_add)
    daily.sortByKey()\
        .map(format_add_result)\
        .saveAsTextFile(args.daily_path)
    
    minutely = half_trips.map(get_time_keyval).reduceByKey(tuple_add)
    minutely.sortByKey()\
        .map(format_add_result)\
        .saveAsTextFile(args.minutely_path)

if __name__ == '__main__':
    main()

