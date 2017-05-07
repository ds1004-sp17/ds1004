from __future__ import print_function

import csv
import re
import argparse
import os
from datetime import date
from cStringIO import StringIO
from operator import add
import datetimes
from pyspark import SparkContext, SparkConf

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

SATURDAY = 5
passenger_count_index = 2
distance_index = 3
# skip payment type
total_amt_index = 5
class HalfTrip(object):
    def __init__(self, mode, columns, d, loc):
        # such and such is in this location at this moment
        # PU = departure
        # DO = arrival
        self.mode = mode
        self.year = d['year']
        self.month = d['month']
        self.day = d['day']
        self.minute = (d['minute'] // 5) * 5
        self.is_weekend = \
                date(d['year'], d['month'], d['day']).weekday() >= SATURDAY
        self.loc = loc

        self.passengers = columns[passenger_count_index]
        self.distance = columns[distance_index]
        self.total = columns[total_amt_index]
        self.count = 1

    def get_time_key(self):
        return (self.loc,
                self.mode,
                self.hour,
                self.minute,
                self.is_weekend)

    def get_daily_key(self):
        return (self.loc,
                self.mode,
                self.year,
                self.month,
                self.day)

    def get_val(self):
        return (self.passengers,
                self.distance,
                self.total,
                self.count)


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
    try:
        pu = get_half_trip(columns, pu_datetime_index, pu_location_id_index)
        do = get_half_trip(columns, do_datetime_index, do_location_id_index)
    except:
        return [None]
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
    for year in range(2013, 2017)
    for month in range(1, 13)
]

def main():
    conf = SparkConf().setAppName('summarize')
    sc = SparkContext()
    sc.setLogLevel(args.loglevel)
    sc.addPyFile('datetimes.py')

    print('-'*80 + '\n' + 'net traffic counter' + '\n' + '-'*80)
    for filepath in filepaths:
        print(filepath)

    print('Save to:', args.save_path)

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

