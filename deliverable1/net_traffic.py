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

pu_datetime_index = 1
do_datetime_index = 2
pu_location_id_index = 7
do_location_id_index = 8

parser = argparse.ArgumentParser(description='Taxi net traffic.')
parser.add_argument('--input_dir', type=str, default='public/taxis/',
                    help='location of csv files in HDFS.')
parser.add_argument('--start_month', type=int, default=7, help='start month.')
parser.add_argument('--end_month', type=int, default=12, help='end month.')
parser.add_argument('--save_path', type=str, default='./net_traffic.csv',
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
def process_pair(columns, time_index, loc_id_index):
    '''Processes either input date/pickup, or output date/pickup'''
    date_str = columns[time_index]
    loc_str = columns[loc_id_index]
    _, _, d = datetimes._process(date_str)
    if d is None:
        return None
    try:
        loc = int(loc_str)
    except:
        return None
    if loc < 1 or loc > 263:
        return None
    is_weekend = date(d['year'], d['month'], d['day']).weekday() >= SATURDAY
    return (d['hour'], d['minute'], loc, is_weekend)


def process(string):
    '''Args:
        string: Raw input from the textFile(s)
    Returns:
        [None], or tuple of
            (key: (location, is_weekend, in/out, hour, minute), count: 1)
        If 'string' is invalid, returns [None].'''
    columns = csv_row_read[string]
    if len(columns) < 10:
        return [None]
    try:
        pu_hour, pu_minute, pu_loc, pu_weekend = process_pair(
                columns, pu_datetime_index, pu_location_id_index)
        do_hour, do_minute, do_loc, do_weekend = process_pair(
                columns, do_datetime_index, do_location_id_index)
    except:
        return [None]
    pu_key = (pu_loc, pu_weekend, pu_hour, pu_minute)
    do_key = (do_loc, do_weekend, do_hour, do_minute)
    # Feed to a flatMap.
    return [(pu_key, (1, 0)), (do_key, (0, 1))]


def tuple_add(x, y):
    x0, x1 = x
    y0, y1 = y
    return (x0 + y0, x1 + y1)


def format_add_result(pair):
    key, (pu_count, do_count) = pair
    row = list(key)
    row.append(pu_count)
    row.append(do_count)
    return to_csv(row)

filepaths = [
    os.path.join(
        args.input_dir, 'yellow_tripdata_2016-{:02d}.csv'.format(month))
    for month in range(args.start_month, args.end_month + 1)
]

def main():
    conf = SparkConf().setAppName('net_traffic')
    sc = SparkContext()
    sc.setLogLevel(args.loglevel)
    sc.addPyFile('datetimes.py')

    print('-'*80 + '\n' + 'net traffic counter' + '\n' + '-'*80)
    for filepath in filepaths:
        print(filepath)

    print('Save to:', args.save_path)

    not_null = lambda x: x is not None
    all_data = sc.union([sc.textFile(x) for x in filepaths])
    counts = all_data.flatMap(process).filter(not_null).reduceByKey(tuple_add)
    counts.map(format_add_result).saveAsTextFile(args.save_path)

if __name__ == '__main__':
    main()

