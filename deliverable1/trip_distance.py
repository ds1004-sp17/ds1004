import csv
import argparse
import os
import re

from pyspark import SparkContext
from operator import add


def matches_date(string):
    '''Detects if a string is a date, and extract the date values.

    Output:
        {year: '2015', month: '03', ...}, or
        None, if the regex failed to match.'''

    date_str = re.compile('(\d\d\d\d)-(\d\d)-(\d\d) (\d\d):(\d\d):(\d\d)')

    # Sample date: 2015-01-15 19:23:42
    match = date_str.match(string)

    if not match:
        return None

    date = {
        'year': int(match.group(1)),
        'month': int(match.group(2)),
        'day': int(match.group(3)),
        'hour': int(match.group(4)),
        'minute': int(match.group(5)),
        'second': int(match.group(6)),
        'year_month': str(match.group(1)) + "/" + str(match.group(2))
    }
    return date


def csv_row_read(x):
    return next(csv.reader([x]))


def filter_amount(x, ind, max_amount=200.0):
    try:
        val = float(x[ind])
        if val > 0 and val < max_amount:
            return True
        else:
            return False
    except:
        return False


def filter_distance(x, ind, max_distance=100.0):
    try:
        val = float(x[ind])
        if val > 0 and val < max_distance:
            return True
        else:
            return False
    except:
        return False


def main():
    parser = argparse.ArgumentParser(description='Parser for time-series of trips')

    parser.add_argument('--filepaths_file', type=str, required=True)
    parser.add_argument('--min_partitions', type=int, default=5,
                        help='minimum number of data partitions when loading')
    parser.add_argument('--output_path', type=str, required=True,
                        help='path for output file')
    args = parser.parse_args()

    sc = SparkContext()
    values_list = []

    with open(args.filepaths_file) as f:
        filepaths = f.readlines()
        filepaths = [x.strip() for x in filepaths]

    for filename in filepaths:

        rdd = sc.textFile(filename, minPartitions=args.min_partitions)
        header = rdd.first() #extract header
        header_list = [x.lower().strip() for x in csv_row_read(header)]
        rdd = rdd.filter(lambda row: row != header)

        pickup_datetime_ind = 1
        distance_ind = header_list.index('trip_distance')
        total_amount_ind = header_list.index('total_amount')

        # Split into columns.
        all_rows = rdd.map(lambda x: csv_row_read(x))
        # Get rows that have the containing column.
        rows = all_rows.filter(lambda col: len(col) > total_amount_ind)

        # Filter out outlier or invalid distance amounts
        rows = rows.filter(lambda x: filter_distance(x, distance_ind))

        # Filter out invalid currency amounts
        rows = rows.filter(lambda x: filter_amount(x, total_amount_ind))

        values = rows.map(lambda x: (matches_date(x[pickup_datetime_ind])['year_month'], float(x[distance_ind]))).reduceByKey(add)
        values_list.append(values)

    result = sc.union(values_list)
    result.saveAsTextFile(args.output_path)


if __name__ == "__main__":
    main()
