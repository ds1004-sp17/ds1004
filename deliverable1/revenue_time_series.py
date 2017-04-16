import csv
import argparse
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

def filter_amount(x, ind):
    try:
        val = float(x[ind])
        if val > 0 and val < 200:
            return True
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
        pickup_datetime_ind = 1

        rdd = sc.textFile(filename, minPartitions=args.min_partitions)
        header = rdd.first() #extract header
        rdd = rdd.filter(lambda row: row != header)

        header = [x.lower() for x in csv_row_read(header)]
        total_amount_ind = header.index('total_amount')

        # Split into columns.
        all_rows = rdd.map(lambda x: csv_row_read(x))
        # Get rows that have the containing column.
        rows = all_rows.filter(lambda col: len(col) > total_amount_ind)

        # Filter out invalid currency amounts
        rows = rows.filter(lambda x: filter_amount)

        values = rows.map(lambda x: (matches_date(x[pickup_datetime_ind])['year_month'], (1, float(x[total_amount_ind]))))
        values = values.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
        values_list.append(values)

    result = sc.union(values_list)
    result.saveAsTextFile(args.output_path)



if __name__ == '__main__':
    main()