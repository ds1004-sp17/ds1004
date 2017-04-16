import csv
import argparse
import re

from pyspark import SparkContext
from operator import add

def matches_date(string):
    date_str = re.compile('(\d\d\d\d)-(\d\d)-(\d\d) (\d\d):(\d\d):(\d\d)')
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

def to_float(x):
    try:
        return float(x)
    except:
        return x


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

    for col, filt in (('total_amount', 5), ('tolls_amount', 1),('tip_amount', 1),('fare_amount',5)):
        values_list = []
        for filename in filepaths:
            rdd = sc.textFile(filename, minPartitions=args.min_partitions)
            header = rdd.first() #extract header
            header_list = [x.lower().strip() for x in csv_row_read(header)]
            rdd = rdd.filter(lambda row: row != header)

            ind = header_list.index(col)

            all_rows = rdd.map(lambda x: csv_row_read(x))
            rows = all_rows.filter(lambda col: len(col) >= ind)

            values = rows.map(lambda x: (int(to_float(x[ind]) // filt * filt), 1))
            values = values.reduceByKey(add)
            values_list.append(values)

        result = sc.union(values_list).reduceByKey(add)
        result.saveAsTextFile(args.output_path + '_' + col)



if __name__ == '__main__':
    main()





