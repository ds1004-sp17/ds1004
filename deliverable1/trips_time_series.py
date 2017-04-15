import csv
import argparse
import os
from org.apache.hadoop.fs import FileSystem

from pyspark import SparkContext


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


def main():
    parser = argparse.ArgumentParser(description='Parser for time-series of trips')

    parser.add_argument('--folder_path', type=str, required=True,
                        help='location of folder containing csv file in HDFS.')
    parser.add_argument('--min_partitions', type=int, default=5,
                        help='minimum number of data partitions when loading')
    parser.add_argument('--output_path', type=str, required=True,
                        help='path for output file')
    args = parser.parse_args()

    sc = SparkContext()
    values_list = []

    for filename in FileSystem.get(sc.hadoopConfiguration()).listFiles(args.folder_path, true):
        filename = os.path.join(args.folder_path, filename)
        rdd = sc.textFile(filename, minPartitions=args.min_partitions)
        header = rdd.first() #extract header
        rdd = rdd.filter(lambda row: row != header)
        pickup_datetime_ind = 1
        values = rdd.map(lambda x: (matches_date(next(csv.reader([x]))[pickup_datetime_ind])['year_month'], 1)).reduceByKey(add)
        values_list.append(values)

    result = sc.union(values_list)
    result.saveAsTextFile(args.output_path)


if __name__ == "__main__":
    main()
