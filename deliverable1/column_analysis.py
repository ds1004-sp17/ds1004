import csv
import re
import argparse
import os
import random
from cStringIO import StringIO
from operator import add
import datetimes
import locations

from pyspark import SparkContext

parser = argparse.ArgumentParser(description='Big Data Taxi Parser')
parser.add_argument('--input_dir', type=str, default='test_inputs.txt',
                    help='location of csv files in HDFS.')
parser.add_argument('--min_year', type=int, default=2013,
                    help='first year to begin parsing.')
parser.add_argument('--max_year', type=int, default=2016,
                    help='last year to begin parsing.')
parser.add_argument('--min_partitions', type=int, default=5,
                    help='minimum number of data partitions when loading')
parser.add_argument('--save_path', type=str, default='./',
                    help='directory in HDFS to save files to.')
parser.add_argument('--dump', action='store_true',
                    help='dump contents to terminal instead of saving')
parser.add_argument('--keep_valid_rate', type=float, default=1.0,
                    help='how many valid values to keep (for debugging).')
parser.add_argument('--keep_invalid_rate', type=float, default=1.0,
                    help='how many invalid values to keep (for debugging).')
args = parser.parse_args()


# Find out which year/month we're dealing with.
file_month_re = re.compile('(\d\d\d\d)-(\d\d)')
def read_file_path(filepath):
    '''Reads the file path and determines the data range of this file.

    Args:
        filepath: a string (the filename), e.g. yellow_tripdata_2016-09.csv
    Returns:
        expected_year: expected calendar year of records in this file.
        expected_month: expected calendar month of records in this file.
        If no time information can be inferred, both will return None.'''
    filename_match = file_month_re.search(filepath)
    expected_year = None
    expected_month = None
    if filename_match:
        expected_year = int(filename_match.group(1))
        expected_month = int(filename_match.group(2))
        # print('='*80 + '\n' + 'YEAR: {0}, MONTH: {1:02d}'.format(
        #     expected_year, expected_month) + '\n' + '='*80)
    return expected_year, expected_month


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

################################################################################
# Column parser definitions.
#
# For each function, 'x' is a tuple (value, count).
# The expected return is a tuple (value, base_type, semantic_type, validity)
################################################################################

def parse_0_vendor(x):
    key, occur_count = x
    base_type = 'INT'
    semantic_type = 'vendor id'
    data_label = 'VALID'  #VALID | NULL | INVALID
    if key is None:
        base_type = 'NULL'
        semantic_type = 'unknown value'
        data_label = 'NULL'
    else:
        try:
            int(key)
            if key not in ('1', '2'):
                data_label = 'INVALID'
        except:
            base_type = 'STRING'
            semantic_type = 'unknown value'
            data_label = 'INVALID'
    return (key, base_type, semantic_type, data_label, occur_count)


def parse_1_pickup_datetime(x):
    return datetimes.process_pickup(x, expected_year, expected_month)


def parse_2_dropoff_datetime(x):
    return datetimes.process_dropoff(x, expected_year, expected_month)

# 'PULocationID': parse_7,
def parse_pu_location_id(x):
    return locations.process_location_id(x)

# 'DOLocationID': parse_8,
def parse_do_location_id(x):
    return locations.process_location_id(x)

def parse_3_passenger_count(x):
    key, occur_count = x
    base_type, semantic_type, data_label = 'INT', 'passenger count', 'VALID'
    if key is None:
        base_type = 'NULL'
        semantic_type = 'missing value'
        data_label = 'NULL'
    else:
        try:
            int(key)
            if (int(key) > 6) or (int(key) <= 0):
                data_label = 'INVALID|OUTLIER'
        except:
            base_type = 'STRING'
            semantic_type = 'unknown value'
            data_label = 'INVALID'
    return (key, base_type, semantic_type, data_label, occur_count)


def parse_4_trip_distance(x):
    key, occur_count = x
    base_type, semantic_type, data_label = 'FLOAT', 'distance (miles)', 'VALID'
    if key is None:
        base_type = 'NULL'
        semantic_type = 'missing value'
        data_label = 'NULL'
    else:
        try:
            float(key)
#***TO CHECK
            if (float(key) <= 0.0) or (float(key) >= 100.0):
                semantic_type = 'INVALID|OUTLIER'
        except:
            base_type = 'STRING'
            semantic_type = 'unknown value'
            data_label = 'INVALID'
    return (key, base_type, semantic_type, data_label, occur_count)


def parse_5_rate_code(x):
    key, occur_count = x
    base_type, semantic_type, data_label = 'INT', 'rate code id', 'VALID'
    if key is None:
        base_type, semantic_type, data_label = 'NULL', 'missing value', 'NULL'
    else:
        try:
            int(key)
            if key not in ('1','2','3','4','5','6'):
                data_label = 'INVALID'
            elif key == '99':
                data_label = 'NULL'
        except:
            base_type = 'STRING'
            semantic_type = 'unknown value'
            data_label = 'INVALID'
    return (key, base_type, semantic_type, data_label, occur_count)


def parse_6_store_and_fwd(x):
    key, occur_count = x
    base_type, semantic_type, data_label = 'BOOLEAN', 'store and forward flag', 'VALID'
    if key is None:
        base_type, semantic_type, data_label = 'NULL', 'missing value', 'NULL'
    else:
        if key not in ('Y', 'N'):
            data_label, base_type, semantic_type = 'INVALID', 'STRING', 'unknown value'
    return (key, base_type, semantic_type, data_label, occur_count)


def parse_9_payment_type(x):
    key, occur_count = x
    base_type, semantic_type, data_label = 'INT', 'payment_type', 'VALID'
    if key is None:
        base_type, semantic_type, data_label = 'NULL', 'missing value', 'NULL'
    else:
        try:
            int(key)
            if key not in ('1','2','3','4','5','6'):
                data_label = 'INVALID'
        except:
            base_type, semantic_type, data_label = 'STRING', 'unknown value', 'INVALID'
    return (key, base_type, semantic_type, data_label, occur_count)


def parse_10_fare(x):
    key, occur_count = x
    base_type, semantic_type, data_label = 'FLOAT', 'extra charge (dollars)', 'VALID'
    if key is None:
        base_type, semantic_type, data_label = 'NULL', 'missing value', 'NULL'
    else:
        try:
            float(key)
            if (float(key) < 0.0) or (float(key) >= 100.0):
                data_label = 'INVALID|OUTLIER'
        except:
            base_type, semantic_type, data_label = 'STRING', 'unknown value', 'INVALID'
    return (key, base_type, semantic_type, data_label, occur_count)


def parse_11_extra(x):
    key, occur_count = x
    base_type, semantic_type, data_label = 'FLOAT', 'extra charge (dollars)', 'VALID'
    if key is None:
        base_type, semantic_type, data_label = 'NULL', 'missing value', 'NULL'
    else:
        try:
            float(key)
            if float(key) not in (0, 0.5, 1.0, 4.5):
                data_label = 'INVALID|OUTLIER'
        except:
            base_type, semantic_type, data_label = 'STRING', 'unknown value', 'INVALID'
    return (key, base_type, semantic_type, data_label, occur_count)


def parse_12_tax(x):
    key, occur_count = x
    base_type, semantic_type, data_label = 'FLOAT', 'mta tax (dollars)', 'VALID'
    if key is None:
        base_type, semantic_type, data_label = 'NULL', 'missing value', 'NULL'
    else:
        try:
            float(key)
            if float(key) not in (0.0, 0.5):
                data_label = 'INVALID|OUTLIER'
        except:
            base_type, semantic_type, data_label = 'STRING', 'unknown value', 'INVALID'
    return (key, base_type, semantic_type, data_label, occur_count)


def parse_15_improvement(x):
    key, occur_count = x
    base_type, semantic_type, data_label = 'FLOAT', 'improvement surcharge (dollars)', 'VALID'
    if key is None:
        base_type, semantic_type, data_label = 'NULL', 'missing value', 'NULL'
    else:
        try:
            float(key)
            if float(key) not in (0.0, 0.3):
                data_label = 'INVALID|OUTLIER'
        except:
            base_type, semantic_type, data_label = 'STRING', 'unknown value', 'INVALID'
    return (key, base_type, semantic_type, data_label, occur_count)


def parse_13_tip(x):
    key, occur_count = x
    base_type, semantic_type, data_label = 'FLOAT', 'tip (dollars)', 'VALID'
    if key is None:
        base_type, semantic_type, data_label = 'NULL', 'missing value', 'NULL'
    else:
        try:
            float(key)
#*** TO CHECK
            if (float(key) < 0.0) or (float(key) > 200.0):
                data_label = 'INVALID|OUTLIER'
        except:
            base_type, semantic_type, data_label = 'STRING', 'unknown value', 'INVALID'
    return (key, base_type, semantic_type, data_label, occur_count)


def parse_14_tolls(x):
    key, occur_count = x
    base_type, semantic_type, data_label = 'FLOAT', 'tolls (dollars)', 'VALID'
    if key is None:
        base_type, semantic_type, data_label = 'NULL', 'missing value', 'NULL'
    else:
        try:
            float(key)
#*** TO CHECK
            if (float(key) < 0.0) or (float(key) > 100.0):
                data_label = 'INVALID|OUTLIER'
        except:
            base_type, semantic_type, data_label = 'STRING', 'unknown value', 'INVALID'
    return (key, base_type, semantic_type, data_label, occur_count)


def parse_16_total(x):
    key, occur_count = x
    base_type, semantic_type, data_label = 'FLOAT', 'total (dollars)', 'VALID'
    if key is None:
        base_type, semantic_type, data_label = 'NULL', 'missing value', 'NULL'
    else:
        try:
            float(key)
#*** TO CHECK
            if (float(key) < 0.0) or (float(key) > 200.0):
                data_label = 'INVALID|OUTLIER'
        except:
            base_type, semantic_type, data_label = 'STRING', 'unknown value', 'INVALID'
    return (key, base_type, semantic_type, data_label, occur_count)



################################################################################

def keep_valid(row):
    '''For a given data row, choose whether to print out the data. Args:
        row: A tuple (value, base_type, semantic_type, label, count)'''
    try:
        # Row: (value, base_type, semantic_type, valid_invalid)
        if row[3] == 'VALID' and random.random() > args.keep_valid_rate:
            return False
        if row[3] == 'INVALID' and random.random() > args.keep_invalid_rate:
            return False
        return True
    except:
        return True

def csv_row_read(x):
    '''Turns a CSV string (x) into a list of columns.'''
    return next(csv.reader([x]))


################################################################################

def process_one_file(filepath):
    '''Breaks a file into columns and does value tagging on each column.

    Args:
        filepath: string, where to get the file
    Returns:
        a list of CSV line RDDs, one for each column. They can either be
        dumped or saved to a file.

        Each column emits a tuple: (colname, RDD, invalid_rows) where the
        second value are rows that don't contain this column.
    '''
    # This helps date column validation.
    expected_year, expected_month = read_file_path(filepath)

    # Common columns for all years.
    column_dict = {
        'VendorID': parse_0_vendor,
        'passenger_count': parse_3_passenger_count,
        'trip_distance': parse_4_trip_distance,
        'RatecodeID': parse_5_rate_code,
        'store_and_fwd_flag': parse_6_store_and_fwd,
        'payment_type': parse_9_payment_type,
        'fare_amount': parse_10_fare,
        'extra': parse_11,
        'mta_tax': parse_12,
        'tip_amount': parse_13,
        'tolls_amount': parse_14,
        'improvement_surcharge': parse_15,
        'total_amount': parse_16
    }

    # Date column changes from 2015.
    if expected_year < 2015:
        column_dict['pickup_datetime'] =
            pickup_datetime_parser(expected_year, expected_month)
        column_dict['dropoff_datetime'] =
            pickup_datetime_parser(expected_year, expected_month)
    else:
        column_dict['tpep_pickup_datetime'] =
            pickup_datetime_parser(expected_year, expected_month)
        column_dict['tpep_dropoff_datetime'] =
            pickup_datetime_parser(expected_year, expected_month)

    # Locations are different before/after 2016.
    if expected_year < 2016:
        column_dict['pickup_longitude'] = locations.parse_longitude
        column_dict['pickup_latitude'] = locations.parse_latitude
        column_dict['dropoff_longitude'] = locations.parse_longitude
        column_dict['dropoff_latitude'] = locations.parse_latitude
    else:
        column_dict['PULocationId'] = parse_pu_location_id
        column_dict['DOLocationId'] = parse_do_location_id

    # Load the text file and split out the header.
    rdd = sc.textFile(file_path, minPartitions=args.min_partitions)
    header_line = rdd.first()
    header = csv_row_read(header_line)
    # Filter empty lines and the header.
    rdd = rdd.filter(lambda row: len(row) > 0 and row != header_line)

    # Split each row into columns.
    all_rows = rdd.map(lambda x: csv_row_read(x))

    # Split off each column and analyze.
    column_results = []
    for i, col in enumerate(header):
        col = col.strip()
        parse_func = column_dict.get(col, None)
        if parse_func is None:
            print('{0}: Don\'t know how to read column: {1}'.format(
                filepath, col))
            continue

        # Get rows that have the containing column.
        rows = all_rows.filter(lambda col: len(col) > i)
        rows_missing_this_col = all_rows.filter(lambda col: len(col) <= i)
        # Get all unique values and analyze.
        values = rows.map(lambda row: (row[i], 1)).reduceByKey(add)
        values = values.map(parse_func)

        column_results.append((col, values, rows_missing_this_col))
    return column_results

################################################################################

def main():
    sc = SparkContext()

    # If your code calls out to other python files, add them here.
    sc.addPyFile('datetimes.py')
    sc.addPyFile('locations.py')
    sc.addFile('taxi_zone_lookup.csv')

    filename_format = 'yellow_tripdata_{0}-{1:02d}.csv'
    for year in range(args.min_year, args.max_year + 1):
        for month in range(1, 13):
            filename = filename_format.format(year, month)
            filepath = os.path.join(args.input_dir, filename)
            print('Getting:', filepath)

        # Feed values RDD to a parser.
        # For each tuple returned by the parse_func, dup it to a csv
        # defined per column.
        # if values is not None:
        #     values = values.filter(keep_valid).map(to_csv)
        #     if args.dump:
        #         print('Column: {0}'.format(col))
        #         for row in values.collect():
        #             print(row)
        #     else:
        #         values.saveAsTextFile(
        #                 args.save_path + '/{}.csv'.format(col))
        #
        # # Dump some of the invalid rows.
        # for row in rows_non.collect():
        #     print(row)
        

if __name__ == '__main__':
    main()
