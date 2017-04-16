from __future__ import print_function

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
parser.add_argument('--input_dir', type=str, default='public/taxis_test',
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
parser.add_argument('--columns', type=str, default=None,
                    help='what columns to run analysis for (default all)')
parser.add_argument('--print_invalid_rows', action='store_true',
                    help='for each column, print some rows where the column '
                    'is missing.')
parser.add_argument('--loglevel', type=str, default='WARN',
                    help='log verbosity.')
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

# These date parsers can actually check the date against the file name.
# However the output format that we have (keyed by value) doesn't allow
# us to take advantage of this. For example, 2015-02-01 can be valid or invalid
# depending on whether it's in the February or July file.

def parse_1_pickup_datetime(x):
    return datetimes.process_pickup(x, None, None)

def parse_2_dropoff_datetime(x):
    return datetimes.process_dropoff(x, None, None)

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

def drop_values(row):
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

def process_one_file(sc, filepath, whitelist_columns=None):
    '''Breaks a file into columns.

    Args:
        filepath: string, where to get the file
        whitelist_columns: only analyze these columns.
    Returns:
        a list of CSV line RDDs, one for each column.

        Each column emits a tuple: (colname, RDD, invalid_rows) where the
        second value are rows that don't contain this column.
    '''
    # This helps date column validation.
    expected_year, expected_month = read_file_path(filepath)

    # Load the text file and split out the header.
    rdd = sc.textFile(filepath, minPartitions=args.min_partitions)
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
        if whitelist_columns and col not in whitelist_columns:
            continue

        # Get rows that have the containing column.
        rows = all_rows.filter(lambda col: len(col) > i)
        rows_missing_this_col = all_rows\
                .filter(lambda col: len(col) <= i)\
                .map(to_csv)\
                .map(lambda line: filepath + ':' + line)
                # Tag the invalid row so we know which file it's from.

        # Get all unique values and analyze.
        col_id = i # Important to establish a closure.
        values = rows.map(lambda row: row[i])

        column_results.append((col, values, rows_missing_this_col))
    return column_results

################################################################################

def main():
    sc = SparkContext()
    sc.setLogLevel(args.loglevel)

    print('='*80 + '\n' + 'BIG DATA TAXIS PARSER' + '\n' + '='*80)

    if args.dump and args.keep_valid_rate > 0.1:
        warn_msg = '''
WARNING WARNING WARNING

Option --dump will print file contents to the terminal.
Setting keep rate to a high value ({0}) may cause overload.
This parser will dump the first 1k rows per column.

WARNING WARNING WARNING
'''
        print(warn_msg.format(args.keep_valid_rate))

    if not args.dump and (args.keep_valid_rate < 1.0 or \
            args.keep_invalid_rate < 1.0):
        warn_msg = '''
WARNING WARNING WARNING

Options --keep_valid/invalid_rates are set without --dump.
These options are usually for controlling the stuff that gets
printed to terminal. You probably want to set this at 1?

WARNING WARNING WARNING
'''
        print(warn_msg.format(args.keep_valid_rate))

    # If your code calls out to other python files, add them here.
    sc.addPyFile('datetimes.py')
    sc.addPyFile('locations.py')
    sc.addFile('taxi_zone_lookup.csv')

    # All the possible columns. Some years may only have a subset of columns.
    column_dict = {
        'vendor_id': parse_0_vendor, # 2013-2014 formatting.
        'VendorID': parse_0_vendor,
        # Date time column for 2014.
        'pickup_datetime': parse_1_pickup_datetime,
        'dropoff_datetime': parse_2_dropoff_datetime,
        # Date time column for 2015-2016.
        'tpep_pickup_datetime': parse_1_pickup_datetime,
        'tpep_dropoff_datetime': parse_2_dropoff_datetime,
        # Locations.
        'PULocationID': parse_pu_location_id,
        'DOLocationID': parse_do_location_id,
        # Locations (pre-2016).
        'pickup_longitude': locations.parse_longitude,
        'pickup_latitude': locations.parse_latitude,
        'dropoff_longitude': locations.parse_longitude,
        'dropoff_latitude': locations.parse_latitude,
        # More fields.
        'passenger_count': parse_3_passenger_count,
        'trip_distance': parse_4_trip_distance,
        'RatecodeID': parse_5_rate_code,
        'store_and_fwd_flag': parse_6_store_and_fwd,
        'payment_type': parse_9_payment_type,
        'fare_amount': parse_10_fare,
        'extra': parse_11_extra,
        'mta_tax': parse_12_tax,
        'tip_amount': parse_13_tip,
        'tolls_amount': parse_14_tolls,
        'improvement_surcharge': parse_15_improvement,
        'total_amount': parse_16_total
    }

    user_columns = set(column_dict.keys())
    if args.columns:
        print('Only doing columns:', args.columns)
        user_columns = set(args.columns.split(','))

    filename_format = 'yellow_tripdata_{0}-{1:02d}.csv'
    column_values = {}
    invalid_rows = {}
    for col in user_columns:
        column_values[col] = []
        invalid_rows[col] = []

    # For each year and each month, read in columns.
    for year in range(args.min_year, args.max_year + 1):
        for month in range(1, 13):
            filename = filename_format.format(year, month)
            filepath = os.path.join(args.input_dir, filename)
            print('Getting:', filepath)

            columns = process_one_file(sc, filepath, user_columns)
            for col, values, missing_rows in columns:
                if col not in column_values:
                    print('{0}: Unknown column: {1}'.format(filename, col))
                    continue
                column_values[col].append(values)
                invalid_rows[col].append(missing_rows)

    # After collecting columns, parse them all.
    for col, values in column_values.items():
        print('----- Analyzing Column: {0} [{1}] -----'.format(
            col, len(values)))

        if col not in user_columns or len(values) == 0:
            continue
        all_values = sc.union(values) # All values from all items.
        all_invalids = sc.union(invalid_rows[col]) # All invalid rows.
        unique_values = all_values.map(lambda row: (row, 1)).reduceByKey(add)
        parsed_values = unique_values.map(column_dict[col])

        # For each tuple returned by the parse_func, dup it to a csv
        # defined per column.
        parsed_values = parsed_values.filter(drop_values).map(to_csv)
        if args.dump:
            print('Some tagged rows:')
            for row in parsed_values.take(1000):
                print(row)
        else:
            values.saveAsTextFile(args.save_path + '/{}.csv'.format(col))

        # Dump some of the invalid rows.
        if args.print_invalid_rows:
            print('Here are some of the invalid rows:')
            for row in all_invalids.take(100):
                print(row)
        

if __name__ == '__main__':
    main()
