import csv
import re
import argparse
import random
from cStringIO import StringIO
from operator import add
import datetimes

from pyspark import SparkContext

parser = argparse.ArgumentParser(description='Big Data Taxi Parser')
parser.add_argument('--file_path', type=str, default='yellow_tripdata_test.csv',
                    help='location of csv file in HDFS.')
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
filename_match = file_month_re.search(args.file_path)
expected_year = None
expected_month = None
if filename_match:
    expected_year = int(filename_match.group(1))
    expected_month = int(filename_match.group(2))
    print('='*80 + '\n' + 'YEAR: {0}, MONTH: {1:02d}'.format(
        expected_year, expected_month) + '\n' + '='*80)


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
            if (float(key) <= 0.0) or (float(key) >= 100.0):
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
    return next(csv.reader([x]))

def main():
    # Keep as a list because dictionary iteration order is undefined.
    column_dict = {
        'VendorID': parse_0_vendor,
        # Date time column for 2014.
        'pickup_datetime': parse_1_pickup_datetime,
        'dropoff_datetime': parse_2_dropoff_datetime,
        # Date time column for 2015.
        'tpep_pickup_datetime': parse_1_pickup_datetime,
        'tpep_dropoff_datetime': parse_2_dropoff_datetime,
        # 'passenger_count': parse_3_passenger_count,
        # 'trip_distance': parse_4_trip_distance,
        # 'RatecodeID': parse_5_rate_code,
        # 'store_and_fwd_flag': parse_6_store_and_fwd,
        # 'PULocationID': parse_7,
        # 'DOLocationID': parse_8,
        # 'payment_type': parse_9_payment_type,
        # 'fare_amount': parse_10_fare,
        # 'extra': parse_11,
        # 'mta_tax': parse_12,
        # 'tip_amount': parse_13,
        # 'tolls_amount': parse_14,
        # 'improvement_surcharge': parse_15,
        # 'total_amount': parse_16
    }

    sc = SparkContext()

    # If your code calls out to other python files, add them here.
    sc.addPyFile('datetimes.py')

    rdd = sc.textFile(args.file_path, minPartitions=args.min_partitions)
    header = csv_row_read(rdd.first()) #extract header
    rdd = rdd.filter(lambda row: row != header)
    for i, col in enumerate(header):
        col = col.strip()
        parse_func = column_dict.get(col, None)
        if parse_func is None:
            print('Don\'t know how to read column: {0}'.format(col))
            continue

        # Split into columns.
        all_rows = rdd.map(lambda x: csv_row_read(x))
        # Get rows that have the containing column.
        rows = all_rows.filter(lambda col: len(col) > i)
        # What about rows that don't have enough columns.
        rows_non = all_rows.filter(lambda col: len(col) <= i)

        values = rows.map(lambda row: (row[i], 1)).reduceByKey(add)
        # Feed values RDD to a parser.
        values = values.map(parse_func)
        # For each tuple returned by the parse_func, dup it to a csv
        # defined per column.
        if values is not None:
            values = values.filter(keep_valid).map(to_csv)
            if args.dump:
                print('Column: {0}'.format(col))
                for row in values.collect():
                    print(row)
            else:
                values.saveAsTextFile(
                        args.save_path + '/{}.csv'.format(col))

        # Dump some of the invalid rows.
        rn = rows_non.filter(lambda r: random.random() < args.keep_invalid_rate)
        for row in rn.collect():
            print(row)
        

if __name__ == '__main__':
    main()
