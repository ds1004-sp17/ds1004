import csv
import argparse
from cStringIO import StringIO
from operator import add

from pyspark import SparkContext

parser = argparse.ArgumentParser(description='PyTorch Quora RNN/LSTM Language Model')
parser.add_argument('--file_path', type=str, default='yellow_tripdata_test.csv',
                    help='location of csv file in HDFS.')
parser.add_argument('--min_partitions', type=int, default=5,
                    help='minimum number of data partitions when loading')
parser.add_argument('--save_path', type=str, default='./',
                    help='directory in HDFS to save files to.')
parser.add_argument('--dump', action='store_true',
                    help='dump contents to terminal instead of saving')
parser.add_argument('--keep_valid_rate', type=float, default=1e-5,
                    help='how many valid values to keep (for debugging).')
args = parser.parse_args()

def combine_csv_files():
    pass

def to_csv(l):
    return ','.join(l)

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
    base_type, semantic_type, data_label = 'FLOAT', 'fare amount (Dollars)', 'VALID'
    if key is None:
        base_type = 'NULL'
        semantic_type = 'missing value'
        data_label = 'NULL'
    else:
        try:
            float(key)
            key = key // 10 * 10
#***TO CHECK
            if (float(key) <= 0.0) or (float(key) >= 100.0):
                semantic_type = 'INVALID|OUTLIER'
        except:
            base_type = 'STRING'
            semantic_type = 'unknown value'
            data_label = 'INVALID'
    # return (key, base_type, semantic_type, data_label, occur_count)
    return (key, occur_count)



def main():
    # Keep as a list because dictionary iteration order is undefined.
    column_dict = [
        ('VendorID', parse_0_vendor),
        ('tpep_pickup_datetime', parse_1_pickup_datetime),
        ('tpep_dropoff_datetime', parse_2_dropoff_datetime),
        # 'passenger_count': parse_3_passenger_count,
        # 'trip_distance': parse_4_trip_distance,
        # 'RatecodeID': parse_5,
        # 'store_and_fwd_flag': parse_6,
        # 'PULocationID': parse_7,
        # 'DOLocationID': parse_8,
        # 'payment_type': parse_9,
        # 'fare_amount': parse_10,
        # 'extra': parse_11,
        # 'mta_tax': parse_12,
        # 'tip_amount': parse_13,
        # 'tolls_amount': parse_14,
        # 'improvement_surcharge': parse_15,
        # 'total_amount': parse_16
    ]

    sc = SparkContext()
    rdd = sc.textFile(args.file_path, minPartitions=args.min_partitions)
    header = rdd.first() #extract header
    rdd = rdd.filter(lambda row: row != header)
    for i, (col, parse_func) in enumerate(column_dict):
        # Get unique values and their counts.
        values = rdd.map(lambda x: (next(csv.reader([x]))[i], 1)).\
                    reduceByKey(add)
        # Feed values RDD to a parser.
        values = values.map(parse_func)
        # For each tuple returned by the parse_func, dup it to a csv
        # defined per column.
        if values is not None:
            values = values.filter(keep_valid).map(to_csv)
            if args.dump:
                for row in values.collect():
                    print(row)
            else:
                values.map(to_csv).saveAsTextFile(
                        args.save_path + '_{}.csv'.format(col))
        

if __name__ == '__main__':
    main()