import csv
from cStringIO import StringIO
from operator import add

from pyspark import SparkContext

parser = argparse.ArgumentParser(description='PyTorch Quora RNN/LSTM Language Model')
parser.add_argument('--file_path', type=str, default='yellow_tripdata_test.csv',
                    help='location of csv file in HDFS.')
parser.add_argument('--min_partitions', type=int, default=20,
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
    pass

def parse_1_pickup_datetime(x):
    return (x, 'DATETIME', 'Pickup date/time in seconds', 'VALID')

def parse_2_dropoff_datetime(x):
    pass

def parse_3_passenger_count(x):
    pass

def parse_4_trip_distance(x):
    pass

################################################################################

def keep_valid(row):
    # Row: (value, base_type, semantic_type, valid_invalid)
    if row[3] == 'VALID' and random.random() > args.keep_valid_rate:
        return False
    return True


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
    for i, (col, parse_func) in enumerate(column_dict.iteritems()):
        # Get unique values and their counts.
        values = rdd.map(lambda x: (next(csv.reader([x]))[i], 1)).\
                    reduceByKey(add)
        # Feed values RDD to a parser.
        values = values.map(parse_func)
        # For each tuple returned by the parse_func, dup it to a csv
        # defined per column.
        if values is not None:
            values = values.filter(keep_valid).map(to_csv)
            if dump:
                for row in values.collect():
                    print(row)
            else:
                values.map(to_csv).saveAsTextFile(
                        args.save_path + '_{}.csv'.format(col))
        

if __name__ == '__main__':
    main()
