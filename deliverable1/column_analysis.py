import csv
from operator import add

from pyspark import SparkContext


file_path = 'yellow_tripdata_test.csv'
min_partitions = 20
save_path = ''


def combine_csv_files():
    pass

def to_csv(l):
    return ','.join(l)

def parse_0_vendor(x):
    pass

def parse_3_passenger_count(x):
    pass

def parse_4_trip_distance(x):
    pass


def main():
    column_dict = {'VendorID': parse_0_vendor,
                    # 'tpep_pickup_datetime': parse_1,
                    # 'tpep_dropoff_datetime': parse_2,
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
                    }

    sc = SparkContext()
    rdd = sc.textFile(file_path, minPartitions = min_partitions)
    for i, (col, parse_func) in enumerate(column_dict.iteritems()):
        values = rdd.map(lambda x: (next(csv.reader([x]))[i], 1)).\
                    reduceByKey(add)
        values = values.map(parse_func)
        values.map(to_csv).saveAsTextFile(save_path + '_{}.csv'.format(col))

        

if __name__ == '__main__':
    main()