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
    column_dict = {'VendorID': parse_0_vendor,
                    # 'tpep_pickup_datetime': parse_1,
                    # 'tpep_dropoff_datetime': parse_2,
                    # 'passenger_count': parse_3_passenger_count,
                    # 'trip_distance': parse_4_trip_distance,
                    # 'RatecodeID': parse_5_rate_code,
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
    header = rdd.first() #extract header
    rdd = rdd.filter(lambda row: row != header)
    for i, (col, parse_func) in enumerate(column_dict.iteritems()):
        values = rdd.map(lambda x: (next(csv.reader([x]))[i], 1)).\
                    reduceByKey(add)
        values = values.map(parse_func)
        values.map(to_csv).saveAsTextFile(save_path + '_{}.csv'.format(col))



if __name__ == '__main__':
    main()