import csv
# import argparse
import re
import os

from pyspark import SparkContext
from pyspark.sql import SQLContext
# from operator import add


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
        # 'year_month': str(match.group(1)) + "/" + str(match.group(2))
    }
    return date

def map_taxi_date(x):
    if not x:
        return (-1, -1)
    elif not isinstance(x, str):
        return (-1, -1)
    d = ''.join(x[:10].split('-'))
    t = ''.join(x[11:].split(':'))
    try:
        t = int(t)
    except:
        t = -1
    return [d, t]

def clean_weather(x):
    clean = [x[2]]
    for v in x[3:]:
        clean.append(float(v))
    return clean

def csv_row_read(x):
    return next(csv.reader([x]))


def main():
    # parser = argparse.ArgumentParser(description='Parser for time-series of trips')
    # parser.add_argument('--filepaths_file', type=str, required=True)
    # parser.add_argument('--min_partitions', type=int, default=20,
    #                     help='minimum number of data partitions when loading')
    # parser.add_argument('--output_path', type=str, required=True,
    #                     help='path for output file')
    # args = parser.parse_args()

    sc = SparkContext()
    sqlContext = SQLContext(sc)

    input_dir = 's3://cipta-bigdata1004/'
    filepaths = [
        os.path.join(input_dir,
            'yellow_extract_{}-{:02d}.csv'.format(year, month))
        for year in range(2013, 2017)
        for month in range(1, 13)
    ]

    weather_data = sc.textFile('central_park_weather.csv')
    weather_header = weather_data.first()
    weather_data = weather_data.filter(lambda row: row != weather_header)
    weather_data = weather_data.map(lambda x: csv_row_read(x))
    w_header = [x.lower().strip() for x in csv_row_read(weather_header)][2:]
    weather_data = weather_data.map(lambda x: clean_weather(x))
    weather_df = sqlContext.createDataFrame(weather_data, w_header)

    all_rows = sc.union([sc.textFile(x) for x in filepaths])
    all_rows = all_rows.map(lambda x: csv_row_read(x))
    all_rows = all_rows.map(lambda x: x[0:6] + map_taxi_date(x[6]))

    t_header = ['pulocationid', 'dolocationid', 'passenger_count', 'trip_distance', 'payment_type', 'total_amount', 'pickup_date', 'pickup_time']
    taxi_data = sqlContext.createDataFrame(all_rows, t_header)
        
    joined_df = taxi_data.join(weather_df, weather_df.date==taxi_data.pickup_date)
    joined_df.registerTempTable("joined")

    # output_1 = sqlContext.sql('''SELECT FLOOR(tmin/2) * 2 as tmin_floor, COUNT(DISTINCT pickup_date) as total_days, 
    output_1 = sqlContext.sql('''SELECT tmin, COUNT(DISTINCT pickup_date) as total_days, 
                            COUNT(trip_distance) as count, 
                            AVG(trip_distance) as avg_dist
                            FROM joined 
                            WHERE (pickup_time < 040000 
                                OR pickup_time > 220000)
                                AND (trip_distance < 200)
                            GROUP BY tmin
                            ORDER BY tmin DESC
                            ''')
    output_1.write.csv('min_temp.csv')

    output_2 = sqlContext.sql('''SELECT tmax, 
                                COUNT(DISTINCT pickup_date) as total_days, 
                                COUNT(trip_distance) as count, 
                                AVG(trip_distance) as avg_dist
                            FROM joined 
                            WHERE pickup_time > 110000 
                                AND pickup_time < 150000
                            GROUP BY tmax
                            ORDER BY tmax DESC
                            ''')
    output_2.write.csv('max_temp.csv')

    output_3 = sqlContext.sql('''SELECT snow_inc, 
                                COUNT(DISTINCT pickup_date) as total_days, 
                                COUNT(trip_distance) as count, 
                                AVG(trip_distance) as avg_dist
                            FROM joined 
                            GROUP BY snow_inc
                            ORDER BY snow_inc DESC
                            ''')
    output_3.write.csv('snowfall.csv')

    output_4 = sqlContext.sql('''SELECT prcp, 
                                COUNT(DISTINCT pickup_date) as total_days, 
                                COUNT(trip_distance) as count, 
                                AVG(trip_distance) as avg_dist
                            FROM joined 
                            GROUP BY prcp
                            ORDER BY prcp DESC
                            ''')
    output_4.write.csv('precipitation.csv')

    output_5 = sqlContext.sql('''SELECT date, prcp, snwd, snow, tmax, tmin,
                                COUNT(trip_distance) as trip_count, 
                                AVG(trip_distance) as avg_dist
                            FROM joined 
                            GROUP BY date, prcp, snwd, snow, tmax, tmin
                            ORDER BY trip_count
                            LIMIT 1000
                            ''')
    output_5.write.csv('lowest_count_trip.csv')

if __name__ == '__main__':
    main()










