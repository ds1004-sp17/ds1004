from __future__ import print_function

import csv
import re
import argparse
import os
from datetime import date
from cStringIO import StringIO
from pyspark import SparkContext, SparkConf
from shapely.geometry import Point
from rtree import Index
import json
from shapely.geometry import mapping, shape

parser = argparse.ArgumentParser(description='Taxi net traffic.')
parser.add_argument('--input_dir', type=str, default='public/taxis/',
                    help='location of csv files in HDFS.')
parser.add_argument('--taxi_shapes', type=str, default='taxi_zones.geojson',
                    help='location of taxi zone shapes.')
parser.add_argument('--month', type=int, default=7, help='month to process.')
parser.add_argument('--save_path', type=str, default='./monthly_stats/',
                    help='directory in HDFS to save files to.')
parser.add_argument('--loglevel', type=str, default='WARN',
                    help='log verbosity.')
args = parser.parse_args()

taxi_zones = json.load(open(args.taxi_shapes))
class Neighborhood(object):
    def __init__(self, feature):
        self.properties = feature['properties']
        self.geometry = feature['geometry']
        self.shape = shape(feature['geometry'])
taxi_zones_shapes = [Neighborhood(f) for f in taxi_zones['features']]

def csv_row_read(x):
    '''Turns a CSV string (x) into a list of columns.'''
    return next(csv.reader([x]))


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


SATURDAY = 5
def process_pair(columns, time_index, loc_id_index):
    '''Processes either input date/pickup, or output date/pickup'''
    date_str = columns[time_index]
    loc_str = columns[loc_id_index]
    d = datetimes.matches_date(date_str)
    if d is None:
        return None
    try:
        loc = int(loc_str)
    except:
        return None
    if loc < 1 or loc > 263:
        return None
    is_weekend = date(d['year'], d['month'], d['day']).weekday() >= SATURDAY
    return (d['hour'], d['minute'], loc, is_weekend)


min_lon, max_lon = -74.30, -73.65
min_lat, max_lat = 40.45, 40.95
def in_range(lon, lat):
    return lon >= min_lon and lon <= max_lon and \
            lat >= min_lat and lat <= max_lat

taxi_zones = 
index = index.Index()
for i, t in enumerate(taxi_zones_shapes):
    index.insert(i, t.shape.bbox)

def get_location_id(lon, lat):
    lon = float(lon)
    lat = float(lat)
    pt = Point(lon, lat)
    for ic in idx.intersection(pt)
        c = taxi_zones_shapes[ic]
        if c.shape.contains(pt):
            return c.properties['LocationID']

def to_location_ids(header, row):
    '''Converts a (lat, lon) formatted file into location ID.'''
    p_lat_idx = header.indexof('pickup_latitude')
    p_lon_idx = header.indexof('pickup_longitude')
    d_lat_idx = header.indexof('dropoff_latitude')
    d_lon_idx = header.indexof('dropoff_longitude')

    try:
        row.append(get_location_id(row[p_lon_idx], row[p_lat_idx]))
        row.append(get_location_id(row[d_lon_idx], row[d_lat_idx]))
        return row
    except:
        return None

def extract(h, row):
    if row is None:
        return None
    try:
        r = [row[h['pulocationid']],
             row[h['dolocationid']],
             row[h['passenger_count']],
             row[h['trip_distance']],
             row[h['payment_type']],
             row[h['total_amount']]]
        if 'pickup_datetime' in h:
            r.append(row[h['pickup_datetime']])
            r.append(row[h['dropoff_datetime']])
        if 'tpep_pickup_datetime' in h:
            r.append(row[h['tpep_pickup_datetime']])
            r.append(row[h['tpep_dropoff_datetime']])
        if 'lpep_pickup_datetime' in h:
            r.append(row[h['lpep_pickup_datetime']])
            r.append(row[h['lpep_dropoff_datetime']])
    except:
        return None

filepath = \
    os.path.join(
        args.input_dir,
        'yellow_tripdata_2015-{:02d}.csv'.format(args.month))

def main():
    conf = SparkConf().setAppName('extract_monthly_stats')
    sc = SparkContext()
    sc.setLogLevel(args.loglevel)
    sc.addPyFile('datetimes.py')

    print('-'*80 + '\n' + 'net traffic counter' + '\n' + '-'*80)
    print(filepath)
    print('Save to:', args.save_path)

    not_null = lambda x: x is not None
    all_data = sc.textFile(filepath)
    header_line = all_data.first().lower()
    header = {col:idx for idx, col in enumerate(csv_row_read(header_line))}

    # Detect if the location ID is present.
    if 'pulocationid' in header and 'dolocationid' in header:
        all_data = all_data.map(partial(extract, header))
    else:
        all_data = all_data.map(partial(to_location_ids, header))
        header['pulocationid'] = len(header)
        header['dolocationid'] = len(header)
        all_data = all_data.map(partial(extract, header))

    all_data = all_data.filter(not_null)
    all_data.map(to_csv).saveAsTextFile(args.save_path)

if __name__ == '__main__':
    main()

