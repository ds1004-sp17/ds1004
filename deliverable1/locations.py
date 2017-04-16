import re
import pandas as pd

lookup = pd.read_csv('taxi_zone_lookup.csv', index_col=None)
unique_location_ids = set(lookup['LocationID'])

latlng_bound_min = (40.3573, -74.6672) # Princeton, NJ
latlng_bound_max = (41.3163, -72.9223) # Yale, CT

def process_location_id(pair):
    value, occurrence = pair
    try:
        intval = int(value)
    except ValueError:
        return (value, 'STRING', 'invalid location id', 'INVALID')
    if intval not in unique_location_ids:
        return (value, 'INTEGER', 'ID for nonexistent location', 'INVALID')
    return (value, 'INTEGER', 'Location ID', 'VALID')

def parse_latitude(pair):
    value, occurrence = pair
    try:
        lat = float(value)
    except ValueError:
        return (value, 'STRING', 'invalid latitude', 'INVALID')
    if lat < -90.0 or lat > 90.0:
        return (value, 'FLOAT', 'invalid latitude value.', 'INVALID')
    if lat < latlng_bound_min[0] or lat > latlng_bound_max[0]:
        return (value, 'FLOAT', 'valid outlier latitude.', 'OUTLIER')
    return (value, 'FLOAT', 'valid latitude', 'VALID')

def parse_longitude(pair):
    value, occurrence = pair
    try:
        lng = float(value)
    except ValueError:
        return (value, 'STRING', 'invalid longitude', 'INVALID')
    if lng < -90.0 or lng > 90.0:
        return (value, 'FLOAT', 'invalid longitude value.', 'INVALID')
    if lng < latlng_bound_min[1] or lng > latlng_bound_max[1]:
        return (value, 'FLOAT', 'valid outlier longitude.', 'OUTLIER')
    return (value, 'FLOAT', 'valid longitude', 'VALID')

