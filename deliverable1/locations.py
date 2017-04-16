import re
import pandas as pd

lookup = pd.read_csv('taxi_zone_lookup.csv', index_col=None)
unique_location_ids = set(lookup['LocationID'])

latlng_bound_min = (40.3573, -74.6672) # Princeton, NJ
latlng_bound_max = (41.3163, -72.9223) # Yale, CT

def process_location_id(pair):
    value, occ = pair
    try:
        intval = int(value)
    except ValueError:
        return (value, 'STRING', 'invalid location id', 'INVALID', occ)
    if intval not in unique_location_ids:
        return (value, 'INTEGER', 'ID for nonexistent location', 'INVALID', occ)
    return (value, 'INTEGER', 'Location ID', 'VALID', occ)

def parse_latitude(pair):
    value, occ = pair
    try:
        lat = float(value)
    except ValueError:
        return (value, 'STRING', 'invalid latitude', 'INVALID', occ)
    if lat < -90.0 or lat > 90.0:
        return (value, 'FLOAT', 'invalid latitude value.', 'INVALID', occ)
    if lat < latlng_bound_min[0] or lat > latlng_bound_max[0]:
        return (value, 'FLOAT', 'valid outlier latitude.', 'OUTLIER', occ)
    return (value, 'FLOAT', 'valid latitude', 'VALID', occ)

def parse_longitude(pair):
    value, occ = pair
    try:
        lng = float(value)
    except ValueError:
        return (value, 'STRING', 'invalid longitude', 'INVALID', occ)
    if lng < -90.0 or lng > 90.0:
        return (value, 'FLOAT', 'invalid longitude value.', 'INVALID', occ)
    if lng < latlng_bound_min[1] or lng > latlng_bound_max[1]:
        return (value, 'FLOAT', 'valid outlier longitude.', 'OUTLIER' occ)
    return (value, 'FLOAT', 'valid longitude', 'VALID', occ)

