import re
import pandas as pd

lookup = pd.read_csv('taxi_zone_lookup.csv', index_col=None)
unique_location_ids = set(lookup['LocationID'])

def process_location_id(pair):
    value, occurrence = pair
    try:
        intval = int(value)
    except ValueError:
        return (value, 'STRING', 'invalid location id', 'INVALID')
    if intval not in unique_location_ids:
        return (value, 'INTEGER', 'ID for nonexistent location', 'INVALID')
    return (value, 'INTEGER', 'Location ID', 'VALID')
