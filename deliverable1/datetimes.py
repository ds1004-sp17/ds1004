from __future__ import print_function

"""Process date columns.

This script takes the NYC TripData file for one month.

Then it computes statistics:
    - Unique year, month, day, down to seconds (INPUT.year.csv, etc.)
    - List of strings that don't look like dates (INPUT.nondate.csv, etc.)
    Each output is comma-separated with counts of rows that follow the criteria.
"""

import sys
import re
import csv
import random
from operator import add

date_str = re.compile('(\d\d\d\d)-(\d\d)-(\d\d) (\d\d):(\d\d):(\d\d)')


def matches_date(string):
    '''Detects if a string is a date, and extract the date values.

    Output: 
        {year: '2015', month: '03', ...}, or
        None, if the regex failed to match.'''

    # Sample date: 2015-01-15 19:23:42
    match = date_str.match(string)

    if not match:
        return None

    date = {
        'year': int(match.group(1)),
        'month': int(match.group(2)),
        'day': int(match.group(3)),
        'hour': int(match.group(4)),
        'minute': int(match.group(5)),
        'second': int(match.group(6))
    }
    return date

def _process(pair):
    '''Processes a date string.
    Args:
        pair: (date_string, occurrence_count)
    Returns:
        row: a list/tuple of values to be csv'ed. Columns:
             (value, base_type, semantic_data_type, label)'''

    date_string, occurrence_count = pair
    if date_string is None:
        return (None, 'NULL', 'missing value', 'INVALID'), \
                occurrence_count, None

    date = matches_date(date_string)
    if not date:
        return (date_string, 'STRING', 'unknown value', 'INVALID'), \
                occurrence_count, None

    return (date_string, 'STRING', 'date and time value', 'VALID'), \
            occurrence_count, date

def process_pickup(pair, expected_year, expected_month):
    '''Process pickup dates.
    Args:
        pair: (date_string, occurrence_count)
        expected_year: int, the year the date row is for
        expected_month: int, the month the date row is for
    Returns:
        row: Tuple of values in the correct format.'''

    row, occurrence, date = _process(pair)
    if not date:
        return row
    (date_string, base_type, semantic_type, validity) = row
    semantic_type = 'valid pickup date'
    # Check if value is within desired date range.
    # Pickup dates should be strictly inside the date range.
    if expected_year is not None and expected_month is not None:
        if date['year'] != expected_year or date['month'] != expected_month:
            semantic_type = 'pickup date (out of range for file)'
            validity = 'INVALID'
    return (date_string, base_type, semantic_type, validity)


def process_dropoff(pair, expected_year, expected_month):
    '''Process dropoff dates. Dropoffs can go past the current month.
    Args:
        pair: (date_string, occurrence_count)
        expected_year: int, the year the date row is for
        expected_month: int, the month the date row is for
    Returns:
        row: Tuple of values in the correct format.'''

    row, occurrence, date = _process(pair)
    if not date:
        return row
    (date_string, base_type, semantic_type, validity) = row
    # Check if value is within desired date range.
    # Pickup dates should be strictly inside the date range.
    semantic_type = 'valid dropoff date'
    if expected_year is not None and expected_month is not None:
        year, month = date['year'], date['month']
        if year == expected_year + 1 and month == 1 and expected_month == 12:
            # Taxi trips that cross midnight january 1st. Happy new year!
            semantic_type = 'dropoff date (crosses year boundary)'
            validity = 'OUTLIER'
        elif year == expected_year and month == expected_month + 1:
            # Taxi trips that cross months (March 31 -> April 1)
            semantic_type = 'dropoff date (crosses month boundary)'
            validity = 'OUTLIER'
        elif date['year'] != expected_year or date['month'] != expected_month:
            # Any other date, we deem it invalid.
            semantic_type = 'dropoff date (out of range for file)'
            validity = 'INVALID'
    return (date_string, base_type, semantic_type, validity)

#def process_dropoff(pair):


#def process_dropoff(pair):

