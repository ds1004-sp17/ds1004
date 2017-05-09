# Deliverable 2

Here we describe the programs we use to analyze data for Deliverable 2.

## Monthly Column Extractor

This script works on a single taxi trip data month from TLC, and outputs one file.
It standardizes the columns over time. Each row is a taxi trip, just like the
original dataset.

The output file columns are:
1. `pulocationid` - Location ID of the pickup. We convert lat/long information from
   previous years to location IDs so we can analyze all the data.
1. `dolocationid` - Location ID of the dropoff.
1. `passenger_count`
1. `trip_distance`
1. `payment_type`
1. `total_amount` - from the `Total_amount` column in the original file.
1. `pickup_datetime`
1. `dropoff_datetime`

Example:
```
113,246,2,1.8999999999999999,CSH,13,2014-05-21 17:33:48,2014-05-21 17:48:39
151,238,1,0.59999999999999998,CSH,6.5,2014-05-21 18:42:17,2014-05-21 18:46:47
90,233,1,2,CSH,12.5,2014-05-21 20:06:07,2014-05-21 20:21:36
233,263,1,2,CSH,9,2014-05-21 22:32:07,2014-05-21 22:39:23
90,68,3,0.5,CSH,4.5,2014-05-21 21:01:00,2014-05-21 21:03:02
116,247,1,1.8,CSH,10.5,2014-05-22 00:50:23,2014-05-22 01:00:28
148,48,1,3.6000000000000001,CSH,14.5,2014-05-22 03:40:06,2014-05-22 03:55:46
127,239,3,7,CSH,21.5,2014-05-21 21:57:18,2014-05-21 22:08:03
```
