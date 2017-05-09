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

## Summarizer

This script looks at the results from the previous program and outputs two
aggregated files. One output is aggregated by date, and one by time of day.
Essentially they're the results of two group by operations:

### Daily file
```
SELECT locationid, event, year, month, day,
       SUM(passengers_count), SUM(distance), SUM(total_amount), COUNT(*)
FROM trips
GROUP BY locationid, event, year, month, day
```
Each `event` is either `PU` for pickup or `DO` for dropoff. Each row contains
data like this:
1. How many passengers arrive in Chelsea on 2013-09-24?
1. How many trips start from Financial District on 2013-01-10?

Example:
```
22,PU,2013,9,9,1,0.0,3.5,1
22,PU,2013,9,10,3,2.87,15.0,2
22,PU,2013,9,11,1,8.94,27.0,1
22,PU,2013,9,12,18,15.16,76.8,7
22,PU,2013,9,13,11,37.239999999999995,140.63,6
22,PU,2013,9,14,11,44.2,224.16,7
```
       
