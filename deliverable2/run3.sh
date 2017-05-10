#!/bin/bash

time spark-submit extract_monthly_stats.py \
  --input 's3://nyc-tlc/trip data/yellow_tripdata_2015-04.csv' \
  --output 'yellow_extract_2015-04.csv'
