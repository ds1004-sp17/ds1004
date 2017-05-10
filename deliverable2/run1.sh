#!/bin/bash

cd deliverable2
time spark-submit extract_monthly_stats.py \
  --input 's3://nyc-tlc/trip data/yellow_tripdata_2015-01.csv' \
  --output 's3://cipta-bigdata1004/yellow_extract_2015-01.csv'
