#!/bin/bash

time spark-submit extract_monthly_stats.py \
  --input "s3://nyc-tlc/trip data/green_tripdata_2014-01.csv" \
  --output "s3://cipta-bigdata1004/green_extract_2014-01.csv" &
time spark-submit extract_monthly_stats.py \
  --input "s3://nyc-tlc/trip data/green_tripdata_2014-02.csv" \
  --output "s3://cipta-bigdata1004/green_extract_2014-02.csv" &
time spark-submit extract_monthly_stats.py \
  --input "s3://nyc-tlc/trip data/green_tripdata_2014-06.csv" \
  --output "s3://cipta-bigdata1004/green_extract_2014-06.csv" &
time spark-submit extract_monthly_stats.py \
  --input "s3://nyc-tlc/trip data/green_tripdata_2014-09.csv" \
  --output "s3://cipta-bigdata1004/green_extract_2014-09.csv" &
