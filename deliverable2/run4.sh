#!/bin/bash

time spark-submit extract_monthly_stats.py \
  --input 's3://nyc-tlc/trip data/yellow_tripdata_2015-04.csv' \
  --output 's3://cipta-bigdata1004/yellow_extract_2015-04.csv'
time spark-submit extract_monthly_stats.py \
  --input 's3://nyc-tlc/trip data/yellow_tripdata_2015-05.csv' \
  --output 's3://cipta-bigdata1004/yellow_extract_2015-05.csv'
time spark-submit extract_monthly_stats.py \
  --input 's3://nyc-tlc/trip data/yellow_tripdata_2015-06.csv' \
  --output 's3://cipta-bigdata1004/yellow_extract_2015-06.csv'
time spark-submit extract_monthly_stats.py \
  --input 's3://nyc-tlc/trip data/yellow_tripdata_2015-07.csv' \
  --output 's3://cipta-bigdata1004/yellow_extract_2015-07.csv'
time spark-submit extract_monthly_stats.py \
  --input 's3://nyc-tlc/trip data/yellow_tripdata_2015-08.csv' \
  --output 's3://cipta-bigdata1004/yellow_extract_2015-08.csv'
time spark-submit extract_monthly_stats.py \
  --input 's3://nyc-tlc/trip data/yellow_tripdata_2015-09.csv' \
  --output 's3://cipta-bigdata1004/yellow_extract_2015-09.csv'
time spark-submit extract_monthly_stats.py \
  --input 's3://nyc-tlc/trip data/yellow_tripdata_2015-11.csv' \
  --output 's3://cipta-bigdata1004/yellow_extract_2015-11.csv'
time spark-submit extract_monthly_stats.py \
  --input 's3://nyc-tlc/trip data/yellow_tripdata_2015-12.csv' \
  --output 's3://cipta-bigdata1004/yellow_extract_2015-12.csv'
