#!/bin/bash

for month in `seq -f %02g 8 12`; do
  time spark-submit extract_monthly_stats.py \
      --input "s3://nyc-tlc/trip data/green_tripdata_2013-$month.csv" \
      --output "s3://cipta-bigdata1004/green_extract_2013-$month.csv"
done
