#!/bin/sh

module purge
module load python/gnu/2.7.11

# Print this file out.
cat $0

hdfs dfs -rm -r -f ./monthly_stats_test/
hdfs dfs -mkdir ./monthly_stats_test/

time spark-submit extract_monthly_stats.py \
  --month 10 \
  --input_dir /user/ch1751/public/taxis_test/ \
  --save_path ./monthly_stats_test/
