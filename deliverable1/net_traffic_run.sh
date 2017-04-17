#!/bin/sh

module purge
module load python/gnu/2.7.11

# Print this file out.
cat net_traffic_run.sh

hdfs dfs -rm -r -f net_traffic.csv

time spark-submit net_traffic.py \
  --input_dir /user/ch1751/public/taxis/ \
  --save_path net_traffic.csv

hdfs dfs -getmerge net_traffic.csv net_traffic.csv
