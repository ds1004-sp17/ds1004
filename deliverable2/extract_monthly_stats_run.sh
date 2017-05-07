#!/bin/sh

module purge
module load python/gnu/2.7.11

# Print this file out.
cat $0

hdfs dfs -rm -r -f results.csv

time spark-submit extract_monthly_stats.py
