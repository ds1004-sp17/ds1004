#!/bin/bash

#### THIS RUNS ON THE ENTIRE TAXIS DATASET ####

module purge
module load python/gnu/2.7.11
module load pandas/0.18.1

# Print this file out.
cat run_test.sh

hdfs dfs -mkdir taxis_results/
hdfs dfs -rm -r -f taxis_results/*

time spark-submit column_analysis.py \
  --input_dir /user/ch1751/public/taxis/ \
  --save_path taxis_results/ \
  --print_invalid_rows \
  --min_partitions 10 \
  --cache

echo 'Saved files available in:'
hdfs -ls taxis_results/

#### THIS RUNS ON THE ENTIRE TAXIS DATASET ####
