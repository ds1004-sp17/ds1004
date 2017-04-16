#!/bin/bash

module purge
module load python/gnu/2.7.11
module load pandas/0.18.1

# Print this file out.
cat run_test.sh

hdfs dfs -rm -r -f taxis_test_results/*

time spark-submit column_analysis.py \
  --input_dir /user/ch1751/public/taxis_test/ \
  --save_path taxis_test_results/ \
  --keep_invalid_rate 0.1 \
  --dump
