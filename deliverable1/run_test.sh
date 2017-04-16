#!/bin/bash

module purge
module load python/gnu/2.7.11
module load pandas/0.18.1

# Print this file out.
cat run_test.sh

hdfs dfs -mkdir taxis_test_results/
hdfs dfs -rm -r -f taxis_test_results/*
hdfs dfs -mkdir taxis_test_results/_tmp/

time spark-submit column_analysis.py \
  --input_dir /user/ch1751/public/taxis_test/ \
  --save_path taxis_test_results/ \
  --tempdir taxis_test_results/_tmp \
  --print_invalid_rows \
  --cache

#hdfs dfs -rm -r -f taxis_results/_tmp/

echo 'Saved files available in:'
hdfs -ls taxis_test_results/
