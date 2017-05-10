#!/bin/bash

#### THIS RUNS ON THE ENTIRE **GREEN** TAXIS DATASET ####

OUTDIR=green_taxis_results

module purge
module load python/gnu/2.7.11
module load pandas/0.18.1

# Print this file out.
cat run_green_real.sh

hdfs dfs -mkdir $OUTDIR/
hdfs dfs -rm -r -f $OUTDIR/*
hdfs dfs -mkdir $OUTDIR/_tmp/

time spark-submit column_analysis.py \
  --input_dir /user/ch1751/public/green_taxis/ \
  --save_path $OUTDIR \
  --tempdir $OUTDIR/_tmp \
  --min_partitions 10 \
  --taxi_type green

hdfs dfs -rm -r -f $OUTDIR/_tmp/

echo 'Saved files available in:'
hdfs dfs -ls $OUTDIR/

#### THIS RUNS ON THE ENTIRE **GREEN** TAXIS DATASET ####
