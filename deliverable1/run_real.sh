#!/bin/bash

#### THIS RUNS ON THE ENTIRE TAXIS DATASET ####

OUTDIR=taxis_results

module purge
module load python/gnu/2.7.11
module load pandas/0.18.1

# Print this file out.
cat run_real.sh

hdfs dfs -mkdir $OUTDIR/
hdfs dfs -rm -r -f $OUTDIR/*
hdfs dfs -mkdir $OUTDIR/_tmp/

time spark-submit column_analysis.py \
  --input_dir /user/ch1751/public/taxis/ \
  --save_path $OUTDIR \
  --tempdir $OUTDIR/_tmp \
  --min_partitions 20

hdfs dfs -rm -r -f $OUTDIR/_tmp/

echo 'Saved files available in:'
hdfs dfs -ls $OUTDIR/

#### THIS RUNS ON THE ENTIRE TAXIS DATASET ####
