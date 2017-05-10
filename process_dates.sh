#!/bin/bash

# Example usage: process_dates.sh yellow_tripdata_2015-01_small.csv

IN=$1
PARTITION=$2
OUTS=(
  "nondate"
  "year"
  "month"
  "day"
  "hour"
  "minute"
  "second"
)
COLS=(
  "tpep_pickup_datetime"
  "tpep_dropoff_datetime"
)

for col in "${COLS[@]}"; do
  for o in "${OUTS[@]}"; do
    cmd="hdfs dfs -rm -r -f $IN.$col.$o.csv"
    echo $cmd
    $cmd
  done

  spark-submit process_dates.py $IN 20 $col

  OD="$IN.$col"
  mkdir -p $OD
  rm $OD/*

  for o in "${OUTS[@]}"; do
    cmd="hdfs dfs -getmerge $IN.$col.$o.csv $OD/$o.csv"
    echo $cmd
    $cmd
  done
done
