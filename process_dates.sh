#!/bin/bash

IN='extract_yellow_small.csv'
PARTITION=5
OUTS=(
  "nondate"
  "year"
  "month"
  "day"
  "hour"
  "minute"
  "second"
)

for o in "${OUTS[@]}"; do
  cmd="hdfs dfs -rm -r -f $IN.$o.csv"
  echo $cmd
  $cmd
done

spark-submit process_dates.py $IN $PARTITION

OD="$IN.out"
mkdir -p $OD

for o in "${OUTS[@]}"; do
  cmd="hdfs dfs -getmerge $IN.$o.csv $OD/$IN.$o.csv"
  echo $cmd
  $cmd
done
