#!/bin/bash

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

for o in "${OUTS[@]}"; do
  cmd="hdfs dfs -rm -r -f $IN.$o.csv"
  echo $cmd
  $cmd
done

spark-submit process_dates.py $IN $PARTITION

OD="$IN.out"
mkdir -p $OD
rm $OD/*

for o in "${OUTS[@]}"; do
  cmd="hdfs dfs -getmerge $IN.$o.csv $OD/$IN.$o.csv"
  echo $cmd
  $cmd
done
