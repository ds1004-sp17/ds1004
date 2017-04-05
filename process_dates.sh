#!/bin/bash

IN='extract_yellow_small.csv'
PARTITION=5

hdfs dfs -rm -f "$IN.nondate.csv"
hdfs dfs -rm -f "$IN.year.csv"
hdfs dfs -rm -f "$IN.month.csv"
hdfs dfs -rm -f "$IN.day.csv"
hdfs dfs -rm -f "$IN.hour.csv"
hdfs dfs -rm -f "$IN.minute.csv"
hdfs dfs -rm -f "$IN.second.csv"

spark-submit $IN $PARTITION
