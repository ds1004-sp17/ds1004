#!/bin/bash

for i in `seq -f %02g 8 12`; do
  URL=https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2013-$i.csv
  FILE=yellow_tripdata_2013-$i.csv
  echo $URL
  wget $URL
  hdfs dfs -put $FILE /user/ch1751/public/taxis/$FILE
done
