# ds1004

## Date Processing Pipeline

There are two dates: pickup and dropoff.

1. First, extract the column using ``extract_column.py``.
1. Upload the file to HDFS.
1. Then, run ``process_dates.sh <file> <partitions>``.
   For a month, 20 is a good number.
