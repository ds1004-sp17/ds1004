# ds1004

Code for NYC Taxi analysis.

## Deliverable 1

The main parts of the submission are the per-column analysis files and scripts
to generate plots in the report.

### Per-column analyzer

This is a Spark 1 job that loads the yellow taxis dataset and analyzes each
column value independently. The program takes a directory and reads files for
each month in the given years.

The script expects file names in a certain format:

``$inputdir/yellow_tripdata_$year-$month.csv``

For a given year, all months from January to December must be present. It then
dumps results to a directory in HDFS of your choosing:
```
taxis_test_results/DOLocationID.csv
taxis_test_results/PULocationID.csv
taxis_test_results/RatecodeID.csv
taxis_test_results/VendorID.csv
...
```
The easiest way to run the script is to call one of the starter shell scripts
``run_test.sh`` or ``run_real.sh``. **IMPORTANT: the program or the scripts
MUST be run from inside the deliverables1/ subdirectory.**
The first one runs on a small subset of the real data. I've made copies of
the data publicly readable on these HDFS directories:
```
/user/ch1751/public/taxis/          Yellow taxis full
/user/ch1751/public/taxis_test/     Yellow taxis testing data (small subset)
```
Some files in the testing data is manually edited and corrupted to introduce
errors, to make sure the program can handle it.

#### Tuning

There are some other parameters you can tune in the python file.
```
--min_partitions MIN_PARTITIONS
                      minimum number of data partitions when loading raw
                      data for a single month.
--tempfile_partitions TEMPFILE_PARTITIONS
                      minimum number of data partitions for the per-month
                      /per-column temp files.
```
These control ``minPartitions`` when calling ``sc.textFile()``, in case the
program is running too slowly or the process is too big to fit into the
given number of machines. Run ``spark-submit column_analysis.py --help``
to see more options.

#### Outputting to stdout

This script can also print the joined contents to standard output instead of
saving into files, using the ``--dump`` option.
