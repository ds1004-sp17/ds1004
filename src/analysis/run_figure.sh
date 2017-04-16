module purge 
module load python/gnu/3.4.4
export PYSPARK_PYTHON=/share/apps/python/3.4.4/bin/python
export PYTHONHASHSEED=0
export SPARK_YARN_USER_ENV=PYTHONHASHSEED=0

filename="$1"

spark-submit agency_dist_year.py $filename
hadoop fs -getmerge num_agency_day_box.out num_agency_day_box.out
hadoop fs -rm -r num_agency_day_box.out

spark-submit distribution_month_agency_bar.py $filename
hadoop fs -getmerge month_agency_bar.out month_agency_bar.out
hadoop fs -rm -r month_agency_bar.out

spark-submit num_agency_day.py $filename
hadoop fs -getmerge num_agency_day.out num_agency_day.out
hadoop fs -rm -r num_agency_day.out

spark-submit type_year_pie.py $filename
hadoop fs -getmerge type_year_pie.out type_year_pie.out
hadoop fs -rm -r type_year_pie.out 