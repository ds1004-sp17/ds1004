module purge 
module load python/gnu/3.4.4
export PYSPARK_PYTHON=/share/apps/python/3.4.4/bin/python
export PYTHONHASHSEED=0
export SPARK_YARN_USER_ENV=PYTHONHASHSEED=0

filename="$1"

# The commands below run pyspark MapReduce

./run.sh complaint_type "$filename"
./run.sh location_type "$filename"
./run.sh nums_in_month "$filename"
./run.sh city "$filename"
./run.sh agency "$filename"
./run.sh duration_by_agency "$filename"
./run.sh zipcode "$filename"
./run.sh nums_in_hour "$filename"
./run.sh nums_in_month_by_agency "$filename"
./run.sh nums_in_hour_by_agency "$filename"
./run.sh nums_in_month_by_agency_year "$filename"
./run.sh check_zip "$filename"

# The commands below run sparkSQL

./run_sql.sh street
./run_sql.sh type_broadway
./run_sql.sh type_broadway_noise
