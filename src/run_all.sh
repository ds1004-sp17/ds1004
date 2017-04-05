module purge 
module load python/gnu/3.4.4
export PYSPARK_PYTHON=/share/apps/python/3.4.4/bin/python
export PYTHONHASHSEED=0
export SPARK_YARN_USER_ENV=PYTHONHASHSEED=0

#./run.sh complaint_type
#./run.sh location_type
#./run.sh nums_in_month
#./run.sh city
#./run.sh agency
#./run.sh duration_by_agency
#./run.sh zipcode
#./run.sh nums_in_hour
#./run.sh nums_in_month_by_agency
#./run.sh nums_in_hour_by_agency
./run.sh nums_in_month_by_agency_year
