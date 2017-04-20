module purge 
module load python/gnu/3.4.4
export PYSPARK_PYTHON=/share/apps/python/3.4.4/bin/python
export PYTHONHASHSEED=0
export SPARK_YARN_USER_ENV=PYTHONHASHSEED=0

out="$1"
rm "$out.txt"
pyspark --packages com.databricks:spark-csv_2.11:1.5.0 "$out.py"
hadoop fs -getmerge "$out.out" "$out.txt"
hadoop fs -rm -r "$out.out"
