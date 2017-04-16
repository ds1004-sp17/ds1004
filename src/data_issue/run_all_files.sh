module purge 
module load python/gnu/3.4.4
export PYSPARK_PYTHON=/share/apps/python/3.4.4/bin/python
export PYTHONHASHSEED=0
export SPARK_YARN_USER_ENV=PYTHONHASHSEED=0

# The input csv file
filename = "$2"

mkdir basic_out
spark-submit basic_statistics_info.py "$filename"
for ((i=0; i<52; i++)); do
	hfs -getmerge "basic_$i.out" "basic_$i.txt"
	mv "basic_$i.txt" basic_out/
	hfs -rm -r "basic_$i.out"
done

mkdir mv_out
spark-submit missing_values.py "$filename"
for ((i=0; i<52; i++)); do
	hfs -getmerge "mv_$i.out" "mv_$i.txt"
	mv "mv_$i.txt" mv_out/
	hfs -rm -r "mv_$i.out"
done

mkdir validation
spark-submit validate_index0_to_16.py "$filename"
for ((i=0; i<17; i++)); do
	hfs -getmerge "valid_$i.out" "valid_$i.txt"
	mv "valid_$i.txt" validation
	hfs -rm -r "valid_$i.out"
done

spark-submit validate_index17_to_33.py "$filename"
for ((i=17; i<34; i++)); do
	hfs -getmerge "valid_$i.out" "valid_$i.txt"
	mv "valid_$i.txt" validation
	hfs -rm -r "valid_$i.out"
done

spark-submit validate_index34_to_51.py "$filename"
for ((i=34; i<52; i++)); do
	hfs -getmerge "_$i.out" "basic_$i.txt"
	mv "valid_$i.txt" validation
	hfs -rm -r "valid_$i.out"
done