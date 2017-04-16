# The input csv file
filename="$1"

mkdir basic_out
spark-submit basic_statistics_info.py $filename
for ((i=0; i<52; i++)); 
do
	hadoop fs -getmerge "basic_$i.out" "basic_$i.txt"
	mv "basic_$i.txt" basic_out/
	hadoop fs -rm -r "basic_$i.out"
done

mkdir mv_out
spark-submit missing_values.py $filename
for ((i=0; i<52; i++)); 
do
	hadoopfs -getmerge "mv_$i.out" "mv_$i.txt"
	mv "mv_$i.txt" mv_out/
	hadoopfs -rm -r "mv_$i.out"
done

mkdir validation
spark-submit validate_index0_to_16.py $filename
for ((i=0; i<17; i++));
do
	hadoop fs -getmerge "valid_$i.out" "valid_$i.txt"
	mv "valid_$i.txt" validation
	hadoop fs -rm -r "valid_$i.out"
done

spark-submit validate_index17_to_33.py $filename
for ((i=17; i<34; i++));
do
	hadoop fs -getmerge "valid_$i.out" "valid_$i.txt"
	mv "valid_$i.txt" validation
	hadoop fs -rm -r "valid_$i.out"
done

spark-submit validate_index34_to_51.py $filename
for ((i=34; i<52; i++));
do
	hadoop fs -getmerge "valid_$i.out" "basic_$i.txt"
	mv "valid_$i.txt" validation
	hadoop fs -rm -r "valid_$i.out"
done