filename="$1"

mkdir validation

for ((i=0; i<52; i++)); do
	hadoop fs -rm -r "valid_$i.out"
done

spark-submit validate_index0_to_16.py "$filename"
spark-submit validate_index17_to_33.py "$filename"
spark-submit validate_index34_to_51.py "$filename"

for ((i=0; i<52; i++)); do
	hadoop fs -getmerge "valid_$i.out" "valid_$i.txt"
	mv "valid_$i.txt" validation
	hadoop fs -rm -r "valid_$i.out"
done