# The input csv file
filename="$1"

mkdir basic_out

for ((i=0; i<52; i++)); do
	hadoop fs -rm -r "basic_$i.out"
done

spark-submit basic_statistics_info.py "$filename"
for ((i=0; i<52; i++)); do
	hadoop fs -getmerge "basic_$i.out" "basic_$i.txt"
	mv "basic_$i.txt" basic_out/
	hadoop fs -rm -r "basic_$i.out"
done