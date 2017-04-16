# The input csv file
filename="$1"

mkdir mv_out

for ((i=0; i<52; i++)); do
	hadoop fs -rm -r "mv_$i.out"
done

spark-submit missing_values.py "$filename"

for ((i=0; i<52; i++)); do
	hadoop fs -getmerge "mv_$i.out" "mv_$i.txt"
	mv "mv_$i.txt" mv_out/
	hadoop fs -rm -r "mv_$i.out"
done