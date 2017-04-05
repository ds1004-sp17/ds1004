out="$1"
rm "$out.txt"
spark-submit "$out.py" /user/jc7459/311.csv
hadoop fs -getmerge "$out.out" "$out.txt"
hadoop fs -rm -r "$out.out"
