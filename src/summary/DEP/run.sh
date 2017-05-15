filename="$1"


hadoop fs -rm -r DHS_complaint_month.out
hadoop fs -rm -r DHS_complaint_year.out



spark-submit summary.py $filename

hadoop fs -getmerge DHS_complaint_month.out DHS_complaint_month.txt
hadoop fs -getmerge DHS_complaint_year.out DHS_complaint_year.txt

