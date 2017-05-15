filename="$1"


hadoop fs -rm -r HPD_complaint_year.out
hadoop fs -rm -r HPD_complaint_day.out
hadoop fs -rm -r HPD_complaint_week.out
hadoop fs -rm -r HPD_complaint_all_days.out


spark-submit summary.py $filename

hadoop fs -getmerge HPD_complaint_year.out HPD_complaint_year.txt
hadoop fs -getmerge HPD_complaint_day.out HPD_complaint_day.txt
hadoop fs -getmerge HPD_complaint_week.out HPD_complaint_week.txt
hadoop fs -getmerge HPD_complaint_all_days.out HPD_complaint_all_days.txt

