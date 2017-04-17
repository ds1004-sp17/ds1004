
filename="$1"


hadoop fs -rm -r num_agency_day_box.out
spark-submit agency_dist_year.py $filename
hadoop fs -getmerge num_agency_day_box.out num_agency_day_box.txt


hadoop fs -rm -r month_agency_bar.out
spark-submit distribution_month_agency_bar.py $filename
hadoop fs -getmerge month_agency_bar.out month_agency_bar.txt


hadoop fs -rm -r num_agency_day.out
spark-submit num_agency_day.py $filename
hadoop fs -getmerge num_agency_day.out num_agency_day.txt


hadoop fs -rm -r type_year_pie.out 
spark-submit type_year_pie.py $filename
hadoop fs -getmerge type_year_pie.out type_year_pie.txt


hadoop fs -rm -r board_agency_year.out 
spark-submit board_agency_year.py $filename
hadoop fs -getmerge board_agency_year.out board_agency_year.txt
