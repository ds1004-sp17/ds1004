
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

hadoop fs -rm -r scatter_matrix.out 
spark-submit scatter_matrix.py $filename
hadoop fs -getmerge scatter_matrix.out scatter_matrix.txt

hadoop fs -rm -r bubble_average_agency.out 
spark-submit bubble_average_agency.py $filename
hadoop fs -getmerge bubble_average_agency.out bubble_average_agency.txt

hadoop fs -rm -r map_zip_amount.out 
spark-submit map_zip_amount.py $filename
hadoop fs -getmerge map_zip_amount.out map_zip_amount.txt


hadoop fs -rm -r weekday_agency.out 
spark-submit weekday_agency.py $filename
hadoop fs -getmerge weekday_agency.out weekday_agency.txt