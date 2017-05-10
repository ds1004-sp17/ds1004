filename="$1"

mkdir NYPD_noise

hadoop fs -rm -r "NYPD_year_type.out"
hadoop fs -rm -r "date_noise.out"
hadoop fs -rm -r "hour_noise.out"

spark-submit case_each_year.py "$filename"
spark-submit hour_noise.py "$filename"
spark-submit date_noise.py "$filename"

hadoop fs -getmerge NYPD_year_type.out NYPD_year_type.out
hadoop fs -getmerge date_noise.out date_noise.out
hadoop fs -getmerge hour_noise hour_noise

mv NYPD_year_type.out NYPD_noise
mv date_noise.out NYPD_noise
mv hour_noise NYPD_noise