filename="$1"

mkdir NYPD_noise

hadoop fs -rm -r "NYPD_year_type.out"
hadoop fs -rm -r "date_noise.out"
hadoop fs -rm -r "hour_noise.out"
hadoop fs -rm -r "noise_top10.out"

spark-submit case_each_year.py "$filename"
spark-submit hour_noise.py "$filename"
spark-submit date_noise.py "$filename"
spark-submit noise_top10.py "$filename"

hadoop fs -getmerge NYPD_year_type.out NYPD_year_type.out
hadoop fs -getmerge date_noise.out date_noise.out
hadoop fs -getmerge hour_noise.out hour_noise.out
hadoop fs -getmerge noise_top10.out noise_top10.out

mv NYPD_year_type.out NYPD_noise
mv date_noise.out NYPD_noise
mv hour_noise.out NYPD_noise
mv noise_top10.out NYPD_noise