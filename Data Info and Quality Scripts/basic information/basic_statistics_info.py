# This program counts some important columns.
# The content of the dataset is from 2010 to 2017, March 28th
# Usage:
#	spark-submit basic_statistics_info.py 311_test_new.csv

from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
	sc = SparkContext()
	data = sc.textFile(sys.argv[1], 1)
	header = data.first() 
	lines = data.filter(lambda row: row != header) 
	line = lines.mapPartitions(lambda x: (reader(x, delimiter = ',', quotechar = '"')))

	# Count how many cases there are every day
	count_case_everyday = line.map(lambda x: (x[1][1:10].encode('utf-8'), 1))\
						.reduceByKey(add)\
						.sortByKey()\
						.map(lambda x: str(x[0])+'\t'+str(x[1]))					
	count_case_everyday.saveAsTextFile("case_everyday.out")

	# Count the number of cases of each type
	count_complain_type = line.map(lambda x: (x[5].encode('utf-8'),1))\
							.reduceByKey(add)\
							.sortByKey()\
							.map(lambda x: str(x[0])+'\t'+str(x[1]))
	count_complain_type.saveAsTextFile("complain_type.out")

	# Count the number of cases of each location type
	count_location_type = line.map(lambda x: (x[7].encode('utf-8'),1))\
							.reduceByKey(add)\
							.sortByKey()\
							.map(lambda x: str(x[0])+'\t'+str(x[1]))
	count_location_type.saveAsTextFile("location_type.out")

	# Count the number of cases of each incident zip
	count_incident_zip = line.map(lambda x: (x[8].encode('utf-8'),1))\
							.reduceByKey(add)\
							.sortByKey()\
							.map(lambda x: str(x[0])+'\t'+str(x[1]))
	count_incident_zip.saveAsTextFile("incedent_zip.out")

	# Count the number of cases in each city
	count_city = line.map(lambda x: (x[16].encode('utf-8'),1))\
					.reduceByKey(add)\
					.sortByKey()\
					.map(lambda x: str(x[0])+'\t'+str(x[1]))
	count_city.saveAsTextFile("city.out")

	# Count the number of cases which are solved and not
	count_status = line.map(lambda x: (x[19].encode('utf-8'),1))\
					.reduceByKey(add)\
					.sortByKey()\
					.map(lambda x: str(x[0])+'\t'+str(x[1]))
	count_status.saveAsTextFile("status.out")

	count_all_rows = lines.count().saveAsTextFile("count_all.out")


	sc.stop()
