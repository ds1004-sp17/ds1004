from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from datetime import datetime

if __name__ == "__main__":
	sc = SparkContext()
	data = sc.textFile(sys.argv[1], 1)
	header = data.first() 
	lines = data.filter(lambda row: row != header) 
	line = lines.map(lambda x:(x.encode('ascii','ignore')))\
				.mapPartitions(lambda x: (reader(x, delimiter = ',', quotechar = '"')))

	def check_datetime(dt):
		try:
			d = datetime.strptime(dt, "%m/%d/%Y %I:%M:%S %p")
			if d.year < 2009 or d.year > 2017:
				return False
		except ValueError:
			return False

		return True

	def get_year(dt):
	    d = datetime.strptime(dt, "%m/%d/%Y %I:%M:%S %p")
	    return d.year

	year_agency = line.filter(lambda x: (check_datetime(x[1]),x[3]))\
	                    .map(lambda x: (str(get_year(x[1])), x[3]))\
	                    .map(lambda x: ((x[0], x[1]),1))\
	                    .reduceByKey(add)\
	                    .sortByKey()\
	                    .map(lambda x: (str(x[0][1])+ ',' + str(x[0][0])+','+str(x[1])))\
	                    .saveAsTextFile("agency_year.out")

	sc.stop()
