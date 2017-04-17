from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
import datetime

if __name__ == "__main__":
	sc = SparkContext()
	data = sc.textFile(sys.argv[1], 1)
	header = data.first() 
	lines = data.filter(lambda row: row != header) 
	line = lines.map(lambda x:(x.encode('ascii','ignore')))\
				.mapPartitions(lambda x: (reader(x, delimiter = ',', quotechar = '"')))

	def get_weekday_agency(date, agency):
	    date = date.lower()
	    year = date[6:10]
	    hour = date[11:13]
	    period = date[20:22]
	    agency = agency
	    
	    if period == 'pm':
	        hour = int(hour) + 12
	        hour = str(hour)

	    try:
	        date = datetime.datetime.strptime(date, '%m/%d/%Y %I:%M:%S %p')
	        weekday = str(date.weekday())
	    except ValueError:
	        pass

	    return year, weekday, hour, agency


	weekday_agency = line.map(lambda x: (x[1],x[3]))\
						.map(lambda x:(get_weekday_agency(x[0],x[1]),1))\
						.reduceByKey(add)\
						.sortByKey()\
						.map(lambda x: "%s,%s,%s,%s,%d" % (x[0][0],x[0][1],x[0][2],x[0][3],x[1]))
	weekday_agency.saveAsTextFile("weekday_agency.out")

	sc.stop()