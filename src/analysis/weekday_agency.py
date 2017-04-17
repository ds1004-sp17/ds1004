from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from datetime import datetime


def check_datetime(dt):
	try:
		d = datetime.strptime(dt, "%m/%d/%Y %I:%M:%S %p")
		if d.year < 2009 or d.year > 2017:
			return False
	except ValueError:
		return False

	return True

def get_week(dt):
    d = datetime.strptime(dt, "%m/%d/%Y %I:%M:%S %p")
    return d.year, d.weekday(), d.hour


if __name__ == "__main__":
	sc = SparkContext()
	data = sc.textFile(sys.argv[1], 1)
 
	line = data.map(lambda x:(x.encode('ascii','ignore')))\
				.mapPartitions(lambda x: (reader(x, delimiter = ',', quotechar = '"')))


	weekday_agency = line.filter(lambda x : check_datetime(x[1])) \
						.map(lambda x:((get_week(x[1])),1))\
						.foldByKey(0 , add) \
						.map(lambda x: "%s,%s,%s,%d" % (x[0][0],x[0][1],x[0][2],x[1]))
	
	weekday_agency.saveAsTextFile("weekday_agency.out")

	sc.stop()