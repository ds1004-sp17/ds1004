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

	def get_date(dt):
	    d = datetime.strptime(dt, "%m/%d/%Y %I:%M:%S %p")
	    return (str(d.year)+'-'+str(d.month)+'-'+str(d.day))

	def check_complaint_type(complaint_type):
		complaint_type = complaint_type.lower()
		try:
			if complaint_type.startswith("noise"):
				return complaint_type
			else:
				pass
		except ValueError:
			pass

	def select_top10(): 

	noise_top10 = line.filter(lambda x: check_datetime(x[1]))\
	                    .map(lambda x: (get_date(x[1]),x[5]))\
	                    .filter(lambda x : check_complaint_type(x[1]))\
	                    .map(lambda x: ((x[1], x[0]),1))\
	                    .reduceByKey(add)\
	                    .map(lambda x: (((x[0][0]),x[1]),x[0][1]))\
	                    .sortByKey(ascending=False)\
	                    .map(lambda x:())
	                    .map(lambda x: (str(x[0][0])+ ',' + str(x[0][1])+','+str(x[1])))\
	                    .saveAsTextFile("noise_top10.out")

	sc.stop()