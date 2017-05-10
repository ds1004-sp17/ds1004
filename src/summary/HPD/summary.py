from csv import reader
from pyspark import SparkContext
from operator import add
import sys
import os
from datetime import datetime

import heapq
 
def takeOrderedByKey(self, num, sortValue = None, reverse=False):
 
        def init(a):
            return [a]
 
        def combine(agg, a):
            agg.append(a)
            return getTopN(agg)
 
        def merge(a, b):
            agg = a + b
            return getTopN(agg)
 
        def getTopN(agg):
            if reverse == True:
                return heapq.nlargest(num, agg, sortValue)
            else:
                return heapq.nsmallest(num, agg, sortValue)              
 
        return self.combineByKey(init, combine, merge)

from pyspark.rdd import RDD
RDD.takeOrderedByKey = takeOrderedByKey

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
    return str(d.year), str(d.month), str(d.day)

def get_week(dt):
    d = datetime.strptime(dt, "%m/%d/%Y %I:%M:%S %p")
    return d.year, d.weekday(), d.hour

if __name__ == "__main__":

	sc = SparkContext()
	complaint = sc.textFile(sys.argv[1], 1)
	complaints = complaint.map(lambda x: x.encode('ascii', 'ignore')) \
					  .mapPartitions(lambda x: reader(x, delimiter = ',',quotechar = '"'))

	HPD_complaint_year = complaints.filter(lambda x: check_datetime(x[1])) \
	                          .filter(lambda x: x[3] == 'HPD') \
	                          .map(lambda x: ((get_date(x[1])[0], x[3], x[5]),1)) \
	                          .foldByKey(0, add) \
	                          .map(lambda x : "%s,%s,%s" % (x[0][0], x[0][2], x[1]))
	HPD_complaint_year.saveAsTextFile("HPD_complaint_year.out") 

	# 2
	HPD_complaint_day = complaints.filter(lambda x: check_datetime(x[1])) \
	                          .filter(lambda x: x[3] == 'HPD') \
	                          .filter(lambda x: x[5] == 'HEAT/HOT WATER') \
	                          .map(lambda x: ((get_date(x[1]), x[3], x[5]),1)) \
	                          .foldByKey(0, add) \
	                          .map(lambda x : "%s,%s,%d" % ('-'.join(x[0][0]), x[0][2],x[1]))
	HPD_complaint_day.saveAsTextFile("HPD_complaint_day.out")  
	#HPD_complaint_day.take(1)

	# 2.1
	HPD_complaint_week = complaints.filter(lambda x: check_datetime(x[1])) \
	                          .filter(lambda x: x[3] == 'HPD') \
	                          .filter(lambda x: x[5] == 'HEAT/HOT WATER') \
	                          .map(lambda x: ((get_week(x[1]), x[3], x[5]),1)) \
	                          .foldByKey(0, add) \
	                          .map(lambda x : "%s,%s,%s,%s,%d" % (x[0][0][0], x[0][0][1],x[0][0][2], x[0][2],x[1]))
	HPD_complaint_week.saveAsTextFile("HPD_complaint_week.out")  
	#HPD_complaint_week.take(1)



	HPD_complaint_outlier = complaints.filter(lambda x: check_datetime(x[1])) \
	                          .filter(lambda x: x[3] == 'HPD') \
	                          .filter(lambda x: x[5] == 'HEAT/HOT WATER') \
	                          .map(lambda x: (get_date(x[1]),1)) \
	                          .foldByKey(0, add) \
	                          .map(lambda x: (x[0][0],(x[0],x[1]))) \
	                          .takeOrderedByKey(10, sortValue=lambda x: x[1], reverse=True) \
	                          .flatMap(lambda x: x[1])
	                          #.map(lambda x : "%s,%s,%s,%s,%d" % (x[0][0][0], x[0][0][1],x[0][0][2], x[0][2],x[1]))
	#HPD_complaint_outlier.saveAsTextFile("HPD_complaint_outlier.out")  
	#HPD_complaint_outlier.collect()

	time_lists = HPD_complaint_outlier.map(lambda x: '-'.join(x[0])).collect()
	#HPD_complaint_top_day = 

	HPD_complaint_all_days = complaints.filter(lambda x: check_datetime(x[1])) \
	                                  .filter(lambda x: x[3] == 'HPD') \
	                                  .filter(lambda x: x[5] == 'HEAT/HOT WATER') \
	                                  .map(lambda x: ('-'.join(get_date(x[1])),x[3],x[5],x[8],x[51])) \
	                                  .filter(lambda x: x[0] in time_lists) \
	                                  .map(lambda x : "%s,%s,%s,%s,%s" % (x[0], x[1],x[2], x[3],x[4])) 

	HPD_complaint_all_days.saveAsTextFile("HPD_complaint_all_days.out") 

	sc.stop()