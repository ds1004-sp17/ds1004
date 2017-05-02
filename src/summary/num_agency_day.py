from csv import reader
from pyspark import SparkContext
from operator import add
import sys
import os
from datetime import datetime

def check_datetime(dt):
    try:
        d = datetime.strptime(dt, "%m/%d/%Y %I:%M:%S %p")
        if d.year < 2009 or d.year > 2017:
            return False
    except ValueError:
        return False

    return True

def sum_none(x):
    s = 0
    for i in x:
        if i == None:
            s = s
        else:
            s += i
    return s

def get_date(dt):
    d = datetime.strptime(dt, "%m/%d/%Y %I:%M:%S %p")
    return str(d.year), str(d.month), str(d.day)


def get_date2(dt):
    d = datetime.strptime(dt, "%m/%d/%Y %I:%M:%S %p")
    return str(d.date())


if __name__ == "__main__":

	sc = SparkContext()
	complaint = sc.textFile(sys.argv[1], 1)

	complaints = complaint.map(lambda x: x.encode('ascii', 'ignore')) \
					  .mapPartitions(lambda x: reader(x, delimiter = ',',quotechar = '"'))

	## number of complaint by agency each day in years
	num_agency_day = complaints.filter(lambda x : check_datetime(x[1])) \
							   .map(lambda x : ((get_date2(x[1]),x[3]), 1)) \
							   .foldByKey(0, add) 
				
	agency = complaints.filter(lambda x : check_datetime(x[1])) \
					   .map(lambda x : x[3]) \
					   .distinct()

	date = complaints.filter(lambda x : check_datetime(x[1])) \
					 .map(lambda x : get_date2(x[1])) \
					 .distinct()
			
	agency_day = date.cartesian(agency) \
					 .map(lambda x : (x, 0)) \
					 .leftOuterJoin(num_agency_day) \
					 .mapValues(sum_none) \
					 .map(lambda x: "%s,%s,%d" % (x[0][0], x[0][1], x[1]))

	agency_day.saveAsTextFile("num_agency_day.out")
	sc.stop()