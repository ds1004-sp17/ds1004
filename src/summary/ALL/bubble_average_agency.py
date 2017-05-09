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

def get_date(dt):
    d = datetime.strptime(dt, "%m/%d/%Y %I:%M:%S %p")
    return str(d.year), str(d.month), str(d.day)


def solve_time(created, closed):
    created = datetime.strptime(created, "%m/%d/%Y %I:%M:%S %p")
    try:
        closed = datetime.strptime(closed, "%m/%d/%Y %I:%M:%S %p")
    except ValueError:
        closed = datetime.now()
    return (closed - created).days


if __name__ == "__main__":

	sc = SparkContext()
	complaint = sc.textFile(sys.argv[1], 1)
	complaints = complaint.map(lambda x: x.encode('ascii', 'ignore')) \
					  .mapPartitions(lambda x: reader(x, delimiter = ',',quotechar = '"'))


	bubble_average_agency = complaints.filter(lambda x : check_datetime(x[1])) \
	          .map(lambda x: ((get_date(x[1])[0],x[3]), solve_time(x[1],x[2]))) \
	          .combineByKey(lambda value: (value,1.0),
	                        lambda x,value: (x[0]+value, x[1]+1),
	                        lambda x,y : (x[0]+y[0], x[1]+y[1])) \
	          .map(lambda x : "%s,%s,%d,%f" % (x[0][0], x[0][1] ,x[1][1], x[1][0]/x[1][1]))


	bubble_average_agency.saveAsTextFile("bubble_average_agency.out")


	sc.stop()








