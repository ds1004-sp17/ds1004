from csv import reader
from pyspark import SparkContext
from operator import add
import sys
import os



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

if __name__ == "__main__":

	sc = SparkContext()
	complaint = sc.textFile(sys.argv[1], 1)
	complaints = complaint.map(lambda x: x.encode('ascii', 'ignore')) \
					  .mapPartitions(lambda x: reader(x, delimiter = ',',quotechar = '"'))


	num_agency_day_box = complaints.filter(lambda x : check_datetime(x[1])) \
	          .map(lambda x: ((get_date(x[1]),x[3]), 1)) \
	          .foldByKey(0, add) \
	          .map(lambda x: ((x[0][0][0], x[0][1]),x[1])) \
	          .map(lambda x : "%s,%s,%d" % (x[0][0], x[0][1], x[1])) \
	          
	num_agency_day_box.saveAsTextFile("num_agency_day_box.out")

	sc.stop()