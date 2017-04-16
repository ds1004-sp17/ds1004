from csv import reader
from pyspark import SparkContext
from operator import add
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),'..')))
from utils import sum_none, check_datetime, get_date2


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