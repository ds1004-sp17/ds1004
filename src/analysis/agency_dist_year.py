from csv import reader
from pyspark import SparkContext
from operator import add
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),'..')))
from analysis.utils import sum_none, check_datetime, get_date


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