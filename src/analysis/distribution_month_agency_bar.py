from csv import reader
from pyspark import SparkContext
from operator import add
import pandas as pd
import sys
import os
#sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),'..')))
from utils import sum_none, check_datetime, get_date


if __name__ == "__main__":

	sc = SparkContext()
	complaint = sc.textFile(sys.argv[1], 1)
	complaints = complaint.map(lambda x: x.encode('ascii', 'ignore')) \
					  .mapPartitions(lambda x: reader(x, delimiter = ',',quotechar = '"'))


	month_agency_bar = complaints.filter(lambda x : check_datetime(x[1])) \
	          .map(lambda x: ((get_date(x[1])[0:2],x[3],x[5]), 1)) \
	          .foldByKey(0, add) \
	          .map(lambda x: ('_'.join([x[0][0][0],x[0][0][1]]), x[0][1],x[0][2],x[1])) \
	          .map(lambda x : "%s,%s,%s,%d" % (x[0], x[1], x[2],x[3]))


	month_agency_bar.saveAsTextFile("month_agency_bar.out")

	sc.stop()