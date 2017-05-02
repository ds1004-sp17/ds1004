from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
	sc = SparkContext()
	data = sc.textFile(sys.argv[1], 1)
	header = data.first() 
	lines = data.filter(lambda row: row != header) 
	line = lines.map(lambda x:(x.encode('ascii','ignore')))\
				.mapPartitions(lambda x: (reader(x, delimiter = ',', quotechar = '"')))

	def check_missing_values(content):
		try:
			contents = content.lower()
		except AttributeError:
			contents = content
		
		result = "NOT NULL"

		if contents == '':
			result = "BLANK"
		
		elif (contents == 'n/a' or contents == 'N/A'):
			result = "N/A"
		
		elif (contents == 'na' or contents == 'NA'):
			result = "NA"
		
		elif (contents == 'unspecified' or contents == 'Unspecified'):
			result = "Unspecified"

		return result

	location = line.map(lambda x: (x[52].encode('utf-8')))\
					.map(lambda x: (check_missing_values(x), 1))\
					.reduceByKey(add)\
					.map(lambda x: x[0]+'\t'+str(x[1]))\
					.saveAsTextFile("mv_52.out")

