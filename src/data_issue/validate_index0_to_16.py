# Output the base type, sematinc type and valid or not 
# for the keys in the first column. index[0]

from __future__ import print_function

import datetime
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

	# Index[0]
	def verificate_unique_key(unique_key):
		try:
			if len(unique_key) == 8 and int(unique_key):
				unique_key = unique_key,"VALID"
		except ValueError:
			unique_key = unique_key,"INVALID"
		return unique_key

	unique_key = line.map(lambda x: (x[0].encode('utf-8')))\
					.map(lambda x:verificate_unique_key(x))\
					.map(lambda x: str(x[0])+", Plain Text, Unique identifier of a Service Request (SR) in the open data set, " + str(x[1]))\
					.saveAsTextFile("00_uniquekey.out")

	# Index[1]
	def verificate_date_related(date):
		date = date.lower()

		if date == '' or date == 'n/a' or date == 'na' or date == 'unspecified':
			date = date, "NULL"
		else:

			try:
				datetime.datetime.strptime(date, '%m/%d/%Y %I:%M:%S %p')
				date = date, "VALID"

			except ValueError:
				date = date, "INVALID"

		return date	


	created_date = line.map(lambda x: (x[1].encode('utf-8')))\
					.map(lambda x: verificate_date_related(x))\
					.map(lambda x: str(x[0])+", Date & Time, Date SR was created, " + str(x[1]))\
					.saveAsTextFile("01_created_date.out")

	# Index[2]
	close_date = line.map(lambda x: (x[2].encode('utf-8')))\
					.map(lambda x:verificate_date_related(x))\
					.map(lambda x: str(x[0])+", Date & Time, Date SR was closed by responding agency, " + str(x[1]))\
					.saveAsTextFile("02_close_date.out")

	
	def general_verificate(contents):
		contents = contents.lower()
		
		if contents == '' or contents == 'na' or contents == 'n/a' or contents == 'unspecified':
			contents = contents, "NULL"
		else:
			contents = contents, "VALID"

		return contents

	# Index[3]
	agency = line.map(lambda x: (x[3].encode('utf-8')))\
					.map(lambda x:general_verificate(x))\
					.map(lambda x: str(x[0])+", Plain Text, Acronym of responding City Government Agency, " + str(x[1]))\
					.saveAsTextFile("03_agency.out")


	# Index[4]
	agency_name = line.map(lambda x: (x[4].encode('utf-8')))\
					.map(lambda x:general_verificate(x))\
					.map(lambda x: str(x[0])+", Plain Text, This is the fist level of a hierarchy identifying the topic of the incident or condition. Complaint Type may have a corresponding Descriptor (below) or may stand alone, " + str(x[1]))\
					.saveAsTextFile("04_agency_name.out")

	# Index[5]
	complaint_type = line.map(lambda x: (x[5].encode('utf-8')))\
					.map(lambda x:general_verificate(x))\
					.map(lambda x: str(x[0])+", Plain Text, Full Agency name of responding City Government Agency, " + str(x[1]))\
					.saveAsTextFile("05_complaint_type.out")

	# Index[6]
	descriptor = line.map(lambda x:(x[6].encode('utf-8')))\
						.map(lambda x:general_verificate(x))\
						.map(lambda x: str(x[0])+", Plain Text, This is associated to the Complaint Type, and provides further detail on the incident or condition. Descriptor values are dependent on the Complaint Type, and are not always required in SR, " + str(x[1]))\
						.saveAsTextFile("06_descriptor.out")

	# Index[7]
	location_type = line.map(lambda x:(x[7].encode('utf-8')))\
						.map(lambda x:general_verificate(x))\
						.map(lambda x: str(x[0])+", Plain Text, Describes the type of location used in the address information, " + str(x[1]))\
						.saveAsTextFile("07_location_type.out")


	# Index[8]
	def verificate_incident_zip(incident_zip):
		if incident_zip == '' or incident_zip == 'NA' or incident_zip == 'N/A' or incident_zip == 'Unspecified':
			incident_zip = incident_zip, "NULL"

		elif len(incident_zip)==5:
			if incident_zip == '00083' or incident_zip.startswith('1'):
				incident_zip = incident_zip, "VALID"
			else:
				incident_zip = incident_zip, "INVALID"
		else:
			start = incident_zip[0:5]
			if start.startswith('1'):
				incident_zip = incident_zip, "VALID"
			else:
				incident_zip = incident_zip,"INVALID"
		return incident_zip

	incident_zip = line.map(lambda x:(x[8].encode('utf-8')))\
						.map(lambda x:verificate_incident_zip(x))\
						.map(lambda x: str(x[0])+", Plain Text, Incident location zip code, provided by geo validation, " + str(x[1]))\
						.saveAsTextFile("08_incident_zip.out")

	# Index[9]
	incident_address = line.map(lambda x:(x[9].encode('utf-8')))\
						.map(lambda x:general_verificate(x))\
						.map(lambda x: str(x[0])+", Plain Text, House number of incident address provided by submitter, " + str(x[1]))\
						.saveAsTextFile("09_incident_address.out")

	# Index[10]
	street_name = line.map(lambda x:(x[10].encode('utf-8')))\
						.map(lambda x:general_verificate(x))\
						.map(lambda x: str(x[0])+", Plain Text, Street name of incident address provided by the submitter, " + str(x[1]))\
						.saveAsTextFile("10_street_name.out")

	# Index[11]
	cross_st_01 = line.map(lambda x:(x[11].encode('utf-8')))\
						.map(lambda x:general_verificate(x))\
						.map(lambda x: str(x[0])+", Plain Text, First Cross street based on the geo validated incident location, " + str(x[1]))\
						.saveAsTextFile("11_cross_st_01.out")


	# Index[12]
	cross_st_02 = line.map(lambda x:(x[12].encode('utf-8')))\
						.map(lambda x:general_verificate(x))\
						.map(lambda x: str(x[0])+", Plain Text, Second Cross Street based on the geo validated incident location, " + str(x[1]))\
						.saveAsTextFile("12_cross_st_02.out")


	# Index[13]
	intersection_st_01 = line.map(lambda x:(x[13].encode('utf-8')))\
							.map(lambda x:general_verificate(x))\
							.map(lambda x: str(x[0])+", Plain Text, First intersecting street based on geo validated incident location, " + str(x[1]))\
							.saveAsTextFile("13_intersection_st_01.out")


	# Index[14]
	intersection_st_02 = line.map(lambda x:(x[14].encode('utf-8')))\
							.map(lambda x:general_verificate(x))\
							.map(lambda x: str(x[0])+", Plain Text, Second intersecting street based on geo validated incident location, " + str(x[1]))\
							.saveAsTextFile("14_intersection_st_02.out")


	# Index[15]
	address_type = line.map(lambda x:(x[15].encode('utf-8')))\
						.map(lambda x:general_verificate(x))\
						.map(lambda x: str(x[0])+", Plain Text, Type of incident location information available, " + str(x[1]))\
						.saveAsTextFile("15_address_type.out")


	# Index[16]
	city = line.map(lambda x:(x[16].encode('utf-8')))\
				.map(lambda x:general_verificate(x))\
				.map(lambda x: str(x[0])+", Plain Text, City of the incident location provided by geovalidation, " + str(x[1]))\
				.saveAsTextFile("16_city.out")
	
	sc.stop()



