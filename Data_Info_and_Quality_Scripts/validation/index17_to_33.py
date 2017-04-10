# Output the base type, sematinc type and valid or not 

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
	line = lines.mapPartitions(lambda x: (reader(x, delimiter = ',', quotechar = '"')))

	def general_verificate(contents):
		contents = contents.lower()
		
		if contents == '' or contents == 'na' or contents == 'n/a' or contents == 'unspecified':
			contents = contents, "NULL"
		else:
			contents = contents, "VALID"

		return contents
	
	# Index[17]
	landmark = line.map(lambda x: (x[17].encode('utf-8')))\
					.map(lambda x: general_verificate(x))\
					.map(lambda x: str(x[0])+", Plain Text, If the incident location is identified as a Landmark the name of the landmark will display here, " + str(x[1]))\
					.saveAsTextFile("17_landmark.out")

	# Index[18]
	facility_type = line.map(lambda x: (x[18].encode('utf-8')))\
					.map(lambda x:general_verificate(x))\
					.map(lambda x: str(x[0])+", Plain Text, If available, this field describes the type of city facility associated to the SR, " + str(x[1]))\
					.saveAsTextFile("18_facility_type.out")

	# Index[19]
	status = line.map(lambda x: (x[19].encode('utf-8')))\
					.map(lambda x:general_verificate(x))\
					.map(lambda x: str(x[0])+", Plain Text, Status of SR submitted, " + str(x[1]))\
					.saveAsTextFile("19_status.out")

	# Index[20]
	"""
	def verificate_due_date(due_date):


	due_date = line.map(lambda x: (x[20].encode('utf-8')))\
					.map(lambda x:verificate_due_date(x))\
					.map(lambda x: str(x[0])+", Plain Text, Date when responding agency is expected to update the SR. This is based on the Complaint Type and internal Service Level Agreements (SLAs), " + str(x[1]))\
					.saveAsTextFile("20_due_date.out")
	
	
	# Index[21]
	resolution_action = line.map(lambda x: (x[21].encode('utf-8')))\
							.map(lambda x:general_verificate(x))\
							.map(lambda x: str(x[0])+", Plain Text, Date when responding agency last updated the SR, " + str(x[1]))\
							.saveAsTextFile("21_resolution_action.out")
		"""

	# Index[22]
	community_board = line.map(lambda x: (x[22].encode('utf-8')))\
							.map(lambda x:general_verificate(x))\
							.map(lambda x: str(x[0])+", Plain Text, Provided by geovalidation, " + str(x[1]))\
							.saveAsTextFile("22_community_board.out")

	# Index[23]
	borough = line.map(lambda x: (x[23].encode('utf-8')))\
					.map(lambda x:general_verificate(x))\
					.map(lambda x: str(x[0])+", Plain Text, Provided by the submitter and confirmed by geovalidation, " + str(x[1]))\
					.saveAsTextFile("23_borough.out")

	"""
	# Index[24]
	x_coordinate = line.map(lambda x: (x[24].encode('utf-8')))\
					.map(lambda x:general_verificate(x))\
					.map(lambda x: str(x[0])+", Plain Text, Geo validated: X coordinate of the incident location., " + str(x[1]))\
					.saveAsTextFile("24_x_coordinate.out")

	# Index[25]
	y_coordinate = line.map(lambda x: (x[25].encode('utf-8')))\
					.map(lambda x:general_verificate(x))\
					.map(lambda x: str(x[0])+", Plain Text, Geo validated: Y coordinate of the incident location., " + str(x[1]))\
					.saveAsTextFile("25_y_coordinate.out")
		"""

	# Index[26]
	park_facility_name = line.map(lambda x: (x[26].encode('utf-8')))\
							.map(lambda x:general_verificate(x))\
							.map(lambda x: str(x[0])+", Plain Text, If the incident location is a Parks Dept facility, the Name of the facility will appear here, " + str(x[1]))\
							.saveAsTextFile("26_park_facility_name.out")

	# Index[27]
	park_borough = line.map(lambda x: (x[27].encode('utf-8')))\
							.map(lambda x:general_verificate(x))\
							.map(lambda x: str(x[0])+", Plain Text, The borough of incident if it is a Parks Dept facility, " + str(x[1]))\
							.saveAsTextFile("27_park_borough.out")

	# Index[28]
	school_name = line.map(lambda x: (x[28].encode('utf-8')))\
							.map(lambda x:general_verificate(x))\
							.map(lambda x: str(x[0])+", Plain Text, If the incident location is a Dept of Education school, the name of the school will appear in this field. If the incident is a Parks Dept facility its name will appear here, " + str(x[1]))\
							.saveAsTextFile("22_school_name.out")

	# Index[29] 
	school_number = line.map(lambda x: (x[29].encode('utf-8')))\
							.map(lambda x: general_verificate(x))\
							.map(lambda x: str(x[0])+", Plain Text, If the incident location is a Dept of Education school, the Number of the school will appear in this field. This field is also used for Parks Dept Facilities, " + str(x[1]))\

	
	# Index[30]
	def verificate_school_region(school_region):
		school_region = school_region.lower()
		
		if school_region == '' or school_region == 'na' or school_region == 'n/a' or school_region == 'unspecified':
			school_region = school_region, "NULL"
		
		else:
			if school_region.startswith("region"):
				school_region = school_region, "VALID"
			else:
				school_region = school_region, "INVALID"
		return school_region

	school_region = line.map(lambda x: (x[30].encode('utf-8')))\
							.map(lambda x:verificate_school_region(x))\
							.map(lambda x: str(x[0])+", Plain Text, If the incident location is a Dept of Education School, the school region number will be appear in this field, " + str(x[1]))\
							.saveAsTextFile("30_school_region.out")

	# Index[31]
	def verificate_school_code(school_code):
		school_code = school_code.lower()
		if school_code == '' or school_code == 'na' or school_code == 'n/a' or school_code == 'unspecified':
			school_code = school_code, "NULL"
		else:
			if (len(school_code) == 6):
				school_code = school_code, "VALID"
			else:
				school_code = school_code, "INVALID"
		return school_code

	school_code = line.map(lambda x: (x[31].encode('utf-8')))\
						.map(lambda x:verificate_school_code(x))\
						.map(lambda x: str(x[0])+", Plain Text, If the incident location is a Dept of Education School, the school code number will be appear in this field, " + str(x[1]))\
						.saveAsTextFile("31_school_code.out")

	# Index[32]
	def verificate_school_phone_number(school_phone_number):
		school_phone_number = school_phone_number.lower()
		if school_phone_number == '' or school_phone_number.startswith('na') or school_phone_number == 'n/a' or school_phone_number == 'na' or school_phone_number == 'unspecified':
			school_phone_number = school_phone_number, "NULL"
		else:
			if len(school_phone_number) == 10:
				try:
					school_phone_number = int(school_phone_number)
					school_phone_number = str(school_phone_number), "VALID"
				except ValueError:
					school_phone_number = str(school_phone_number), "INVALID"
			else:
				school_phone_number = school_phone_number, "INVALID"
		return school_phone_number

	school_phone_number = line.map(lambda x: (x[32].encode('utf-8')))\
							.map(lambda x:verificate_school_phone_number(x))\
							.map(lambda x: str(x[0])+", Plain Text, If the facility = Dept for the Aging or Parks Dept, the phone number will appear here. (note - Dept of Education facilities do not display phone number), " + str(x[1]))\
							.saveAsTextFile("32_school_phone_number.out")

	# Index[33]
	school_address = line.map(lambda x: (x[32].encode('utf-8')))\
							.map(lambda x:general_verificate(x))\
							.map(lambda x: str(x[0])+", Plain Text, Address of facility of incident location, if the facility is associated with Dept of Education, Dept for the Aging or Parks Dept, " + str(x[1]))\
							.saveAsTextFile("33_school_address.out")

	sc.stop()

