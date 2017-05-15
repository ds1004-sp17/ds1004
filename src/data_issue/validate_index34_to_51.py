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
	line = lines.map(lambda x:(x.encode('ascii','ignore')))\
				.mapPartitions(lambda x: (reader(x, delimiter = ',', quotechar = '"')))

	def general_verificate(contents):
		contents = contents.lower()
		
		if contents == '' or contents == 'na' or contents == 'n/a' or contents == 'unspecified':
			contents = contents, "NULL"
		else:
			contents = contents, "VALID"

		return contents

	# Index[34]
	school_city = line.map(lambda x: (x[34].encode('utf-8')))\
					.map(lambda x:general_verificate(x))\
					.map(lambda x: str(x[0])+", Plain Text, City of facilities incident location, if the facility is associated with Dept of Education, Dept for the Aging or Parks Dept, " + str(x[1]))\
					.saveAsTextFile("valid_34.out")

	# Index[35]
	school_state = line.map(lambda x: (x[35].encode('utf-8')))\
					.map(lambda x:general_verificate(x))\
					.map(lambda x: str(x[0])+", Plain Text, State of facility incident location, if the facility is associated with Dept of Education, Dept for the Aging or Parks Dept, " + str(x[1]))\
					.saveAsTextFile("valid_35.out")

	# Index[36]
	school_zip = line.map(lambda x: (x[36].encode('utf-8')))\
					.map(lambda x:general_verificate(x))\
					.map(lambda x: str(x[0])+", Plain Text, Zip of facility incident location, if the facility is associated with Dept of Education, Dept for the Aging or Parks Dept, " + str(x[1]))\
					.saveAsTextFile("valid_36.out")

	# Index[37]
	def verficate_school_not_found(school_not_found):
		school_not_found = school_not_found.lower()
		if school_not_found == '' or school_not_found == 'n/a' or school_not_found == 'na' or school_not_found == 'unspecified':
			school_not_found = school_not_found, "NULL"
		else:
			if school_not_found == 'n' or school_not_found == 'y':
				school_not_found = school_not_found, "VALID"
			else:
				school_not_found = school_not_found, "INVALID"
		return school_not_found

	school_not_found = line.map(lambda x: (x[37].encode('utf-8')))\
							.map(lambda x: verficate_school_not_found(x))\
							.map(lambda x: str(x[0])+", Plain Text, Y' in this field indicates the facility was not found, " + str(x[1]))\
							.saveAsTextFile("valid_37.out")

	# Index[38]
	school_or_citywide_complaint = line.map(lambda x: (x[38].encode('utf-8')))\
									.map(lambda x: general_verificate(x))\
									.map(lambda x: str(x[0])+", Plain Text, If the incident is about a Dept of Education facility, this field will indicate if the complaint is about a particualr school or a citywide issue, " + str(x[1]))\
									.saveAsTextFile("valid_38.out")

	# Index[39]
	vehicle_type = line.map(lambda x: (x[39].encode('utf-8')))\
					.map(lambda x: general_verificate(x))\
					.map(lambda x: str(x[0])+", Plain Text, If the incident is a taxi, this field describes the type of TLC vehicle, " + str(x[1]))\
					.saveAsTextFile("valid_39.out")

	# Index[40]
	taxi_company_borough = line.map(lambda x: (x[40].encode('utf-8')))\
							.map(lambda x: general_verificate(x))\
							.map(lambda x: str(x[0])+", Plain Text, If the incident is identified as a taxi, this field will display the borough of the taxi company, " + str(x[1]))\
							.saveAsTextFile("valid_40.out")

	# Index[41]
	taxi_pick_up_location = line.map(lambda x: (x[41].encode('utf-8')))\
								.map(lambda x: general_verificate(x))\
								.map(lambda x: str(x[0])+", Plain Text, If the incident is identified as a taxi, this field displays the taxi pick up location, " + str(x[1]))\
								.saveAsTextFile("valid_41.out")

	# Index[42]
	bridge_highway_name = line.map(lambda x: (x[42].encode('utf-8')))\
								.map(lambda x: general_verificate(x))\
								.map(lambda x: str(x[0])+ ", Plain Text, If the incident is identified as a Bridge/Highway, the name will be displayed here, " + str(x[1]))\
								.saveAsTextFile("valid_42.out")

	# Index[43]
	bridge_highway_direction = line.map(lambda x: (x[43].encode('utf-8')))\
									.map(lambda x: general_verificate(x))\
									.map(lambda x: str(x[0])+", Plain Text, If the incident is identified as a Bridge/Highway, the direction where the issue took place would be displayed here, " + str(x[1]))\
									.saveAsTextFile("valid_43.out")

	# Index[44]
	road_ramp = line.map(lambda x: (x[44].encode('utf-8')))\
					.map(lambda x: general_verificate(x))\
					.map(lambda x: str(x[0])+", Plain Text, If the incident location was Bridge/Highway this column differentiates if the issue was on the Road or the Ramp, " + str(x[1]))\
					.saveAsTextFile("valid_44.out")

	# Index[45]
	bridge_highway_segment = line.map(lambda x: (x[45].encode('utf-8')))\
								.map(lambda x: general_verificate(x))\
								.map(lambda x: str(x[0])+", Plain Text, Additional information on the section of the Bridge/Highway were the incident took place, " + str(x[1]))\
								.saveAsTextFile("valid_45.out")

	# Index[46]
	garage_lot_name = line.map(lambda x: (x[46].encode('utf-8')))\
							.map(lambda x: general_verificate(x))\
							.map(lambda x: str(x[0])+", Plain Text, Related to DOT Parking Meter SR, this field shows what garage lot the meter is located in, " + str(x[1]))\
							.saveAsTextFile("valid_46.out")

	# Index[47]
	ferry_direction = line.map(lambda x: (x[47].encode('utf-8')))\
							.map(lambda x: general_verificate(x))\
							.map(lambda x: str(x[0])+", Plain Text, Used when the incident location is within a Ferry, this field indicates the direction of ferry, " + str(x[1]))\
							.saveAsTextFile("valid_47.out")

	# Index[48]
	ferry_terminal_name = line.map(lambda x: (x[48].encode('utf-8')))\
							.map(lambda x: general_verificate(x))\
							.map(lambda x: str(x[0])+", Plain Text, Used when the incident location is Ferry, this field indicates the ferry terminal where the incident took place., " + str(x[1]))\
							.saveAsTextFile("valid_48.out")

	# Index[49]
	latitude = line.map(lambda x: (x[49].encode('utf-8')))\
					.map(lambda x:general_verificate(x))\
					.map(lambda x: str(x[0])+", Plain Text, Geo based Lat of the incident location, " + str(x[1]))\
					.saveAsTextFile("valid_49.out")

	# Index[50]
	longitude = line.map(lambda x: (x[50].encode('utf-8')))\
					.map(lambda x: general_verificate(x))\
					.map(lambda x: str(x[0])+", Plain Text, Geo based Long of the incident location, " + str(x[1]))\
					.saveAsTextFile("valid_50.out")

	# Index[51]
	location = line.map(lambda x: (x[51].encode('utf-8')))\
					.map(lambda x: general_verificate(x))\
					.map(lambda x: str(x[0])+", Plain Text, Combination of the geo based lat & long of the incident location, " + str(x[1]))\
					.saveAsTextFile("valid_51.out")


	sc.stop()