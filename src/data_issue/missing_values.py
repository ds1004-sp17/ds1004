# This program count the missing values in each column
# The missing values contain '' 'N/A' 'NA' and 'Unspecified'

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


	# Column index[0]
	unique_key = line.map(lambda x: (x[0].encode('utf-8')))\
					.map(lambda x: (check_missing_values(x), 1))\
					.reduceByKey(add)\
					.map(lambda x: x[0]+'\t'+str(x[1]))\
					.saveAsTextFile("mv_0.out")

	# Column index[1]
	created_date = line.map(lambda x: (x[1].encode('utf-8')))\
					.map(lambda x: (check_missing_values(x), 1))\
					.reduceByKey(add)\
					.map(lambda x: x[0]+'\t'+str(x[1]))\
					.saveAsTextFile("mv_1.out")

	# Column index[2]
	close_date = line.map(lambda x: (x[2].encode('utf-8')))\
					.map(lambda x: (check_missing_values(x), 1))\
					.reduceByKey(add)\
					.map(lambda x: x[0]+'\t'+str(x[1]))\
					.saveAsTextFile("mv_2.out")

	# Column index[3]
	agency = line.map(lambda x: (x[3].encode('utf-8')))\
					.map(lambda x: (check_missing_values(x), 1))\
					.reduceByKey(add)\
					.map(lambda x: x[0]+'\t'+str(x[1]))\
					.saveAsTextFile("mv_3.out")

	# Column index[4]
	agency_name = line.map(lambda x: (x[4].encode('utf-8')))\
					.map(lambda x: (check_missing_values(x), 1))\
					.reduceByKey(add)\
					.map(lambda x: x[0]+'\t'+str(x[1]))\
					.saveAsTextFile("mv_4.out")

	# Column index[5]
	complaint_type = line.map(lambda x: (x[5].encode('utf-8')))\
					.map(lambda x: (check_missing_values(x), 1))\
					.reduceByKey(add)\
					.map(lambda x: x[0]+'\t'+str(x[1]))\
					.saveAsTextFile("mv_5.out")
	
	# Column index[6]
	descriptor = line.map(lambda x: (x[6].encode('utf-8')))\
					.map(lambda x: (check_missing_values(x), 1))\
					.reduceByKey(add)\
					.map(lambda x: x[0]+'\t'+str(x[1]))\
					.saveAsTextFile("mv_6.out")

	# Column index[7]
	location_type = line.map(lambda x: (x[7].encode('utf-8')))\
						.map(lambda x: (check_missing_values(x), 1))\
						.reduceByKey(add)\
						.map(lambda x: x[0]+'\t'+str(x[1]))\
						.saveAsTextFile("mv_7.out")

	# Column index[8]
	incident_zip = line.map(lambda x: (x[8].encode('utf-8')))\
						.map(lambda x: (check_missing_values(x), 1))\
						.reduceByKey(add)\
						.map(lambda x: x[0]+'\t'+str(x[1]))\
						.saveAsTextFile("mv_8.out")

	# Column index[9]
	incident_address = line.map(lambda x: (x[9].encode('utf-8')))\
							.map(lambda x: (check_missing_values(x), 1))\
							.reduceByKey(add)\
							.map(lambda x: x[0]+'\t'+str(x[1]))\
							.saveAsTextFile("mv_9.out")

	# Column index[10]
	street_name = line.map(lambda x: (x[10].encode('utf-8')))\
						.map(lambda x: (check_missing_values(x), 1))\
						.reduceByKey(add)\
						.map(lambda x: x[0]+'\t'+str(x[1]))\
						.saveAsTextFile("mv_10.out")

	# Column index[11]
	cross_st_01 = line.map(lambda x: (x[11].encode('utf-8')))\
						.map(lambda x: (check_missing_values(x), 1))\
						.reduceByKey(add)\
						.map(lambda x: x[0]+'\t'+str(x[1]))\
						.saveAsTextFile("mv_11.out")

	# Column index[12]
	cross_st_02 = line.map(lambda x: (x[12].encode('utf-8')))\
						.map(lambda x: (check_missing_values(x), 1))\
						.reduceByKey(add)\
						.map(lambda x: x[0]+'\t'+str(x[1]))\
						.saveAsTextFile("mv_12.out")

	# Column index[13]
	intersection_st_01 = line.map(lambda x: (x[13].encode('utf-8')))\
							.map(lambda x: (check_missing_values(x), 1))\
							.reduceByKey(add)\
							.map(lambda x: x[0]+'\t'+str(x[1]))\
							.saveAsTextFile("mv_13.out")

	# Column index[14]
	intersection_st_02 = line.map(lambda x: (x[14].encode('utf-8')))\
							.map(lambda x: (check_missing_values(x), 1))\
							.reduceByKey(add)\
							.map(lambda x: x[0]+'\t'+str(x[1]))\
							.saveAsTextFile("mv_14.out")
	
	# Column index[15]
	address_type = line.map(lambda x: (x[15].encode('utf-8')))\
							.map(lambda x: (check_missing_values(x), 1))\
							.reduceByKey(add)\
							.map(lambda x: x[0]+'\t'+str(x[1]))\
							.saveAsTextFile("mv_15.out")

	# Column index[16]
	city = line.map(lambda x: (x[16].encode('utf-8')))\
				.map(lambda x: (check_missing_values(x), 1))\
				.reduceByKey(add)\
				.map(lambda x: x[0]+'\t'+str(x[1]))\
				.saveAsTextFile("mv_16.out")

	# Column index[17]
	landmark = line.map(lambda x: (x[17].encode('utf-8')))\
					.map(lambda x: (check_missing_values(x), 1))\
					.reduceByKey(add)\
					.map(lambda x: x[0]+'\t'+str(x[1]))\
					.saveAsTextFile("mv_17.out")

	# Column index[18]
	facility_type = line.map(lambda x: (x[18].encode('utf-8'),))\
						.map(lambda x: (check_missing_values(x), 1))\
						.reduceByKey(add)\
						.map(lambda x: x[0]+'\t'+str(x[1]))\
						.saveAsTextFile("mv_18.out")

	# Column index[19]
	status = line.map(lambda x: (x[19].encode('utf-8'),))\
					.map(lambda x: (check_missing_values(x), 1))\
					.reduceByKey(add)\
					.map(lambda x: x[0]+'\t'+str(x[1]))\
					.saveAsTextFile("mv_19.out")

	# Column index[20]
	due_date = line.map(lambda x: (x[20].encode('utf-8')))\
					.map(lambda x: (check_missing_values(x), 1))\
					.reduceByKey(add)\
					.map(lambda x: x[0]+'\t'+str(x[1]))\
					.saveAsTextFile("mv_20.out")

	# Column index[21]
	resolution_action_update_date = line.map(lambda x: (x[21].encode('utf-8')))\
										.map(lambda x: (check_missing_values(x), 1))\
										.reduceByKey(add)\
										.map(lambda x: x[0]+'\t'+str(x[1]))\
										.saveAsTextFile("mv_21.out")

	# Column index[22]
	community_board = line.map(lambda x: (x[22].encode('utf-8')))\
							.map(lambda x: (check_missing_values(x), 1))\
							.reduceByKey(add)\
							.map(lambda x: x[0]+'\t'+str(x[1]))\
							.saveAsTextFile("mv_22.out")

	# Column index[23]
	borough = line.map(lambda x: (x[23].encode('utf-8')))\
					.map(lambda x: (check_missing_values(x), 1))\
					.reduceByKey(add)\
					.map(lambda x: x[0]+'\t'+str(x[1]))\
					.saveAsTextFile("mv_23.out")

	# Column index[24]
	x_coordinate_state_plane = line.map(lambda x: (x[24].encode('utf-8')))\
									.map(lambda x: (check_missing_values(x), 1))\
									.reduceByKey(add)\
									.map(lambda x: x[0]+'\t'+str(x[1]))\
									.saveAsTextFile("mv_24.out")

	# Column index[25]
	y_coordinate_state_plane = line.map(lambda x: (x[25].encode('utf-8')))\
									.map(lambda x: (check_missing_values(x), 1))\
									.reduceByKey(add)\
									.map(lambda x: x[0]+'\t'+str(x[1]))\
									.saveAsTextFile("mv_25.out")

	# Column index[26]
	park_facility_name = line.map(lambda x: (x[26].encode('utf-8')))\
								.map(lambda x: (check_missing_values(x), 1))\
								.reduceByKey(add)\
								.map(lambda x: x[0]+'\t'+str(x[1]))\
								.saveAsTextFile("mv_26.out")

	# Column index[27]
	park_borough = line.map(lambda x: (x[27].encode('utf-8')))\
						.map(lambda x: (check_missing_values(x), 1))\
						.reduceByKey(add)\
						.map(lambda x: x[0]+'\t'+str(x[1]))\
						.saveAsTextFile("mv_27.out")

	# Column index[28]
	school_name = line.map(lambda x: (x[28].encode('utf-8')))\
						.map(lambda x: (check_missing_values(x), 1))\
						.reduceByKey(add)\
						.map(lambda x: x[0]+'\t'+str(x[1]))\
						.saveAsTextFile("mv_28.out")

	# Column index[29]
	school_number = line.map(lambda x: (x[29].encode('utf-8')))\
						.map(lambda x: (check_missing_values(x), 1))\
						.reduceByKey(add)\
						.map(lambda x: x[0]+'\t'+str(x[1]))\
						.saveAsTextFile("mv_29.out")

	# Column index[30]
	school_region = line.map(lambda x: (x[30].encode('utf-8')))\
					.map(lambda x: (check_missing_values(x), 1))\
					.reduceByKey(add)\
					.map(lambda x: x[0]+'\t'+str(x[1]))\
					.saveAsTextFile("mv_30.out")

	# Column index[31]
	school_code = line.map(lambda x: (x[31].encode('utf-8')))\
					.map(lambda x: (check_missing_values(x), 1))\
					.reduceByKey(add)\
					.map(lambda x: x[0]+'\t'+str(x[1]))\
					.saveAsTextFile("mv_31.out")

	# Column index[32]
	school_phone_number = line.map(lambda x: (x[32].encode('utf-8')))\
							.map(lambda x: (check_missing_values(x), 1))\
							.reduceByKey(add)\
							.map(lambda x: x[0]+'\t'+str(x[1]))\
							.saveAsTextFile("mv_32.out")

	# Column index[33]
	school_address = line.map(lambda x: (x[33].encode('utf-8')))\
						.map(lambda x: (check_missing_values(x), 1))\
						.reduceByKey(add)\
						.map(lambda x: x[0]+'\t'+str(x[1]))\
						.saveAsTextFile("mv_33.out")

	# Column index[34]
	school_city = line.map(lambda x: (x[34].encode('utf-8')))\
					.map(lambda x: (check_missing_values(x), 1))\
					.reduceByKey(add)\
					.map(lambda x: x[0]+'\t'+str(x[1]))\
					.saveAsTextFile("mv_34.out")

	# Column index[35]
	school_state = line.map(lambda x: (x[35].encode('utf-8')))\
					.map(lambda x: (check_missing_values(x), 1))\
					.reduceByKey(add)\
					.map(lambda x: x[0]+'\t'+str(x[1]))\
					.saveAsTextFile("mv_35.out")

	# Column index[36]
	school_zip = line.map(lambda x: (x[36].encode('utf-8')))\
					.map(lambda x: (check_missing_values(x), 1))\
					.reduceByKey(add)\
					.map(lambda x: x[0]+'\t'+str(x[1]))\
					.saveAsTextFile("mv_36.out")

	# Column index[37]
	school_not_found = line.map(lambda x: (x[37].encode('utf-8')))\
							.map(lambda x: (check_missing_values(x), 1))\
							.reduceByKey(add)\
							.map(lambda x: x[0]+'\t'+str(x[1]))\
							.saveAsTextFile("mv_37.out")

	# Column index[38]
	school_or_citywide_complaint = line.map(lambda x: (x[38].encode('utf-8')))\
										.map(lambda x: (check_missing_values(x), 1))\
										.reduceByKey(add)\
										.map(lambda x: x[0]+'\t'+str(x[1]))\
										.saveAsTextFile("mv_38.out")

	# Column index[39]
	vehicle_type = line.map(lambda x: (x[39].encode('utf-8')))\
						.map(lambda x: (check_missing_values(x), 1))\
						.reduceByKey(add)\
						.map(lambda x: x[0]+'\t'+str(x[1]))\
						.saveAsTextFile("mv_39.out")

	# Column index[40]
	taxi_company_borough = line.map(lambda x: (x[40].encode('utf-8')))\
							.map(lambda x: (check_missing_values(x), 1))\
							.reduceByKey(add)\
							.map(lambda x: x[0]+'\t'+str(x[1]))\
							.saveAsTextFile("mv_40.out")

	# Column index[41]
	taxi_pick_up_location = line.map(lambda x: (x[41].encode('utf-8')))\
							.map(lambda x: (check_missing_values(x), 1))\
							.reduceByKey(add)\
							.map(lambda x: x[0]+'\t'+str(x[1]))\
							.saveAsTextFile("mv_41.out")

	# Column index[42]
	bridge_highway_name = line.map(lambda x: (x[42].encode('utf-8')))\
							.map(lambda x: (check_missing_values(x), 1))\
							.reduceByKey(add)\
							.map(lambda x: x[0]+'\t'+str(x[1]))\
							.saveAsTextFile("mv_42.out")

	# Column index[43]
	bridge_highway_direction = line.map(lambda x: (x[43].encode('utf-8')))\
								.map(lambda x: (check_missing_values(x), 1))\
								.reduceByKey(add)\
								.map(lambda x: x[0]+'\t'+str(x[1]))\
								.saveAsTextFile("mv_43.out")

	# Column index[44]
	road_ramp = line.map(lambda x: (x[44].encode('utf-8')))\
					.map(lambda x: (check_missing_values(x), 1))\
					.reduceByKey(add)\
					.map(lambda x: x[0]+'\t'+str(x[1]))\
					.saveAsTextFile("mv_44.out")

	# Column index[45]
	bridge_highway_segment = line.map(lambda x: (x[45].encode('utf-8')))\
								.map(lambda x: (check_missing_values(x), 1))\
								.reduceByKey(add)\
								.map(lambda x: x[0]+'\t'+str(x[1]))\
								.saveAsTextFile("mv_45.out")

	# Column index[46]
	garage_lot_name = line.map(lambda x: (x[46].encode('utf-8')))\
								.map(lambda x: (check_missing_values(x), 1))\
								.reduceByKey(add)\
								.map(lambda x: x[0]+'\t'+str(x[1]))\
								.saveAsTextFile("mv_46.out")

	# Column index[47]
	ferry_direction = line.map(lambda x: (x[47].encode('utf-8')))\
								.map(lambda x: (check_missing_values(x), 1))\
								.reduceByKey(add)\
								.map(lambda x: x[0]+'\t'+str(x[1]))\
								.saveAsTextFile("mv_47.out")

	# Column index[48]
	ferry_terminal_name = line.map(lambda x: (x[48].encode('utf-8')))\
								.map(lambda x: (check_missing_values(x), 1))\
								.reduceByKey(add)\
								.map(lambda x: x[0]+'\t'+str(x[1]))\
								.saveAsTextFile("mv_48.out")

	# Column index[49]
	latitude = line.map(lambda x: (x[49].encode('utf-8')))\
					.map(lambda x: (check_missing_values(x), 1))\
					.reduceByKey(add)\
					.map(lambda x: x[0]+'\t'+str(x[1]))\
					.saveAsTextFile("mv_49.out")

	# Column index[50]
	longitude = line.map(lambda x: (x[50].encode('utf-8')))\
								.map(lambda x: (check_missing_values(x), 1))\
								.reduceByKey(add)\
								.map(lambda x: x[0]+'\t'+str(x[1]))\
								.saveAsTextFile("mv_50.out")

	# Column index[51]
	location = line.map(lambda x: (x[51].encode('utf-8')))\
					.map(lambda x: (check_missing_values(x), 1))\
					.reduceByKey(add)\
					.map(lambda x: x[0]+'\t'+str(x[1]))\
					.saveAsTextFile("mv_51.out")
	
	sc.stop()