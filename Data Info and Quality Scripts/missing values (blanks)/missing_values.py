# This program count the missing values in each column
# If there should be missing values in a column, the result would be in the first part
# If there should not be missing values in a column, the first part generated is empty
# The missing value here only refers to blanks,
# other possibilities would be considered in other scripts.

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

	# Column index[0]
	unique_key = line.map(lambda x: (x[0].encode('utf-8'), 1))\
					.filter(lambda x: x[0]=='')\
					.reduceByKey(add)\
					.map(lambda x: str('number of missing values in unique_key:')+'\t'+str(x[1]))
	unique_key.saveAsTextFile("unique_key.out")

	# Column index[1]
	created_date = line.map(lambda x: (x[1].encode('utf-8'), 1))\
					.filter(lambda x: x[0]=='')\
					.reduceByKey(add)\
					.map(lambda x: str('number of missing values in created date:')+'\t'+str(x[1]))\
					.saveAsTextFile("created_date.out")

	# Column index[2]
	close_date = line.map(lambda x: (x[2].encode('utf-8'), 1))\
					.filter(lambda x: x[0]=='')\
					.reduceByKey(add)\
					.map(lambda x: str('number of missing values in close date:')+'\t'+str(x[1]))\
					.saveAsTextFile("close_date.out")

	# Column index[3]
	agency = line.map(lambda x: (x[3].encode('utf-8'), 1))\
					.filter(lambda x: x[0]=='')\
					.reduceByKey(add)\
					.map(lambda x: str('number of missing values in agency:')+'\t'+str(x[1]))\
					.saveAsTextFile("agency.out")

	# Column index[4]
	agency_name = line.map(lambda x: (x[4].encode('utf-8'), 1))\
					.filter(lambda x: x[0]=='')\
					.reduceByKey(add)\
					.map(lambda x: str('number of missing values in agency_name:')+'\t'+str(x[1]))\
					.saveAsTextFile("agency_name.out")

	# Column index[5]
	complaint_type = line.map(lambda x: (x[5].encode('utf-8'), 1))\
					.filter(lambda x: x[0]=='')\
					.reduceByKey(add)\
					.map(lambda x: str('number of missing values in complaint type:')+'\t'+str(x[1]))\
					.saveAsTextFile("complaint_type.out")

	# Column index[6]
	descriptor = line.map(lambda x: (x[6].encode('utf-8'), 1))\
					.filter(lambda x: x[0]=='')\
					.reduceByKey(add)\
					.map(lambda x: str('number of missing values in descriptor:')+'\t'+str(x[1]))\
					.saveAsTextFile("descriptor.out")

	# Column index[7]
	location_type = line.map(lambda x: (x[7].encode('utf-8'), 1))\
						.filter(lambda x: x[0]=='')\
						.reduceByKey(add)\
						.map(lambda x: str('number of missing values in location type:')+'\t'+str(x[1]))\
						.saveAsTextFile("location_type.out")

	# Column index[8]
	incident_zip = line.map(lambda x: (x[8].encode('utf-8'), 1))\
						.filter(lambda x: x[0]=='')\
						.reduceByKey(add)\
						.map(lambda x: str('number of missing values in incident zip:')+'\t'+str(x[1]))\
						.saveAsTextFile("incident_zip.out")

	# Column index[9]
	incident_address = line.map(lambda x: (x[9].encode('utf-8'), 1))\
							.filter(lambda x: x[0]=='')\
							.reduceByKey(add)\
							.map(lambda x: str('number of missing values in incident address:')+'\t'+str(x[1]))\
							.saveAsTextFile("incident_address.out")

	# Column index[10]
	street_name = line.map(lambda x: (x[10].encode('utf-8'), 1))\
						.filter(lambda x: x[0]=='')\
						.reduceByKey(add)\
						.map(lambda x: str('number of missing values in street name:')+'\t'+str(x[1]))\
						.saveAsTextFile("street_name.out")

	# Column index[11]
	cross_st_01 = line.map(lambda x: (x[11].encode('utf-8'), 1))\
						.filter(lambda x: x[0]=='')\
						.reduceByKey(add)\
						.map(lambda x: str('number of missing values in cross street 1:')+'\t'+str(x[1]))\
						.saveAsTextFile("cross_st_01.out")

	# Column index[12]
	cross_st_02 = line.map(lambda x: (x[12].encode('utf-8'), 1))\
						.filter(lambda x: x[0]=='')\
						.reduceByKey(add)\
						.map(lambda x: str('number of missing values in cross street 2:')+'\t'+str(x[1]))\
						.saveAsTextFile("cross_st_02.out")

	# Column index[13]
	intersection_st_01 = line.map(lambda x: (x[13].encode('utf-8'), 1))\
							.filter(lambda x: x[0]=='')\
							.reduceByKey(add)\
							.map(lambda x: str('number of missing values in intersection street 1:')+'\t'+str(x[1]))\
							.saveAsTextFile("intersection_st_01.out")

	# Column index[14]
	intersection_st_02 = line.map(lambda x: (x[14].encode('utf-8'), 1))\
							.filter(lambda x: x[0]=='')\
							.reduceByKey(add)\
							.map(lambda x: str('number of missing values in intersection street 2:')+'\t'+str(x[1]))\
							.saveAsTextFile("intersection_st_02.out")

	# Column index[15]
	address_type = line.map(lambda x: (x[15].encode('utf-8'), 1))\
							.filter(lambda x: x[0]=='')\
							.reduceByKey(add)\
							.map(lambda x: str('number of missing values in address type:')+'\t'+str(x[1]))\
							.saveAsTextFile("address_type.out")

	# Column index[16]
	city = line.map(lambda x: (x[16].encode('utf-8'), 1))\
				.filter(lambda x: x[0]=='')\
				.reduceByKey(add)\
				.map(lambda x: str('number of missing values in city:')+'\t'+str(x[1]))\
				.saveAsTextFile("city.out")

	# Column index[17]
	landmark = line.map(lambda x: (x[17].encode('utf-8'), 1))\
					.filter(lambda x: x[0]=='')\
					.reduceByKey(add)\
					.map(lambda x: str('number of missing values in landmark:')+'\t'+str(x[1]))\
					.saveAsTextFile("landmark.out")

	# Column index[18]
	facility_type = line.map(lambda x: (x[18].encode('utf-8'), 1))\
						.filter(lambda x: x[0]=='')\
						.reduceByKey(add)\
						.map(lambda x: str('number of missing values in facility type:')+'\t'+str(x[1]))\
						.saveAsTextFile("facility_type.out")

	# Column index[19]
	status = line.map(lambda x: (x[19].encode('utf-8'), 1))\
					.filter(lambda x: x[0]=='')\
					.reduceByKey(add)\
					.map(lambda x: str('number of missing values in status:')+'\t'+str(x[1]))\
					.saveAsTextFile("status.out")

	# Column index[20]
	due_date = line.map(lambda x: (x[20].encode('utf-8'), 1))\
					.filter(lambda x: x[0]=='')\
					.reduceByKey(add)\
					.map(lambda x: str('number of missing values in due date:')+'\t'+str(x[1]))\
					.saveAsTextFile("due date.out")

	# Column index[21]
	resolution_description = line.map(lambda x: (x[21].encode('utf-8'), 1))\
								.filter(lambda x: x[0]=='')\
								.reduceByKey(add)\
								.map(lambda x: str('number of missing values in resolution_description:')+'\t'+str(x[1]))\
								.saveAsTextFile("resolution_description.out")

	# Column index[22]
	resolution_action_update_date = line.map(lambda x: (x[22].encode('utf-8'), 1))\
										.filter(lambda x: x[0]=='')\
										.reduceByKey(add)\
										.map(lambda x: str('number of missing values in resolution_action_update_date:')+'\t'+str(x[1]))\
										.saveAsTextFile("resolution_action_update_date.out")

	# Column index[23]
	community_board = line.map(lambda x: (x[23].encode('utf-8'), 1))\
							.filter(lambda x: x[0]=='')\
							.reduceByKey(add)\
							.map(lambda x: str('number of missing values in community_board:')+'\t'+str(x[1]))\
							.saveAsTextFile("community_board.out")

	# Column index[24]
	borough = line.map(lambda x: (x[24].encode('utf-8'), 1))\
					.filter(lambda x: x[0]=='')\
					.reduceByKey(add)\
					.map(lambda x: str('number of missing values in borough:')+'\t'+str(x[1]))\
					.saveAsTextFile("borough.out")

	# Column index[25]
	x_coordinate_state_plane = line.map(lambda x: (x[25].encode('utf-8'), 1))\
									.filter(lambda x: x[0]=='')\
									.reduceByKey(add)\
									.map(lambda x: str('number of missing values in x coordinate state plane:')+'\t'+str(x[1]))\
									.saveAsTextFile("x_coordinate_state_plane.out")

	# Column index[26]
	y_coordinate_state_plane = line.map(lambda x: (x[26].encode('utf-8'), 1))\
									.filter(lambda x: x[0]=='')\
									.reduceByKey(add)\
									.map(lambda x: str('number of missing values in y coordinate state plane:')+'\t'+str(x[1]))\
									.saveAsTextFile("y coordinate state plane.out")

	# Column index[27]
	park_facility_name = line.map(lambda x: (x[27].encode('utf-8'), 1))\
								.filter(lambda x: x[0]=='')\
								.reduceByKey(add)\
								.map(lambda x: str('number of missing values in park facility name:')+'\t'+str(x[1]))\
								.saveAsTextFile("park_facility_name.out")

	# Column index[28]
	park_borough = line.map(lambda x: (x[28].encode('utf-8'), 1))\
						.filter(lambda x: x[0]=='')\
						.reduceByKey(add)\
						.map(lambda x: str('number of missing values in park borough:')+'\t'+str(x[1]))\
						.saveAsTextFile("park_borough.out")

	# Column index[29]
	school_name = line.map(lambda x: (x[29].encode('utf-8'), 1))\
						.filter(lambda x: x[0]=='')\
						.reduceByKey(add)\
						.map(lambda x: str('number of missing values in school name:')+'\t'+str(x[1]))\
						.saveAsTextFile("school_name.out")

	# Column index[30]
	school_number = line.map(lambda x: (x[30].encode('utf-8'), 1))\
						.filter(lambda x: x[0]=='')\
						.reduceByKey(add)\
						.map(lambda x: str('number of missing values in school number:')+'\t'+str(x[1]))\
						.saveAsTextFile("school_number.out")

	# Column index[31]
	school_region = line.map(lambda x: (x[31].encode('utf-8'), 1))\
					.filter(lambda x: x[0]=='')\
					.reduceByKey(add)\
					.map(lambda x: str('number of missing values in school region:')+'\t'+str(x[1]))\
					.saveAsTextFile("school_region.out")

	# Column index[32]
	school_code = line.map(lambda x: (x[32].encode('utf-8'), 1))\
					.filter(lambda x: x[0]=='')\
					.reduceByKey(add)\
					.map(lambda x: str('number of missing values in school code:')+'\t'+str(x[1]))\
					.saveAsTextFile("school_code.out")

	# Column index[33]
	school_phone_number = line.map(lambda x: (x[33].encode('utf-8'), 1))\
							.filter(lambda x: x[0]=='')\
							.reduceByKey(add)\
							.map(lambda x: str('number of missing values in school phone number:')+'\t'+str(x[1]))\
							.saveAsTextFile("school_phone_number.out")

	# Column index[34]
	school_address = line.map(lambda x: (x[34].encode('utf-8'), 1))\
						.filter(lambda x: x[0]=='')\
						.reduceByKey(add)\
						.map(lambda x: str('number of missing values in school address:')+'\t'+str(x[1]))\
						.saveAsTextFile("school_address.out")

	# Column index[35]
	school_city = line.map(lambda x: (x[35].encode('utf-8'), 1))\
					.filter(lambda x: x[0]=='')\
					.reduceByKey(add)\
					.map(lambda x: str('number of missing values in school city:')+'\t'+str(x[1]))\
					.saveAsTextFile("school_city.out")

	# Column index[36]
	school_state = line.map(lambda x: (x[36].encode('utf-8'), 1))\
					.filter(lambda x: x[0]=='')\
					.reduceByKey(add)\
					.map(lambda x: str('number of missing values in school state:')+'\t'+str(x[1]))\
					.saveAsTextFile("school_state.out")

	# Column index[37]
	school_zip = line.map(lambda x: (x[37].encode('utf-8'), 1))\
					.filter(lambda x: x[0]=='')\
					.reduceByKey(add)\
					.map(lambda x: str('number of missing values in school zip:')+'\t'+str(x[1]))\
					.saveAsTextFile("school_state.out")

	# Column index[38]
	school_not_found = line.map(lambda x: (x[38].encode('utf-8'), 1))\
							.filter(lambda x: x[0]=='')\
							.reduceByKey(add)\
							.map(lambda x: str('number of missing values in school not found:')+'\t'+str(x[1]))\
							.saveAsTextFile("school_not_found.out")

	# Column index[39]
	school_or_citywide_complaint = line.map(lambda x: (x[39].encode('utf-8'), 1))\
										.filter(lambda x: x[0]=='')\
										.reduceByKey(add)\
										.map(lambda x: str('number of missing values in school or citywide complaint:')+'\t'+str(x[1]))\
										.saveAsTextFile("school_or_citywide_complaint.out")

	# Column index[40]
	vehicle_type = line.map(lambda x: (x[40].encode('utf-8'), 1))\
						.filter(lambda x: x[0]=='')\
						.reduceByKey(add)\
						.map(lambda x: str('number of missing values in school or vehicle type:')+'\t'+str(x[1]))\
						.saveAsTextFile("vehicle_type.out")

	# Column index[41]
	taxi_company_borough = line.map(lambda x: (x[41].encode('utf-8'), 1))\
							.filter(lambda x: x[0]=='')\
							.reduceByKey(add)\
							.map(lambda x: str('number of missing values in taxi company borough:')+'\t'+str(x[1]))\
							.saveAsTextFile("taxi_company_borough.out")

	# Column index[42]
	taxi_pick_up_location = line.map(lambda x: (x[42].encode('utf-8'), 1))\
							.filter(lambda x: x[0]=='')\
							.reduceByKey(add)\
							.map(lambda x: str('number of missing values in taxi pick up location:')+'\t'+str(x[1]))\
							.saveAsTextFile("taxi_pick_up_location.out")

	# Column index[43]
	bridge_highway_name = line.map(lambda x: (x[43].encode('utf-8'), 1))\
							.filter(lambda x: x[0]=='')\
							.reduceByKey(add)\
							.map(lambda x: str('number of missing values in bridge highway name:')+'\t'+str(x[1]))\
							.saveAsTextFile("bridge_highway_name.out")

	# Column index[44]
	bridge_highway_direction = line.map(lambda x: (x[44].encode('utf-8'), 1))\
								.filter(lambda x: x[0]=='')\
								.reduceByKey(add)\
								.map(lambda x: str('number of missing values in bridge highway direction:')+'\t'+str(x[1]))\
								.saveAsTextFile("bridge_highway_direction.out")

	# Column index[45]
	road_ramp = line.map(lambda x: (x[45].encode('utf-8'), 1))\
					.filter(lambda x: x[0]=='')\
					.reduceByKey(add)\
					.map(lambda x: str('number of missing values in road ramp:')+'\t'+str(x[1]))\
					.saveAsTextFile("road_ramp.out")

	# Column index[46]
	bridge_highway_segment = line.map(lambda x: (x[46].encode('utf-8'), 1))\
								.filter(lambda x: x[0]=='')\
								.reduceByKey(add)\
								.map(lambda x: str('number of missing values in bridge highway segment:')+'\t'+str(x[1]))\
								.saveAsTextFile("bridge_highway_segment.out")

	# Column index[47]
	garage_lot_name = line.map(lambda x: (x[47].encode('utf-8'), 1))\
								.filter(lambda x: x[0]=='')\
								.reduceByKey(add)\
								.map(lambda x: str('number of missing values in garage lot name:')+'\t'+str(x[1]))\
								.saveAsTextFile("garage_lot_name.out")

	# Column index[48]
	ferry_direction = line.map(lambda x: (x[48].encode('utf-8'), 1))\
								.filter(lambda x: x[0]=='')\
								.reduceByKey(add)\
								.map(lambda x: str('number of missing values in ferry direction:')+'\t'+str(x[1]))\
								.saveAsTextFile("ferry_direction.out")

	# Column index[49]
	ferry_terminal_name = line.map(lambda x: (x[49].encode('utf-8'), 1))\
								.filter(lambda x: x[0]=='')\
								.reduceByKey(add)\
								.map(lambda x: str('number of missing values in ferry terminal name:')+'\t'+str(x[1]))\
								.saveAsTextFile("ferry_terminal_name.out")

	# Column index[50]
	latitude = line.map(lambda x: (x[50].encode('utf-8'), 1))\
					.filter(lambda x: x[0]=='')\
					.reduceByKey(add)\
					.map(lambda x: str('number of missing values in latitude:')+'\t'+str(x[1]))\
					.saveAsTextFile("latitude.out")

	# Column index[51]
	longitude = line.map(lambda x: (x[51].encode('utf-8'), 1))\
								.filter(lambda x: x[0]=='')\
								.reduceByKey(add)\
								.map(lambda x: str('number of missing values in garage lot name:')+'\t'+str(x[1]))\
								.saveAsTextFile("garage_lot_name.out")

	# Column index[52]
	location = line.map(lambda x: (x[52].encode('utf-8'), 1))\
					.filter(lambda x: x[0]=='')\
					.reduceByKey(add)\
					.map(lambda x: str('number of missing values in location:')+'\t'+str(x[1]))\
					.saveAsTextFile("location.out")