import csv
from collections import defaultdict


data_path = "/Users/abhishekkadian/Documents/Github/uber-tlc-foil-response/uber-trip-data/uber-janjune-sample.csv"
output_date_trips_path = "uber_janjune_count_trips.csv"
output_date_location_trips_path = "uber_janjune_location_count_trips.csv"

date_trip_counter = defaultdict(int)
location_date_trip_counter = defaultdict(int)

with open(data_path, "rb") as datafile,\
     open(output_date_trips_path, "wb") as trip_count_file,\
     open(output_date_location_trips_path, "wb") as location_trip_count_file:
    datareader = csv.reader(datafile, delimiter=",")
    for i, row in enumerate(datareader):
        if i == 0:
            continue
        pickup_date = row[1].split(" ")[0]
        pickup_location_id = row[-1]
        date_trip_counter[pickup_date] += 1
        location_date_trip_counter[(pickup_date, pickup_location_id)] += 1
    for key, value in sorted(date_trip_counter.items()):
        trip_count_file.write(key + "," + str(value) + "\n")
    for key, value in sorted(location_date_trip_counter.items()):
        location_trip_count_file.write(key[0] + "," + key[1] + "," + str(value) + "\n")
