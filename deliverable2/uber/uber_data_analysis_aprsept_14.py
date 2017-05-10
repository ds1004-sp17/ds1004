import csv
from collections import defaultdict

base_path = "/Users/abhishekkadian/Documents/Github/uber-tlc-foil-response/uber-trip-data/"
paths = [
            base_path + "uber-raw-data-apr14.csv",
            base_path + "uber-raw-data-may14.csv",
            base_path + "uber-raw-data-jun14.csv",
            base_path + "uber-raw-data-jul14.csv",
            base_path + "uber-raw-data-aug14.csv",
            base_path + "uber-raw-data-sep14.csv"
        ]
trip_count_file = "uber_aprsept_count_trips.csv"
date_trip_counter = defaultdict(int)

for path in paths:
        with open(path, "rb") as datafile, open(trip_count_file, "wb") as output_date_trips_path:
                datareader = csv.reader(datafile, delimiter=",")
                for i, row in enumerate(datareader):
                        if i == 0:
                                continue
                        pickup_date = row[0].split(" ")[0]
                        date_trip_counter[pickup_date] += 1
                for key, value in sorted(date_trip_counter.items()):
                        output_date_trips_path.write(key + "," + str(value) + "\n")
