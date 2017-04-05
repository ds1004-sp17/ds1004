from csv import reader
from pyspark import SparkContext
from operator import add
import sys


def extract_hour(x):
    date = x[1].strip()
    if date == "Created Date":
        return (None, 1)

    split = date.split(" ")
    h = int(split[1][:2]) % 12
    if split[2].upper() == "PM":
        h += 12

    agency = x[3].strip()
    return ((agency, h), 1)

if __name__ == "__main__":
    sc = SparkContext()
    data = sc.textFile(sys.argv[1], 1)

    data = data.mapPartitions(lambda x: reader(x))\
            .map(extract_hour)\
            .reduceByKey(add)\
            .filter(lambda x: x[0])\
            .map(lambda x: "%s\t%d, %d" % (x[0][0], x[0][1], x[1]))

    data.saveAsTextFile("nums_in_hour_by_agency.out")
    sc.stop()
