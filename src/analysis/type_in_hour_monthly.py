from csv import reader
from pyspark import SparkContext
from operator import add
import sys


def extract_hour(x):
    date = x[1].strip()
    if date == "Created Date":
        return (None, 1)

    split = date.split(" ")
    m = split[0][:2]
    h = int(split[1][:2]) % 12
    if split[2].upper() == "PM":
        h += 12

    t = x[5].strip()
    return ((t, m, h), 1)


if __name__ == "__main__":
    sc = SparkContext()
    raw_data = sc.textFile(sys.argv[1], 1)

    raw_data = raw_data.mapPartitions(lambda x: reader(x))
    top10 = raw_data.map(lambda x: (x[5].strip(), 1))\
            .reduceByKey(add)\
            .sortBy(lambda x: -x[1])\
            .take(10)

    top10 = [x[0] for x in top10]

    data = raw_data.map(extract_hour)\
            .reduceByKey(add)\
            .filter(lambda x: x[0] is not None and x[0][0] in top10)\
            .map(lambda x: "%s, %d\t%s, %d" % (x[0][0], x[0][2], x[0][1], x[1]))

    data.saveAsTextFile("type_in_hour_monthly.out")
    sc.stop()
