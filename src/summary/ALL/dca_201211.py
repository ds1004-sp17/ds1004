from csv import reader
from pyspark import SparkContext
from operator import add
import sys


def process(x):
    date = x[1].strip()
    agency = x[3].strip()
    type_ = x[5].strip()
    desc = x[6].strip()
    zipcode  = x[8].strip()
    borough = x[23].upper().strip()
    if date == "Created Date" or agency != "DCA" \
            or type_ != 'Consumer Complaint' or desc != 'Overcharge':
        return (None, 1)

    split = date.split(" ")
    d = int(split[0][3:5])
    m = int(split[0][:2])
    y = int(split[0][-4:])
    if y == 2013 and m == 11 and d >= 5 and d <= 12:
        return (borough, 1)

    return (None, 1)

if __name__ == "__main__":
    sc = SparkContext()
    data = sc.textFile(sys.argv[1], 1)

    data = data.mapPartitions(lambda x: reader(x))\
            .map(process)\
            .reduceByKey(add)\
            .filter(lambda x: x[0])\
            .sortBy(lambda x: -x[1])\
            .map(lambda x: "%s\t%d" % (x[0], x[1]))

    data.saveAsTextFile("dca_201211.out")
    sc.stop()
