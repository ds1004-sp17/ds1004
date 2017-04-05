from csv import reader
from pyspark import SparkContext
from operator import add
import sys


def extract_month(x):
    if x[1] == "Created Date":
        return (None, 1)

    m = x[1].strip().split("/")[0]
    agency = x[3].strip()
    return ((agency, m), 1)

def stat(x):
    """
    format: month, total_num, avg_num
    """
    days = [31,28,31,30,31,30,31,31,30,31,30,31]
    agency = x[0][0]
    m = x[0][1]
    return "%s\t%s, %d, %.2f" % (agency, m, x[1], x[1] * 1. / days[int(m)-1])

if __name__ == "__main__":
    sc = SparkContext()
    data = sc.textFile(sys.argv[1], 1)

    data = data.mapPartitions(lambda x: reader(x))\
            .map(extract_month)\
            .reduceByKey(add)\
            .filter(lambda x: x[0])\
            .map(stat)

    data.saveAsTextFile("nums_in_month_by_agency.out")
    sc.stop()
