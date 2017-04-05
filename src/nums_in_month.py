from csv import reader
from pyspark import SparkContext
from operator import add
import sys


def extract_month(x):
    m = x[1].strip().split("/")[0]
    return (m, 1)

def stat(x):
    """
    format: month, total_num, avg_num
    """
    days = [31,28,31,30,31,30,31,31,30,31,30,31]
    return "%s\t%d, %.2f" % (x[0], x[1], x[1] * 1. / days[int(x[0])-1])

if __name__ == "__main__":
    sc = SparkContext()
    data = sc.textFile(sys.argv[1], 1)

    data = data.mapPartitions(lambda x: reader(x))\
            .map(extract_month)\
            .reduceByKey(add)\
            .filter(lambda x: x[0] != "Created Date")\
            .map(stat)

    data.saveAsTextFile("nums_in_month.out")
    sc.stop()
