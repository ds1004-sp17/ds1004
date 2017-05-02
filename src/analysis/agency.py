from csv import reader
from pyspark import SparkContext
from operator import add
import sys


if __name__ == "__main__":
    sc = SparkContext()
    data = sc.textFile(sys.argv[1], 1)

    data = data.mapPartitions(lambda x: reader(x))\
            .map(lambda x: (x[3].upper().strip(), 1))\
            .reduceByKey(add)\
            .filter(lambda x: x[0] != "AGENCY")\
            .map(lambda x: x[0] + "\t" + str(x[1]))

    data.saveAsTextFile("agency.out")
    sc.stop()
