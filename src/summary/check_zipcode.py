from csv import reader
from pyspark import SparkContext
from operator import add
import sys



def check_zip(x):
    unique_key = x[0].strip()
    if unique_key == "Unique Key":
        return (None, 1)

    zipcode = x[8].strip()
    if not zipcode:
        return (unique_key, "null")
    if zipcode == "00501" or zipcode == "00544" or zipcode == "06390":
        return ("pass", 1)

    try:
        zipnumber = int(zipcode)
    except ValueError:
        return (unique_key, "invalid")

    if zipnumber >= 10001 and zipnumber <= 14925:
        return ("pass", 1)

    return (unique_key, "invalid")


def reducer(x, y):
    x_type = type(x)
    y_type = type(y)
    if x_type == int and y_type == int:
        return x + y

    if x_type != int:
        return x

    return y

if __name__ == "__main__":
    sc = SparkContext()
    data = sc.textFile(sys.argv[1], 1)

    data = data.mapPartitions(lambda x: reader(x))\
            .map(check_zip)\
            .reduceByKey(reducer)\
            .filter(lambda x: x[0])\
            .map(lambda x: "%s\t%s" % (x[0], x[1]))

    data.saveAsTextFile("check_zipcode.out")
    sc.stop()
