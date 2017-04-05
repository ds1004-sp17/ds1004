from csv import reader
from pyspark import SparkContext
from operator import add
import sys
import datetime
import math


def diff(date1, date2):
    """
    return time difference in hour
    """
    try:
        t1 = datetime.datetime.strptime(date1, "%m/%d/%Y %I:%M:%S %p")
        t2 = datetime.datetime.strptime(date2, "%m/%d/%Y %I:%M:%S %p")
        diff = t2 - t1
        return 24. * diff.days + diff.seconds / 3600.
    except ValueError:
        return None

def compute(x):
    """
    for each agency, compute how much time they used to process a complaint, 
    """
    agency = x[3].strip()
    created_date = x[1].strip()
    updated_date = x[21].strip()
    diff_time = diff(created_date, updated_date)
    return (agency, diff_time)

def stats(x):
    """
    return max, min, mean, std
    """
    agency = x[0]
    diffs = x[1]
    n = len(diffs)
    mean = diffs[0] / n
    max_ = diffs[0]
    min_ = diffs[0]
    for t in diffs[1:]:
        mean += t / n
        max_ = max(max_, t)
        min_ = min(min_, t)

    sq_diff = [(x - mean)**2 / n for x in diffs]
    std = math.sqrt(sum(sq_diff))

    return "%s\t%d, %.4f, %.4f, %.4f, %.4f" % (agency, n, max_, min_, mean, std)

if __name__ == "__main__":
    sc = SparkContext()
    data = sc.textFile(sys.argv[1], 1)

    data = data.mapPartitions(lambda x: reader(x))\
            .filter(lambda x: x[1] != "Created Date")\
            .map(compute)\
            .filter(lambda x: x[1])\
            .groupByKey()\
            .mapValues(list)\
            .map(stats)

    data.saveAsTextFile("duration_by_agency.out")
    sc.stop()
