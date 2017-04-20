from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.functions import *
from operator import add
import datetime


def parse_datetime(x):
    """
    parse *year* and *weekday*
    """
    dt = x.C1
    date = datetime.datetime.strptime(dt, "%m/%d/%Y %I:%M:%S %p")
    y = int(date.year)
    m = int(date.month)
    d = int(date.day)
    day = datetime.date(y, m, d).isoweekday()
    return (y, day), 1

def street():
    sc = SparkContext()
    sqlContext = SQLContext(sc)

    df = sqlContext.read.format('com.databricks.spark.csv')\
            .options(header='false', inferschema='true')\
            .load('311.csv')

    res = df.filter("C10 = 'BROADWAY'")\
            .filter("C5 = 'Noise - Commercial'")\
            .select("C1")\
            .collect()

    # for *Noise - Commercial* at broadway, 
    # compare the complaints over weekdays
    sc.parallelize(res).map(parse_datetime)\
            .reduceByKey(add)\
            .map(lambda x: "%d\t%d, %d" % (x[0][0], x[0][1], x[1]))\
            .saveAsTextFile("type_broadway_noise.out")

    sc.stop()

if __name__ == "__main__":
    street()

