from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.functions import *


def street():
    sc = SparkContext()
    sqlContext = SQLContext(sc)

    df = sqlContext.read.format('com.databricks.spark.csv')\
            .options(header='false', inferschema='true')\
            .load('311.csv')

    # which type of complaints at broadway most
    res = df.filter("C10 = 'BROADWAY'")\
            .groupBy("C5")\
            .count()\
            .orderBy(desc("count"))\
            .limit(100)

    res.write.format("json").save("type_broadway.out")
    sc.stop()

if __name__ == "__main__":
    street()

