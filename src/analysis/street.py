from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.functions import *


def street():
    sc = SparkContext()
    sqlContext = SQLContext(sc)

    df = sqlContext.read.format('com.databricks.spark.csv')\
            .options(header='false', inferschema='true')\
            .load('311.csv')

    # which street has complaints most
    res = df.filter("C10 != ''")\
            .groupBy("C10")\
            .count()\
            .orderBy(desc("count"))\
            .limit(10)

    res.write.format("json").save("street.out")
    sc.stop()

if __name__ == "__main__":
    street()

