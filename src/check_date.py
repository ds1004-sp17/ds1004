from pyspark.sql import SQLContext
from pyspark import SparkContext
import datetime


def check_datetime(dt):
    try:
        d = datetime.datetime.strptime(dt, "%m/%d/%Y %I:%M:%S %p")
    except ValueError:
        return False

    return True

def datetime_checker():
    sc = SparkContext()
    sqlContext = SQLContext(sc)

    df = sqlContext.read.format('com.databricks.spark.csv')\
            .options(header='false', inferschema='true')\
            .load('311.csv')
    sqlContext.registerDataFrameAsTable(df, "nyc311")

    #headers = ["Created Date", "Closed Date", "Due Date", "Resolution Action Updated Date"]
    headers = ["C1", "C2", "C20", "C21"]
    result = []
    #res = df.select("Unique Key", headers[0], headers[1], headers[2], headers[3]).collect()
    #
    # the whole data set is too large, incur OurOfMemeryError, 
    # and Spark SQL does not support *offset*.
    batch_size = 5 * 10 ** 6
    res = sqlContext.sql("select min(C0), max(C0) from nyc311 \
            where C0 <> 'Unique Key'").collect()
    index = int(res[0]._c0) - 1
    N_max = int(res[0]._c1)
    while index < N_max:
        prev = index
        sql = "select C0, C1, C2, C20, C21 from nyc311 \
                where C0 <> 'Unique Key' and C0 > '%s' and C0 <= '%s' \
                order by C0 limit %d" % (index, index+batch_size, batch_size)
        res = sqlContext.sql(sql).collect()
        for item in res:
            index = int(item["C0"])
            invalid = []
            for header in headers:
                dt = item[header].strip()
                if dt != "":
                    check = check_datetime(dt)
                    if not check:
                        invalid.append(header)

            if len(invalid) > 0:
                result.append(str(index) + "\t" + ", ".join(invalid))

        if index < prev + batch_size:
            index = prev + batch_size

    print(result)
    sc.parallelize(result).saveAsTextFile("check_date.out")
    sc.stop()

if __name__ == "__main__":
    datetime_checker()

