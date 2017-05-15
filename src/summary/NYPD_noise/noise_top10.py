from __future__ import print_function
import heapq
import sys
from operator import add
from pyspark import SparkContext
from csv import reader
from datetime import datetime
import re

if __name__ == "__main__":
  sc = SparkContext()
  data = sc.textFile(sys.argv[1], 1)
  header = data.first() 
  lines = data.filter(lambda row: row != header) 
  line = lines.map(lambda x:(x.encode('ascii','ignore')))\
        .mapPartitions(lambda x: (reader(x, delimiter = ',', quotechar = '"')))
  
  def check_datetime(dt):
      try:
          d = datetime.strptime(dt, "%m/%d/%Y %I:%M:%S %p")
          if d.year < 2009 or d.year > 2017:
              return False
      except ValueError:
          return False

      return True

  def takeOrderedByKey(self, num, sortValue = None, reverse=False):
   
    def init(a):
        return [a]

    def combine(agg, a):
        agg.append(a)
        return getTopN(agg)

    def merge(a, b):
        agg = a + b
        return getTopN(agg)

    def getTopN(agg):
        if reverse == True:
            return heapq.nlargest(num, agg, sortValue)
        else:
            return heapq.nsmallest(num, agg, sortValue)              

    return self.combineByKey(init, combine, merge)

  def get_date(dt):
    d = datetime.strptime(dt, "%m/%d/%Y %I:%M:%S %p")
    return str(d.year),str(d.month),str(d.day)

  from pyspark.rdd import RDD
  RDD.takeOrderedByKey = takeOrderedByKey

  noise_top10 = line.filter(lambda x: check_datetime(x[1])) \
                              .filter(lambda x: x[3] == 'NYPD') \
                              .filter(lambda x: re.match(r'Noise', x[5]) != None) \
                              .map(lambda x: ((get_date(x[1]),x[5]),1)) \
                              .foldByKey(0, add) \
                              .map(lambda x: ((x[0][1],x[0][0][0]),(x[0][0],x[0][1],x[1]))) \
                              .takeOrderedByKey(10, sortValue=lambda x: x[1], reverse=True)\
                              .flatMap(lambda x: x[1])\
                              .map(lambda x: ('-'.join(x[0]),x[1]))

  time_lists = noise_top10.collect()

  line.filter(lambda x: check_datetime(x[1])) \
                          .filter(lambda x: x[3] == 'NYPD') \
                          .filter(lambda x: re.match(r'Noise', x[5]) != None) \
                          .map(lambda x: (('-'.join(get_date(x[1])),x[5]),x[8],x[52])) \
                          .filter(lambda x: x[0] in time_lists) \
                          .map(lambda x: (str(x[0][0])+ ',' + str(x[0][1])+','+str(x[1])+ ','+str(x[2])))\
                          .saveAsTextFile("noise_top10.out")
                              
  sc.stop()
                            