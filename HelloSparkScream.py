'''
Created on Jun 17, 2017

@author: SathishParthasarathy
'''
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
if __name__ == '__main__':
    sc = SparkContext("local[2]", "Hello Spark Screaming - Python World Count")
    ssc = StreamingContext(sc, 1)
    lines = ssc.socketTextStream("hadoop.master.com", 9999)
    words = lines.flatMap(lambda line: line.split(" "))
    pairs = words.map(lambda word: (word, 1))
    wordCounts = pairs.reduceByKey(lambda x , y : x + y)
    wordCounts.pprint()