'''
Created on Jun 10, 2017

@author: SathishParthasarathy
'''
from pyspark import SparkConf, SparkContext
import hdfs3
if __name__ == '__main__':
    conf = SparkConf().setAppName("Word Count - Python")
    spark = SparkContext(conf = conf)
    hdfs = hdfs3.HDFileSystem('hadoop.master.com', port=9000)
    if hdfs.exists("/user/psathishcs/Input/Books/The_Outline_of_Science.txt") != True:
        text_file = spark.textFile("hdfs://hadoop.master.com:9000/user/psathishcs/Input/Books/The_Outline_of_Science.txt")
        words = text_file.flatMap(lambda line: line.split())
        wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b:  a + b)
        for line in wordCounts.take(1000):
            print line        
