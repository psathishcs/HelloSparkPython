'''
Created on Jun 10, 2017

@author: SathishParthasarathy
'''
from pyspark import SparkConf, SparkContext, SparkFiles

if __name__ == '__main__':
    conf = SparkConf().setAppName("Word Count - Python")
    spark = SparkContext(conf = conf)
    text_file = spark.textFile("hdfs://hadoop.master.com:9000/user/psathishcs/Input/Books/The_Outline_of_Science.txt")
    words = text_file.flatMap(lambda line: line.split())
    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b:  a + b)
    wordCounts.saveAsTextFile("hdfs://hadoop.master.com:9000/user/psathishcs/Output/Books/Science_Python")
     
    
