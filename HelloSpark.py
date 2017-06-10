'''
Created on Jun 9, 2017

@author: SathishParthasarathy
'''
from pyspark import SparkConf, SparkContext
if __name__ == '__main__':
    conf = SparkConf().setMaster("spark://hadoop.master.com:7077").setAppName("HelloSpark-Python")
    spark = SparkContext(conf = conf)
    textfile = spark.textFile("hdfs://hadoop.master.com:9000/user/psathishcs/Input/Books/The_Outline_of_Science.txt")
    print "No of Lines -> %d" % (textfile.count())