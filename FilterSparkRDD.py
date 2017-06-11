'''
Created on Jun 11, 2017

@author: SathishParthasarathy
'''
from pyspark import SparkConf, SparkContext
if __name__ == '__main__':
    conf = SparkConf().setAppName("Word Count - Python")
    spark = SparkContext(conf = conf)
    text_file = spark.textFile("hdfs://hadoop.master.com:9000/user/psathishcs/Input/Books/The_Outline_of_Science.txt")
    print "No of Lines -> %d" % (text_file.count())
    print "First -> %d" % (text_file.first())
    sciLines =  text_file.filter(lambda line: "Science" in line)
    sciLines.saveAsTextFile("hdfs://hadoop.master.com:9000/user/psathishcs/Output/Books/ScienceSci_Python");
    sciWordLines = text_file.filter( lambda line : hasScience(line))
    sciWordLines.saveAsTextFile("hdfs://hadoop.master.com:9000/user/psathishcs/Output/Books/ScienceSciWord_Python");
    

def hasScience(line):
    return "Science" in line 