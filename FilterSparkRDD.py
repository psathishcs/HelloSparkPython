'''
Created on Jun 11, 2017

@author: SathishParthasarathy
'''
from pyspark import SparkConf, SparkContext

def hasScience(line):
    return "Science" in line 

if __name__ == '__main__':
    conf = SparkConf().setAppName("Word Count - Python")
    spark = SparkContext(conf = conf)
    text_file = spark.textFile("hdfs://hadoop.master.com:9000/user/psathishcs/Input/Books/The_Outline_of_Science.txt")
    print "No of Lines -> %d" % (text_file.count())
    print "First -> %s" % (text_file.first())
    sciLines =  text_file.filter(lambda line: "Science" in line)
    sciLines.saveAsTextFile("hdfs://hadoop.master.com:9000/user/psathishcs/Output/Books/ScienceSci_Python")
    sciWordLines = text_file.filter(hasScience)
    sciWordLines.saveAsTextFile("hdfs://hadoop.master.com:9000/user/psathishcs/Output/Books/ScienceSciWord_Python")
    

