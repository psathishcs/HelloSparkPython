'''
Created on Jun 13, 2017

@author: SathishParthasarathy
'''
from pyspark.sql import SparkSession
if __name__ == '__main__':
    spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()
    df = spark.read.json("hdfs://hadoop.master.com:9000/user/psathishcs/Input/Json/Companie.json")     
    df.printSchema()
            
            
    