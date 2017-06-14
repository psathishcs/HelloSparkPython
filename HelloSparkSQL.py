'''
Created on Jun 13, 2017

@author: SathishParthasarathy
'''
from pyspark.sql import SparkSession
if __name__ == '__main__':
    spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()
    df = spark.read.json("hdfs://hadoop.master.com:9000/user/psathishcs/Input/Json/Companie.json")     
    df.printSchema()
    df.show()
    df.select("name").show()
    df.select(df['name'], df['founded_year'], df['founded_year'] +10).show()
    
    edf = spark.read.json("hdfs://hadoop.master.com:9000/user/psathishcs/Input/Json/Companies.json")
    edf.filter(edf['founded_year'] >= 2005).show()
    
    print ("Count  df.filter(df['founded_year'] > 2005) -> %d ") % edf.filter(edf['founded_year'] >= 2005).count()
    edf.createOrReplaceTempView("Companies")
    spark.sql("SELECT name, category_code, blog_url, phone_number, relationships FROM Companies").show()
    spark.sql("SELECT count(*) FROM Companies").show()
    spark.sql("SELECT relationships.person, relationships.title  FROM Companies WHERE name='Facebook'").show()
    
            
            
    