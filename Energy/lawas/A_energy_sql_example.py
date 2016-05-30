# -*- coding: utf-8 -*-
"""
Created on Thu May 26 12:56:23 2016

@author: yusufazishty
"""

#For loading the data to the dataframe and then we can play around the data
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext


conf = SparkConf()
conf.set("spark.executor.memory", "1g")
conf.set("spark.cores.max","2")
conf.setAppName("fetching_data")

# Initialize only once
sc = SparkContext('local', conf=conf)

#test to make sure everything works
#lines=sc.textFile("sari-berita-penting/compiled_news.txt")
#print(lines.count())

#import akses_listrik
SQLContext = SQLContext(sc)
akses_listrik = SQLContext.read.format('json').load("fetched_data/akses_bbm.json")
#akses_listrik.show()
#akses_listrik.printSchema()

# Do SQL queries
#akses_listrik.select("Value").show()
#akses_listrik.filter(akses_listrik["Value"]<50.0).show()
#akses_listrik.groupBy("CountryCode").count().show()
#akses_listrik.groupBy("CountryCode").agg({"Country":"count", "Value":"max"}).show()

# Make Another List
another_list = [{"CountryCode":"AAA", "Value":"100"}, 
                {"CountryCode":"BBB", "Value":"100"}]
another_listDF = SQLContext.createDataFrame(another_list) 
#another_listDF.show()

# Join the dataframe
#akses_listrik.join(another_listDF, akses_listrik.Value==another_listDF.Value).show()

# register a dataframe as table and run SQL statements againts
akses_listrik.registerTempTable("akses_listrik")
SQLContext.sql("select * from akses_listrik where Value > 50").show()

# to pandas dataframe
