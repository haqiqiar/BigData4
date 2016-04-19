# -*- coding: utf-8 -*-
"""
Created on Tue Apr 19 21:59:18 2016

@author: Yudha #114
"""

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("ElectricityClaster")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(';')
    CountryCode = int(fields[1])
    ElectricityPercentage = int(fields[3])
    return (CountryCode, ElectricityPercentage)

DATA_PATH=r"E:\Yudha#114\Dropbox\[PENTING TIDAK URGENT]\[ARSIP KULIAH]\SEMESTER 8\Kuliah\Big Data\BigData4\Energy\fetched_data\akses_listrik.csv"    
lines = sc.textFile(DATA_PATH)
print(lines)
rdd = lines.map(parseLine)
totalsByCountryCode = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averagesByCountryCode = totalsByCountryCode.mapValues(lambda x: x[0] / x[1])
results = averagesByCountryCode.collect()
for result in results:
    print result