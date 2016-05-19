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
    CountryCode = str(fields[1])
    ElectricityPercentage = float(fields[3])
    return (CountryCode, ElectricityPercentage)

DATA_PATH="file:///SparkCourse/fetched_data/akses_listrik.txt"    
#untuk cek apakah sudah membaca dengan benar
#maped=sc.textFile(DATA_PATH).map(lambda line: line.split(";"))
#print(maped)
#filtered=maped.filter(lambda line: len(line)>1)
#print(filtered)
#collected=filtered.collect()
#print(collected)

#print(sc.textFile(DATA_PATH).map(lambda line: line.split(";")).filter(lambda line: len(line)>1).collect())

lines=sc.textFile(DATA_PATH)
# Cek rdd g bermasalah
rdd = lines.map(parseLine)
#print(rdd.filter(lambda line: len(line)>1).collect())
# Cek mappedvalue, g bermasalah
mapped_value = rdd.mapValues(lambda x: (x, 1))
#print(mapped_value.filter(lambda line: len(line)>1).collect())
# Cek totalsByCountryCode
totalsByCountryCode = mapped_value.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
#print(totalsByCountryCode.filter(lambda line: len(line)>1).collect())


averagesByCountryCode = totalsByCountryCode.reduceByKey(lambda x: x[0] / x[1])
#print(averagesByCountryCode.filter(lambda line: len(line)!=0).collect())
print(averagesByCountryCode.collect())
#results = averagesByCountryCode.collect()
#for result in results:
#    print(result)