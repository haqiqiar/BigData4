# -*- coding: utf-8 -*-
"""
Created on Tue Apr 19 21:59:18 2016

@author: Yudha #114
"""
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

# splitting lawas
def parseLine(line):
    fields = line.split(';')
    CountryCode = str(fields[1])
    ElectricityPercentage = float(fields[3])
    return (CountryCode, ElectricityPercentage)

# Function for printing each element in RDD
def println(x):
    print(x)
    
# Function for transforming the tweet
def transformElectricity(line):
    # Data sample: "1, aku suka kamu". Splitted to data[0] = 1, data[1] = "aku suka kamu"
    data = line.split(";")
    id = data[1]
    #print(id)
    electricityData = data[3]
    #print(electricityData)
    
    # Map each word to id using builtin 'map' function
    # The results are [1, "aku"], [1, "suka"], [1, "kamu"]
    #result = map((lambda x: [id, x]), float(electricityData[1])) # input 'x' from splitted string of tweet
    result = (id, electricityData)    
    return result

# Function for transforming the tweet
def CountElectricity(line):
    # Data sample: "1, aku suka kamu". Splitted to data[0] = 1, data[1] = "aku suka kamu"
    #data = line[1]
    #id = line[0]
    
    # Map each word to id using builtin 'map' function
    # The results are [1, "aku"], [1, "suka"], [1, "kamu"]
    result = map((lambda x: (id, (x,1))), line) # input 'x' from splitted string of tweet
    return result

#1 Define the context 
conf = SparkConf().setMaster("local").setAppName("ElectricityClaster")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)


DATA_PATH="file:///SparkCourse/fetched_data/akses_listrik.txt"    
#untuk cek apakah sudah membaca dengan benar
#maped=sc.textFile(DATA_PATH).map(lambda line: line.split(";"))
#print(maped)
#filtered=maped.filter(lambda line: len(line)>1)
#print(filtered)
#collected=filtered.collect()
#print(collected)

#print(sc.textFile(DATA_PATH).map(lambda line: line.split(";")).filter(lambda line: len(line)>1).collect())

# 2. Load and transform electricity
# 2.1 Load the electricity
electricity = sc.textFile(DATA_PATH)
electricity.cache()
electricity.collect()
print('Electricity Data:') 
electricity.foreach(println)
print('\n') 
#lines=sc.textFile(DATA_PATH)
# Cek rdd g bermasalah


# 2.2 Transforming the electricity
electricityMap = electricity.map(transformElectricity)
electricityMap.cache()
electricityMap.collect()
print('Electricity map data:')
electricityMap.foreach(println)
print('\n')
#rdd = lines.map(parseLine)
#print(rdd.filter(lambda line: len(line)>1).collect())
# Cek mappedvalue, g bermasalah


# 2.3 Counting the electricity
electricityCount = electricityMap.reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1]) )
electricityCount.cache()
electricityCount.collect()
print('Electricity Counted data:')
electricityCount.foreach(println)
print('\n')
#mapped_value = rdd.mapValues(lambda x: (x, 1))
#print(mapped_value.filter(lambda line: len(line)>1).collect())


# Cek totalsByCountryCode
#totalsByCountryCode = mapped_value.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
#print(totalsByCountryCode.filter(lambda line: len(line)>1).collect())
#averagesByCountryCode = totalsByCountryCode.reduceByKey(lambda x: x[0] / x[1])
#print(averagesByCountryCode.filter(lambda line: len(line)!=0).collect())
#print(averagesByCountryCode.collect())
#results = averagesByCountryCode.collect()
#for result in results:
#    print(result)