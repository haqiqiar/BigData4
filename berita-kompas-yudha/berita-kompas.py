# -*- coding: utf-8 -*-
"""
Created on Thu May 12 11:11:31 2016

@author: yusufazishty
"""

from pyspark import SparkConf, SparkContext
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF

# Function for printing each element in RDD
def println(x):
    print(x)

# Boilerplate Spark stuff:
conf = SparkConf().setMaster("local[4]").setAppName("SparkTFIDF")
sc = SparkContext(conf = conf)

# Load documents (one per line).
rawData = sc.textFile("sari-berita-penting/compiled_news.txt")
fields = rawData.map(lambda x: x.split("|"))
documents = fields.map(lambda x: x[2].split(" "))

documentNames = fields.map(lambda x: x[0])

hashingTF = HashingTF(100000)  #100K hash buckets just to save some memory
tf = hashingTF.transform(documents)

tf.cache()
idf = IDF(minDocFreq=2).fit(tf)
tfidf = idf.transform(tf)

keywordTF = hashingTF.transform(["Panama"])
keywordHashValue = int(keywordTF.indices[0])

keywordRelevance = tfidf.map(lambda x: x[keywordHashValue])

zippedResults = keywordRelevance.zip(documentNames)

print ("Best document for keywords is:")
print (zippedResults.max())
#print (zippedResults[0:4])
#print(zippedResults)