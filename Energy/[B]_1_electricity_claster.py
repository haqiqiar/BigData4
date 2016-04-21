# -*- coding: utf-8 -*-
"""
Created on Thu Apr 21 22:27:22 2016

@author: yusufazishty
"""

DATA_PATH = r"E:\UserTA\5112100086\Dropbox\[PENTING TIDAK URGENT]\[ARSIP KULIAH]\SEMESTER 8\Kuliah\Big Data\BigData4\Energy\fetched_data\akses_listrik.txt"

import sklearn.cluster as clustering
import numpy as np

def getNameValPair(fields):
    NameValPair=[]    
    for i in range(len(fields)):
        NameValPair.append( ( fields[i][1] , float(fields[i][3]) ) )
    return NameValPair
            
def getAvgPerKey(NameVal):       
    # Get Value and Occurance Per Key    
    val_ocur={}     
    for i in range(len(NameVal)):
        key = NameVal[i][0]
        
        if key not in val_ocur:
            ocur = 1
            value = NameVal[i][1]            
            val_ocur[key]=(value,ocur)
            
        elif key in val_ocur:
            ocur = ocur+1 
            value = value + NameVal[i][1]            
            val_ocur[key]=(value,ocur)
    
    # Get Average Per Key
    keys=list(val_ocur.keys())
    keys=sorted(keys)        
    avg={}
    
    for i in range(len(keys)):
        key = keys[i]
        avg[key]=val_ocur[key][0]/val_ocur[key][1]

    return avg,keys        

def collect_data(keys, avg_data_by_keys, result_claster):
    clustered_data=[]
    for i in range(len(keys)):
        line=[]
        line.append(keys[i])
        line.append(avg_data_by_keys[i][0])
        line.append(result_claster[i])
        clustered_data.append(line)
    return clustered_data
    
def getRangeCluster(clustered_data):
    val=[0,0,0,0]
    num=[0,0,0,0]
    avg=[0,0,0,0]
    for i in range(len(clustered_data)):
        key = clustered_data[i][2]        
        val[key]+=clustered_data[i][1]
        num[key]+=1
    
    for i in range(len(val)):
        avg[i]=val[i]/num[i]
    
    return avg
    
def getFields(data_path): 
    fields=[]
    with open(DATA_PATH) as file:
        for line in file:    
            chunk=line.strip("\n").split(";")
            fields.append(chunk)
    return fields
    
def preprocess(AvgPerKey, keys):
    avg_data_by_keys=[]    
    for i in range(len(keys)):
        avg_data_by_keys.append(AvgPerKey[keys[i]])
    avg_data_by_keys=np.array(avg_data_by_keys).reshape(len(keys),1)
    return avg_data_by_keys
    
if __name__=="__main__":    
    #1 read from data
    fields = getFields(DATA_PATH)
    
    #2 get only CountryCode and Value     
    NameVal = getNameValPair(fields)
    
    #3 get average per key, and list of keys
    AvgPerKey,keys = getAvgPerKey(NameVal)
    
    #4 process avg data to be clustered
    avg_data_by_keys = preprocess(AvgPerKey,keys)    
            
    #5 Start Clustering    
    kmeans_methode = clustering.KMeans(n_clusters=4, n_init=5)
    result_claster = kmeans_methode.fit_predict(avg_data_by_keys)
    
    #6 collect the data become one
    clustered_data = collect_data(keys, avg_data_by_keys, result_claster)
    
    #7 additional, get range of cluster
    cluster_range = getRangeCluster(clustered_data)
    cluster_range = sorted(cluster_range)