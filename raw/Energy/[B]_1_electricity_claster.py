# -*- coding: utf-8 -*-
"""
Created on Thu Apr 21 22:27:22 2016

@author: yusufazishty
"""

DATA_PATH = r"E:\UserTA\5112100086\Dropbox\[PENTING TIDAK URGENT]\[ARSIP KULIAH]\SEMESTER 8\Kuliah\Big Data\BigData4\Energy\fetched_data\akses_listrik.txt"
OUTPUT_CLUSTERED_FILE=r"clastered_data\[B]_1_electricity_clustered.txt"
OUTPUT_RANGE_FILE=r"clastered_data\[B]_1_electricity_range.txt"

import sklearn.cluster as clustering
import numpy as np
from collections import OrderedDict

def getNameValPair(fields):
    NameValPair=[]
    Name=[]
    Code=[]
    CountryNames=OrderedDict()
    CodeNames=OrderedDict()
    
    #Get Name sorted respective to the codename    
    for i in range(len(fields)):
        Name.append(fields[i][0])
        Code.append(fields[i][1])
    
    # Get unique value of unsorted CountryName and Code, it's been sorted becoause an ordered dictionary
    for i in range(len(Name)):
        key=Name[i]        
        if key not in CountryNames:
            CountryNames[key]=1
        elif key in CountryNames:
            pass
    
    for i in range(len(Code)):
        key=Code[i]        
        if key not in CodeNames:
            CodeNames[key]=1
        elif key in CodeNames:
            pass
            
    Name=list(CountryNames.keys())
    Code=list(CodeNames.keys())
    
    #Get NameValPair, doesnt effect each other 
    for i in range(len(fields)):
        NameValPair.append( ( fields[i][1] , float(fields[i][3]) ) )
    
    return NameValPair, Name, Code
            
def getAvgPerKey(NameVal, Name, keys):       
    # Get Value and Occurance Per Key    
    val_ocur=OrderedDict()     
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
    avg=OrderedDict()
    
    for i in range(len(keys)):
        key = keys[i]
        avg[key]=val_ocur[key][0]/val_ocur[key][1]

    return avg        

def collect_data(keys, Name, avg_data_by_keys, result_claster):
    clustered_data=[]
            
    for i in range(len(keys)):
        line=[]
        line.append(keys[i])
        line.append(Name[i])
        line.append(avg_data_by_keys[i][0])
        line.append(result_claster[i])
        clustered_data.append(line)
    return clustered_data
    
def getRangeCluster(clustered_data):
    val=[0,0,0,0]
    num=[0,0,0,0]
    avg=[0,0,0,0]
    for i in range(len(clustered_data)):
        key = clustered_data[i][-1]        
        val[key]+=float(clustered_data[i][2])
        num[key]+=1
    
    for i in range(len(val)):
        avg[i]=val[i]/num[i]
    
    line=[]
    line.append(avg)
    return line
    
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
    
def save_txt(dataToSave,fileName):
    type_0 = type(dataToSave[0][0])
    
    if type_0 == str:
        head="CountryCode;CountryName;AvgElectricity;Cluster\n"
        with open(fileName, 'w') as txtfile:
            txtfile.write(head)
            for i in range(len(dataToSave)):
                try :
                    line=str(dataToSave[i][0])+";"+str(dataToSave[i][1])+";"+str(dataToSave[i][2])+";"+str(dataToSave[i][3])+"\n"
                except IndexError as detail:
                        print(detail)
                        print(i)      
                txtfile.write(line)  
        txtfile.close()

    elif type_0 == float:
        with open(fileName, 'w') as txtfile:
            for i in range(len(dataToSave)):
                try :
                    line=str(dataToSave[i][0])+";"+str(dataToSave[i][1])+";"+str(dataToSave[i][2])+";"+str(dataToSave[i][3])+"\n"
                except IndexError as detail:
                        print(detail)
                        print(i)      
                txtfile.write(line)  
        txtfile.close()


if __name__=="__main__":    
    #1 read from data
    fields = getFields(DATA_PATH)
    
    #2 get only CountryCode and Value     
    NameVal, Name, keys = getNameValPair(fields)
    
    #3 get average per key, and list of keys
    AvgPerKey = getAvgPerKey(NameVal, Name, keys)
    
    #4 process avg data to be clustered
    avg_data_by_keys = preprocess(AvgPerKey,keys)    
            
    #5 Start Clustering    
    kmeans_methode = clustering.KMeans(n_clusters=4, n_init=5)
    result_claster = kmeans_methode.fit_predict(avg_data_by_keys)
    
    #6 collect the data become one
    clustered_data = collect_data(keys, Name, avg_data_by_keys, result_claster)
    
    #7 additional, get range of cluster
    cluster_range = getRangeCluster(clustered_data)
    
    #8 saving the clustered data
    save_txt(clustered_data, OUTPUT_CLUSTERED_FILE)
    save_txt(cluster_range, OUTPUT_RANGE_FILE)
    