# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import plotly
import pandas as pd

if __name__=="__main__":
    #Fetch data  
    akses_listrik = pd.read_csv("akses_listrik.csv", delimiter=";")
    akses_bbm = pd.read_csv("akses_bbm.csv", delimiter=";")
    energi_terbarukan = pd.read_csv("energi_terbarukan.csv", delimiter=";")
    energi_habis = pd.read_csv("energi_habis.csv", delimiter=";")
    energi_nuklir = pd.read_csv("energi_nuklir.csv", delimiter=";")
    
    