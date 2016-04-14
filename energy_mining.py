# -*- coding: utf-8 -*-
"""
Created on Thu Apr 14 14:49:37 2016

@author: Yudha #114
"""

import sqlite3 as lite
import sys

#DATA_PATH kontrakan :
#Remember \U traps on python, use this :
DATA_PATH=r"C:\Users\Yudha #114\Dropbox\[PENTING TIDAK URGENT]\[ARSIP KULIAH]\SEMESTER 8\Kuliah\Big Data\Tugas KElompok 1\world-development-indicators\database.sqlite"
#or this:
#DATA_PATH="C:\\Users\\Yudha #114\\Dropbox\\[PENTING TIDAK URGENT]\\[ARSIP KULIAH]\\SEMESTER 8\\Kuliah\\Big Data\\Tugas KElompok 1\\world-development-indicators\\database.sqlite"

con = None

try:
    con = lite.connect(DATA_PATH)
    
    cur = con.cursor()    
    #cur.execute('SELECT SQLITE_VERSION()')
    cur.execute('SELECT * FROM ')
    
    data = cur.fetchone()
    
    #print("SQLite version: %s" % data)
                    
    
except lite.Error as e:
    
    print("Error %s:" % e.args[0])
    sys.exit(1)
    
finally:
    
    if con:
        con.close()