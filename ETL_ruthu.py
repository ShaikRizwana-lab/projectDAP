from os import path
import sys
sys.path.append(path.abspath(r'C:\\Users\\Rizwana\Desktop\Assignments\DAP\DAP_Project'))

from Connections.Connnections_Riz import DBConnections
from Operations.ops import DBOperations
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

import scipy
import numpy
import csv 
import json

file = r"C:\Users\Rizwana\Desktop\Assignments\DAP\DAP_Project\Datasets\data01.csv"

db = DBConnections.Connection_mongo()
health_db = db["DAP_test"]["Health"]

DBOperations.insert_csv_mongo(file, health_db)

df = DBOperations.get_data_mongo(health_db)

"""
Transform data
"""

def data_dump_mysql(df):
    client = DBConnections.Connection_Mysql()
    mycursor = client.cursor()
    mycursor.execute("CREATE DATABASE IF NOT EXISTS daptest")
    mycursor.execute("USE daptest")
    mycursor.execute("CREATE TABLE IF NOT EXISTS healthData (outcome INT, BMI DOUBLE(7,12), age INT, gendera INT, hypertensive INT, COPD INT, atrialfibrillation INT, CHD with no MI INT, diabetes INT, depression INT, heart rate DOUBLE(7,12))")

    with mycursor as cursor:
            
        for index, row in df.iterrows():
            sql = "INSERT INTO `healthData` (outcome, BMI, age , gendera , hypertensive , COPD, atrialfibrillation, CHD with no MI, diabetes, depression, heart rate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,);"
            #print(row['NAME'], row['DEPARTMENT'], row['TITLE'], row['REGULAR'], row['RETRO'], row['OTHER'], row['OVERTIME'], row['INJURED'], row['DETAIL '], row['QUINN'], row['TOTAL EARNINGS'], row['ZIP'])
            values = (row['outcome'], row['BMI'], row['age'], row['gendera'], row['hypertensive'], row['COPD'], row['atrialfibrillation'], row['CHD with no MI'], row['diabetes'], row['depression'], row['heart rate'])
            cursor.execute(sql, values)
        client.commit()
        #mycursor.commit()
        client.close()

def get_data_mysql(db):
    sql_query = "SELECT * FROM healthData"
    df = pd.read_sql(sql_query, db)
    return df
