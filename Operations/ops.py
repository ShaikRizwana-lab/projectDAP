from os import path
import sys
sys.path.append(path.abspath(r'C:\\Users\\Rizwana\Desktop\Assignments\DAP\DAP_Project'))

from Connections.Connnections_Riz import DBConnections
import pandas as pd
import scipy
import numpy
import csv 
import json
from pathlib import Path

file1 = r"C:\Users\Rizwana\Downloads\employee-earnings-report-2012.csv" #Fiancial data
file2 = r"C:\Users\Rizwana\Downloads\employee-earnings-report-2011.csv" #Fiancial data

class DBOperations:

    def insert_csv_mongo(file,db):
        df = pd.read_csv(file, index_col = 0)
        col_headers = list(df.columns)

        with open(file, 'r') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                #print(row)
                doc={}
                for n in range(0,len(col_headers)):
                    doc[col_headers[n]] = row[n]
                db.insert_one(doc)

    def insert_json_mongo(file,db):
        # data_f = {}
        with open(file, 'r') as f:
            data = json.load(f)
            db.insert_many(data)

    def insert_df_mongo(df,db):
        data = df.to_dict(orient='records')
        db.insert_many(data)
    
    def get_data_mongo(collection):
        cursor = collection.find()
        data = list(cursor)
        df = pd.DataFrame(data)
        return df


    #Function to insert data in MongoDB database
    #def data_dump_mongo(file):
        #client = DBConnections.Connection_mongo()
        #db = client['DAP_test']
        #db.drop_collection("Health")
        #db.drop_collection("Financial")
        #db.drop_collection("Weather")                     
        #collection1 = db.create_collection("Health")
        #collection2 = db.create_collection("Financial")
        #collection3 = db.create_collection("Weather")

    #dumping data in the MONGODB database
    #data_dump_mongo(file_1,file_2,file_3)



    def data_cleaning():
        print("data_cleaning")
        client = DBConnections.Connection_mongo()
        db = client['DAP_test']
        collection1 = db["Health"]
        collection2 = db["Financial"]
        collection3 = db["Weather"]

        health_Data = df = pd.DataFrame(list(collection1.find({})))
        financial_Data = pd.DataFrame(list(collection2.find({})))
        weather_Data = pd.DataFrame(list(collection3.find({})))

        print(health_Data.head)
        print(financial_Data.head)
        print(weather_Data.head)

        

    def data_dump_mysql(df):
        client = DBConnections.Connection_Mysql()
        mycursor = client.cursor()
        mycursor.execute("CREATE DATABASE IF NOT EXISTS daptest")
        mycursor.execute("USE daptest")
        mycursor.execute("CREATE TABLE IF NOT EXISTS financialData (name VARCHAR(255), department VARCHAR(255), title VARCHAR(255), regular decimal(15,2), retro decimal(15,2), other decimal(15,2), overtime decimal(15,2), injured decimal(15,2), detail decimal(15,2), quinn decimal(15,2), totalEarnings decimal(15,2), zip VARCHAR(255))")

        with mycursor as cursor:
            
            for index, row in df.iterrows():
                sql = "INSERT INTO `financialData` (name, department, title, regular, retro, other, overtime, injured, detail, quinn, totalEarnings, zip) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
                #print(row['NAME'], row['DEPARTMENT'], row['TITLE'], row['REGULAR'], row['RETRO'], row['OTHER'], row['OVERTIME'], row['INJURED'], row['DETAIL '], row['QUINN'], row['TOTAL EARNINGS'], row['ZIP'])
                values = (row['NAME'], row['DEPARTMENT'], row['TITLE'], row['REGULAR'], row['RETRO'], row['OTHER'], row['OVERTIME'], row['INJURED'], row['DETAIL '], row['QUINN'], row['TOTAL EARNINGS'], row['ZIP'])
                cursor.execute(sql, values)
            client.commit()
            #mycursor.commit()
            client.close()


#data_dump_mysql()

#data_cleaning()
