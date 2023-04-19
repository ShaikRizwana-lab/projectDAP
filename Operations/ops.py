from os import path
import sys
sys.path.append(path.abspath(r'C:\\Users\\Rizwana\Desktop\Assignments\DAP\DAP_Project'))

from Connections.Connnections_Riz import DBConnections
import pandas as pd
import scipy
import numpy
import csv 



file_1 = r"C:\\Users\\Rizwana\Desktop\Assignments\DAP\DAP_Project\Datasets\data01.csv" #health data
file_2 = r"C:\\Users\\Rizwana\Desktop\Assignments\DAP\DAP_Project\Datasets\\employee-earnings-report-2012.csv" #financial data
file_3 = r"C:\\Users\\Rizwana\Desktop\Assignments\DAP\DAP_Project\Datasets\\ad_viz_plotval_data (1).csv" #weather data

#Function to insert data in MongoDB database
def data_dump_mongo(file1, file2, file3):
    client = DBConnections.Connection_mongo()
    db = client['DAP_test']
    collection1 = db.create_collection("Health")
    collection2 = db.create_collection("Financial")
    collection3 = db.create_collection("Weather")

    df1 = pd.read_csv(file1, index_col = 0)
    col_headers1 = list(df1.columns)

    df2 = pd.read_csv(file2, index_col = 0)
    col_headers2 = list(df2.columns)

    df3 = pd.read_csv(file3, index_col = 0)
    col_headers3 = list(df3.columns)
    #print(col_headers)

    with open(file1, 'r') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            #print(row)
            doc={}
            for n in range(0,len(col_headers1)):
                doc[col_headers1[n]] = row[n]
            collection1.insert_one(doc)
    
    with open(file2, 'r') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            #print(row)
            doc={}
            for n in range(0,len(col_headers2)):
                doc[col_headers2[n]] = row[n]
            collection2.insert_one(doc)
    
    with open(file3, 'r') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            #print(row)
            doc={}
            for n in range(0,len(col_headers3)):
                doc[col_headers3[n]] = row[n]
            collection3.insert_one(doc)

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

    

def data_dump_mysql():
    client = DBConnections.Connection_Mysql()
    mycursor = client.cursor()
    mycursor.execute("CREATE DATABASE daptest2")
    mycursor.execute("USE daptest2")
    mycursor.execute("CREATE TABLE customers (name VARCHAR(255), address VARCHAR(255))")


data_dump_mysql()

#data_cleaning()
