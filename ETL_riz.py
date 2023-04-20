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


file1 = r"C:\Users\Rizwana\Downloads\employee-earnings-report-2012.csv"
file2 = r"C:\Users\Rizwana\Downloads\employee-earnings-report-2011.csv"

raw_data_df = pd.concat(map(pd.read_csv, [file1, file2]), ignore_index=True)

mongo_client = DBConnections.Connection_mongo()
db = mongo_client['DAP_test']
db.drop_collection("Financial")

financial_db = db["Financial"]

DBOperations.insert_df_mongo(raw_data_df,financial_db)

df = DBOperations.get_data_mongo(financial_db)

#print(df.head())



#_______________________________________________________________

#print(len(df))

#checking for all the NA values
#print(df.isnull().sum())

#removing NA and replacing it with 0
df = df.fillna(0.00)
#print(df.isnull().sum())

#checking for duplicate values
#print(df.duplicated().sum())

#print(df.describe())

#Changing datatype from string to 
df['REGULAR'] = (df['REGULAR'].replace('[\$,]', '', regex=True)
                           .astype(float))

df['RETRO'] = (df['RETRO'].replace('[\$,]', '', regex=True)
                           .astype(float))

df['OTHER'] = (df['OTHER'].replace('[\$,]', '', regex=True)
                           .astype(float))

df['OVERTIME'] = (df['OVERTIME'].replace('[\$,]', '', regex=True)
                           .astype(float))

df['INJURED'] = (df['INJURED'].replace('[\$,]', '', regex=True)
                           .astype(float))

df['DETAIL '] = (df['DETAIL '].replace('[\$,]', '', regex=True)
                           .astype(float))

df['QUINN'] = (df['QUINN'].replace('[\$,]', '', regex=True)
                           .astype(float))

df['TOTAL EARNINGS'] = (df['TOTAL EARNINGS'].replace('[\$,]', '', regex=True)
                           .astype(float))

#check the data types of the variables
df.dtypes
print("\n after the pre-processig")
print(df.head())

DBOperations.data_dump_mysql(df)
#create boxplots to identify outliers
"""y = list(df.REGULAR)
plt.boxplot(y)
plt.show()
"""

