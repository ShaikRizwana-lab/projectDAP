from os import path
import sys
sys.path.append(path.abspath(r'C:\\Users\\Rizwana\Desktop\Assignments\DAP\DAP_Project'))

from Connections.Connnections_Riz import DBConnections
from Operations.ops import DBOperations
from Operations.ops import DBOperations
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

import scipy
import numpy
import csv 
import json

import os

path = r"C:\Users\Rizwana\Desktop\Assignments\DAP\DAP_Project\Datasets\weather data\\"

folder = os.fsencode(path)

filenames = []

for file in os.listdir(folder):
    filename = os.fsdecode(file)
    if filename.endswith( ('.json') ): # whatever file types you're using...
        filenames.append(path+filename)


db = DBConnections.Connection_mongo()
weather_db = db['DAP_test']['Weather']
for file in filenames:
    DBOperations.insert_json_mongo(file,weather_db)

df = DBOperations.get_data_mongo(weather_db)

"""
trasform your data here

"""

def data_dump_mysql(df):
    client = DBConnections.Connection_Mysql()
    mycursor = client.cursor()
    mycursor.execute("CREATE DATABASE IF NOT EXISTS daptest")
    mycursor.execute("USE daptest")
    mycursor.execute("CREATE TABLE IF NOT EXISTS weatherData (state_code INT, county_code INT, site_number INT, parameter_code INT, poc INT, latitude DOUBLE(7,12)\
                     ,longitude DOUBLE(7,12), datum VARCHAR(255), parameter VARCHAR(255), sample_duration_code INT, sample_duration INT,\
                     pollutant_standard VARCHAR(255)),date_local DATE, units_of_measure VARCHAR(255), event_type VARCHAR(255),\
                     observation_count INT, observation_percent DOUBLE(7,12), validity_indicator VARCHAR(255), arithmetic_mean DOUBLE(7, 12),\
                     first_max_value DOUBLE(7, 12), first_max_hour INT, aqi VARCHAR(255), method_code INT, method VARCHAR(255), local_site_name VARCHAR(255), site_address VARCHAR(255),\
                     state VARCHAR(255), county VARCHAR(255), city VARCHAR(255), cbsa_code INT, cbsa VARCHAR(255), date_of_last_change DATE")

    with mycursor as cursor:
            
        for index, row in df.iterrows():
            sql = "INSERT INTO `weatherData` (state_code, county_code, site_number, parameter_code, poc, latitude,\
                longitude, datum, parameter, sample_duration_code, sample_duration, pollutant_standard, date_local,\
                    units_of_measure, event_type, observation_count, observation_percent, validity_indicator, arithmetic_mean,\
                        first_max_value, first_max_hour, aqi, method_code, method, local_site_name, site_address,\
                            state, county, city, cbsa_code, cbsa, date_of_last_change)\
             VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
            #print(row['NAME'], row['DEPARTMENT'], row['TITLE'], row['REGULAR'], row['RETRO'], row['OTHER'], row['OVERTIME'], row['INJURED'], row['DETAIL '], row['QUINN'], row['TOTAL EARNINGS'], row['ZIP'])
            values = (row['state_code'], row['county_code'], row['site_number'], row['parameter_code'], row['poc'], row['latitude'],\
                       row['longitude'], row['datum'], row['parameter'], row['sample_duration_code'],\
                          row['sample_duration'], row['pollutant_standard'], row['date_local'],row['units_of_measure'],\
                            row['event_type'],row['observation_count'],row['observation_percent'],row['validity_indicator'],\
                                row['arithmetic_mean'],row['first_max_value'],row['first_max_hour'],row['aqi'],row['method_code'],\
                                    row['method'],row['local_site_name'],row['site_address'],row['state'],row['county'],\
                                        row['city'],row['cbsa_code'],row['cbsa'],row['date_of_last_change'])
            cursor.execute(sql, values)
        client.commit()
        #mycursor.commit()
        client.close()

def get_data_mysql(db):
    sql_query = "SELECT * FROM weatherData"
    df = pd.read_sql(sql_query, db)
    return df
