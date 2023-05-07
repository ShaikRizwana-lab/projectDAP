from os import path
import sys
from Connections.Connnections_Riz import DBConnections
from Operations.ops import DBOperations
from Operations.ops import DBOperations
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import pandas as pd
import matplotlib.pyplot as plt


def pre_processing(df):
    df['date_local'] = pd.to_datetime(df['date_local'])
    #CO post processing
    df = df[df['sample_duration'] == '8-HR RUN AVG END HOUR']
    df = df[df['site_address'] == 'KENMORE SQ']
    #granular representation:
    df_2005 = df[df['date_local'] < '2005-01-01']
    df_2006 = df[df['date_local'] > '2005-01-01']
    return df, df_2005, df_2006

def visualizations(y_variable, df, title_name ):
    sns.set(style="ticks")
    fig, ax = plt.subplots(figsize=(8, 5))
    sns.lineplot(x="date_local", y=y_variable, data=df, ax=ax)
    ax.set_xticklabels(ax.get_xticklabels(), rotation=45)
    ax.set_title(title_name)
    plt.savefig(title_name+".png") # save the plot as a PNG file


def data_dump_mysql(df, sql_password):
    client = DBConnections.Connection_Mysql(password=sql_password)
    mycursor = client.cursor()
    mycursor.execute("CREATE DATABASE IF NOT EXISTS daptest")
    mycursor.execute("USE daptest")
    query = """CREATE TABLE IF NOT EXISTS weatherData (
    state_code INT, 
    county_code INT, 
    site_number INT, 
    parameter_code INT,
    poc INT, 
    datum VARCHAR(255), 
    parameter VARCHAR(255),
    sample_duration_code VARCHAR(255), 
    sample_duration VARCHAR(255), 
    pollutant_standard VARCHAR(255),
    date_local DATE, 
    units_of_measure VARCHAR(255), 
    event_type VARCHAR(255), 
    observation_count INT,
    observation_percent DOUBLE(12,7), 
    validity_indicator VARCHAR(255),
    arithmetic_mean DECIMAL(10, 6), 
    first_max_value DECIMAL(10, 6), 
    first_max_hour INT,
    aqi VARCHAR(255), 
    method_code INT, 
    method VARCHAR(255), 
    local_site_name VARCHAR(255),
    site_address VARCHAR(255),
    state VARCHAR(255), 
    county VARCHAR(255),
    city VARCHAR(255), 
    cbsa_code INT, 
    cbsa VARCHAR(255),
    date_of_last_change DATE);"""

    mycursor.execute(query)

    with mycursor as cursor:
            
        for index, row in df.iterrows():
            sql = "INSERT INTO weatherData (state_code, county_code, site_number, parameter_code, poc,\
                datum, parameter, sample_duration_code, sample_duration, pollutant_standard, date_local,\
                    units_of_measure, event_type, observation_count, observation_percent, validity_indicator, arithmetic_mean,\
                        first_max_value, first_max_hour, aqi, method_code, method, local_site_name, site_address,\
                            state, county, city, cbsa_code, cbsa, date_of_last_change)\
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
            #print(row['NAME'], row['DEPARTMENT'], row['TITLE'], row['REGULAR'], row['RETRO'], row['OTHER'], row['OVERTIME'], row['INJURED'], row['DETAIL '], row['QUINN'], row['TOTAL EARNINGS'], row['ZIP'])
            values = (row['state_code'], row['county_code'], row['site_number'], row['parameter_code'], row['poc'],\
                       row['datum'], row['parameter'], row['sample_duration_code'],row['sample_duration'], row['pollutant_standard'],row['date_local'],row['units_of_measure'],\
                            row['event_type'],row['observation_count'],row['observation_percent'],row['validity_indicator'],\
                                row['arithmetic_mean'],row['first_max_value'],row['first_max_hour'],row['aqi'],row['method_code'],\
                                    row['method'],row['local_site_name'],row['site_address'],row['state'],row['county'],\
                                        row['city'],row['cbsa_code'],row['cbsa'],row['date_of_last_change'])
            cursor.execute(sql, values)

        client.commit()
        client.close()

def get_data_mysql(db):
    sql_query = "SELECT * FROM weatherData"
    df = pd.read_sql(sql_query, db)
    return df



def weather_main(path, sql_password):
    #Extraction
    filenames = [
        r'.\Datasets\weather_data\20010101.json',
        r'.\Datasets\weather_data\20020101.json',
        r'.\Datasets\weather_data\20030101.json',
        r'.\Datasets\weather_data\20040101.json',
        r'.\Datasets\weather_data\20050101.json',
        r'.\Datasets\weather_data\20060101.json',
        r'.\Datasets\weather_data\20070101.json',
        r'.\Datasets\weather_data\20080101.json',
        r'.\Datasets\weather_data\20090101.json',
        r'.\Datasets\weather_data\20100101.json',
        r'.\Datasets\weather_data\20110101.json',
        r'.\Datasets\weather_data\20120101.json'
    ]
    # for file in os.listdir(path):
    #     filename = os.fsdecode(file)
    #     if filename.endswith( ('.json') ): # whatever file types you're using...
    #         filenames.append(str(path)+'\\'+filename)

    db = DBConnections.Connection_mongo()
    weather_db = db['DAP_test']['Weather']


    for file in filenames:
        DBOperations.insert_json_mongo(file,weather_db)
    df = DBOperations.get_data_mongo(weather_db)

    
    #Transformation
    df, df_2005, df_2006 = pre_processing(df)
    visualizations("arithmetic_mean", df_2005, "./Images/weather/2001-2005 Arthemetic Mean")
    visualizations("aqi", df_2005, "./Images/weather/2001-2005 AQI")
    visualizations("arithmetic_mean", df_2006, "./Images/weather/2006-2011 Arthemetic mean")
    visualizations("aqi", df_2006, "./Images/weather/2006-2011 Aarthmetic Mean")
    df.to_csv('merged_weather_json.csv', index=False)

    #Loading
    data_dump_mysql(df, sql_password)

if __name__ == '__main__':
    sys.path.append(path.abspath(r"D:\xyz\projectDAP"))
    path = os.fsencode(r"D:\xyz\projectDAP\Datasets\weather_data")
    weather_main(path, '9700')
    


