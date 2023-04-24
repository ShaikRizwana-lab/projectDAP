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



def preProcessing(df):
    #replace Null values with '0'
    for column in df.columns:
        df[column].fillna(df[column].mode()[0], inplace=True)
    
    #feature engineering
    new_df = df[["outcome","BMI","age",'gendera','hypertensive','COPD','atrialfibrillation','CHD with no MI','diabetes','depression','heart rate']].copy()
    new_df = new_df.rename(columns={'gendera': 'gender'})# Rename the column 'gendera' to 'gender
    return new_df
def visualizations(new_df):
    #plot pairplot to get the linearity between variables
    sns.pairplot(new_df)
    plt.savefig("pairplot"+'.png')
    #plot heatmap to get the corelation between variables
    sns.heatmap(new_df.corr(), annot=True)
    plt.savefig("heatmap"+'.png')
    #Insert date column to produce trends for variables
    start_date = '2001-01-01'
    end_date = '2012-12-31'
    dates = pd.date_range(start=start_date, end=end_date, periods=1177)
    dates_series = pd.Series(dates).dt.date
    new_df['date'] = dates_series
    
    # Create a histogram of the COPD column using histplot
    sns.histplot(data=new_df, x='COPD',binwidth=0.4)
    # Set the x-axis tick locations and labels
    plt.xticks([0, 1], ['No COPD', 'COPD'])
    # Add labels and a title
    plt.xlabel('COPD Status')
    plt.ylabel('Count')
    plt.title('Distribution of COPD in Health Dataset')
    plt.savefig("histplot"+'.png')
    
    sns.displot(new_df, x="BMI", binwidth=3)
    plt.savefig("displot"+'.png')
    return new_df
    
def scatterPlot(df, y, hue, filename):
    g = sns.FacetGrid(df, col="gender", hue='diabetes')
    g.map(sns.scatterplot, "date", y, alpha=.7)
    g.add_legend()
    plt.savefig(filename+'.png')


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

def health_main(path, sql_password):
    #Extraction
    file = r"C:\Users\Rizwana\Desktop\Assignments\DAP\DAP_Project\Datasets\data01.csv"
    db = DBConnections.Connection_mongo()
    health_db = db["DAP_test"]["Health"]
    DBOperations.insert_csv_mongo(file, health_db)
    df = DBOperations.get_data_mongo(health_db)

    #Transformation
    new_df = preProcessing(df)
    df_1 = visualizations(new_df)
    scatterPlot(df_1, "BMI", "COPD", "COPD dist")
    scatterPlot(df_1, "age", "CHD with no MI", "CHD with no MI")
    

    #Loading
    data_dump_mysql(df, sql_password)
   
if __name__ == '__main__':
    sys.path.append(path.abspath(r"D:\NCI\DAP\projectDAP"))
    path = os.fsencode(r"D:\NCI\Database and Analytics\TABA\weather_data_CO_20230420\\")
    health_main(path, '9700')
    
