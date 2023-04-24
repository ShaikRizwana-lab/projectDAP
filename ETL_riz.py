from os import path
import sys
sys.path.append(path.abspath(r'C:\\Users\\Rizwana\Desktop\Assignments\DAP\DAP_Project'))
from Connections.Connnections_Riz import DBConnections
from Operations.ops import DBOperations
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


def Extract(raw_data_df):
    mongo_client = DBConnections.Connection_mongo()
    db = mongo_client['DAP_test']
    db.drop_collection("Financial")
    financial_db = db["Financial"]
    DBOperations.insert_df_mongo(raw_data_df,financial_db)
    df = DBOperations.get_data_mongo(financial_db)
    return df


def pre_processing(df):
    df = df.fillna(0.00)
    mod_columns = ['REGULAR', 'RETRO', 'OTHER', 'OVERTIME', 'INJURED', 'DETAIL ', 'QUINN', 'TOTAL EARNINGS']
    for col in mod_columns:
        df[col] = df[col].replace('[\$,]', '', regex=True).astype(float)

    df = df[df['Total Earnings'] > 1000] #removing the possible data errors
    return df

def vizualization(df):
    sns.histplot(df["Total Earnings"])
    plt.title("Total Earnings by Department")
    plt.xlabel("Earning")
    plt.ylabel("Counts")
    plt.xticks(rotation=90)
    plt.show()
    plt.savefig('earning distribution.png')

def fianace_main(file1, file2):
    raw_data_df = pd.concat(map(pd.read_csv, [file1, file2]), ignore_index=True)
    df = Extract(raw_data_df)
    df = pre_processing(df)
    vizualization(df)
    DBOperations.data_dump_mysql(df)

if __name__ = '__main__':
    file1 = r"C:\Users\Rizwana\Downloads\employee-earnings-report-2012.csv"
    file2 = r"C:\Users\Rizwana\Downloads\employee-earnings-report-2011.csv"
    main(file1, file2)
