import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

file1 = r"C:\Users\Rizwana\Downloads\employee-earnings-report-2012.csv"
file2 = r"C:\Users\Rizwana\Downloads\employee-earnings-report-2011.csv"

df = pd.concat(map(pd.read_csv, [file1, file2]), ignore_index=True)
#print(len(df))

#checking for all the NA values
#print(df.isnull().sum())

#removing NA and replacing it with 0
df = df.fillna(0)
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

#check the data types of the variables
df.dtypes


#create boxplots to identify outliers
y = list(df.REGULAR)
plt.boxplot(y)
plt.show()


