import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

############################Read Health data CSV##########################################
df =pd.read_csv(r'D:\dap1\projectDAP\Datasets\data01.csv')
print(df.head())
############################EDA###########################################################
df.describe()
df.isnull().sum()
############################Preprocessing#################################################
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


if __name__ == '__main__':
    new_df = preProcessing(df)
    df_1 = visualizations(new_df)
    print(df_1['BMI'])
    scatterPlot(df_1, "BMI", "COPD", "COPD dist")
    scatterPlot(df_1, "age", "CHD with no MI", "CHD with no MI")
    
    














