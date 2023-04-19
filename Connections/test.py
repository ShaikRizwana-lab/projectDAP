import pandas as pd


file1 = r"C:\Users\Rizwana\Downloads\employee-earnings-report-2012.csv"
file2 = r"C:\Users\Rizwana\Downloads\employee-earnings-report-2011.csv"

df1 = pd.read_csv(file1)
df2 = pd.read_csv(file2)

print(len(df1)+ len(df2))

df = pd.concat(map(pd.read_csv, [file1, file2]), ignore_index=True)
print(len(df))