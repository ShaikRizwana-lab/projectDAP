import imghdr
import os
import magic
import csv, json
import pandas as pd

def check_type(file):
    file_type = imghdr.what(file)
    print(file_type)
    if file_type == "csv":
        return(file_type)
    elif file_type == "json":
        return(file_type)
    else:
        print("the file is neither json or csv")
        return(file_type)

"""def csv_to_json(csv_file):
    df = pd.read_csv (csv_file)
    
    with open('data.json', 'w') as outfile:
        df.to_json (r)
"""