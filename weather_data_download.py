import pandas as pd
from pathlib import Path
from tqdm import tqdm
import requests
import json

save_path = Path(r"D:\NCI\Database and Analytics\TABA\weather_data_CO_20230420")

if not save_path.exists():
    save_path.mkdir()


def calling_the_api(start_date, end_date):
    api_key = "silverhare75"
    url = "https://aqs.epa.gov/data/api/dailyData/byState"

    params = {
        "email": "syedahmedomers@gmail.com",
        "key": api_key,
        "param": "42101",
        "bdate": start_date,
        "edate": end_date,
        "state": "25"
    }
    try:
        response = requests.get(url, params=params)
    except requests.ConnectionError as error:
        print(error)
    return response, start_date

def saving_the_file(data, file_name, path, format = 'csv'):
    df = pd.DataFrame(data.json()['Data'])
    saving_path = Path(path, file_name)
    if format == 'csv':
        df.to_csv(Path(path, file_name+'.csv'))
    elif format == 'json':
        # data.to_json(Path(path, file_name+'.json'))
        with open(Path(path, file_name+'.json'), "w") as outfile:
            json.dump(data.json()['Data'], outfile)
        


required_dates = {'20010101':'20011230', '20020101':'20021230', '20030101':'20031230', '20040101':'20041230', '20050101':'20051230',
                  '20060101':'20061230', '20070101':'20071230', '20080101':'20081230', '20090101':'20091230', '20100101':'20101230',
                  '20110101':'20111230', '20120101':'20121230'}



for sdate, edate in tqdm(required_dates.items()):
    data, start_date = calling_the_api(sdate, edate)
    saving_the_file(data, start_date, save_path, format='json')
    print("data download is done for:", sdate, edate)