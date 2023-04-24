import argparse
from ETL_weather_data import weather_main
from ETL_riz import fianace_main
from pathlib import Path

# Create the parser object
parser = argparse.ArgumentParser()

# Add the arguments
parser.add_argument('param1', type=str, help='weather data path', default='.\Datasets\weather data')
parser.add_argument('param2', type=str, help='MySql Password')
# parser.add_argument('param3', type=str, help='finance data 1 path', default='.\Datasets\finance data')
# parser.add_argument('param4', type=str, help='finance data 2 path', default='.\Datasets\finance data')


# Parse the arguments
args = parser.parse_args()


if __name__ == '__main__':
    weather_main(path=Path(args.param1), sql_password=args.param2)
    # fianace_main(file1 = Path(args.param3), file2=Path(args.param4))

    print('Completed Running all the pipelines')