import logging
import pandas as pd

from app import logger

# Load logging settings
logger.logger()

# Function to save a df to a csv file
def df_to_csv(df, dir):
    try:
        logging.info(f'Saving full result as csv file at {dir}...')
        df.to_csv(dir, index=False, sep=';', encoding='utf-8')
        logging.info(f'Saved {df.shape[0]} lines on csv file at {dir}')

    except Exception as e:
        logging.error(f'An error occurred when using df_to_csv(): {e}')


# Function to read a csv as a df
def csv_to_df(dir):
    try:
        logging.info(f'Reading csv file from {dir}...')
        df = pd.read_csv(dir, sep=';')
        logging.info(f'Sucessfully read {df.shape[0]} lines of csv file from {dir}')

        return df

    except Exception as e:
        logging.error(f'An error occurred when using csv_to_df(): {e}')


# Function to save a dictionary of dataframes to csv files
def dict_dfs_to_csv(dfs_dict, dir):
    try:
        # Iterate over dfs names stored as keys
        for key in dfs_dict.keys():
            name = key.lower().replace(' ',  '_')
            # Create a unique name that explicits the fuel type
            csvname = f'stations_{name}.csv'
            csvpath = f'{dir}/{csvname}'
            # Retrieve the df in question
            df = dfs_dict[key]
            # Save it as csv
            df_to_csv(df, csvpath)

    except Exception as e:
        logging.error(f'An error occurred when using dict_dfs_to_csv(): {e}')