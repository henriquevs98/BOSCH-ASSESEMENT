import logging
import pandas as pd


# Function to save a df to a csv file
def df_to_csv(df, dir):
    try:
        logging.info(f'Saving full result as csv file at {dir}...')
        df.to_csv(dir, index=False, sep=';')
        logging.info(f'Saved {df.shape[0]} lines on csv file at {dir}')

    except Exception as e:
        logging.error(f'An error occurred when using df_to_csv(): {e}')


# Function to read a csv as a df
def csv_to_df(dir):
    try:
        logging.info(f'Reading csv file from {dir}...')
        df = pd.read_csv(dir, sep=';')
        logging.info(f'Sucessfully read {df.shape[0]} lines of csv file from {dir}')

    except Exception as e:
        logging.error(f'An error occurred when using csv_to_df(): {e}')