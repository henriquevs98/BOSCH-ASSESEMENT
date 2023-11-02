import logging
import pandas as pd
from google.cloud import bigquery

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
        logging.error(f'Error occurred using df_to_csv(): {e}')


# Function to read a csv as a df
def csv_to_df(dir):
    try:
        logging.info(f'Reading csv file from {dir}...')
        df = pd.read_csv(dir, sep=';')
        logging.info(f'Sucessfully read {df.shape[0]} lines of csv file from {dir}')

        return df

    except Exception as e:
        logging.error(f'Error occurred using csv_to_df(): {e}')


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
        logging.error(f'Error occurred using dict_dfs_to_csv(): {e}')


# Function to send df to Google BigQuery
def df_to_gcp(df, tablename, bq_schema, bq_client, insertion='truncate'):
    try:
        if insertion == 'truncate':  # check if the files in database are from this execution to append new info. If not, replace them
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
            logging.debug(f'Dropping {tablename} old table in BigQuery {bq_schema}')
            logging.debug(f'Creating new {tablename} table in BigQuery {bq_schema}')
            job = bq_client.load_table_from_dataframe(df, f'{bq_schema}.{tablename}', job_config=job_config)  
            job.result()  
            logging.info(f'Sent {len(df.index)} rows to BigQuery as {tablename}')
        else:
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            job = bq_client.load_table_from_dataframe(df, f'{bq_schema}.{tablename}', job_config=job_config)  
            job.result()  
            logging.info(f'Sent {len(df.index)} rows to BigQuery as {tablename}')

    except Exception as e:
        logging.error(f'Error occurred using df_to_gcp(): {e}')


# Function to send a dict of dfs to Google BigQuery
def dict_df_to_gcp(dict , bq_schema, bq_client, insertion='append'):
    
    for info, df in dict.items():
        try:
            name = info.lower().replace(' ',  '_')
            # Create a unique name that explicits the fuel type
            tablename = f'stations_{name}.csv'
            df_to_gcp(df, tablename, bq_schema, bq_client, insertion)

        except Exception as e:
            logging.error('Error occurred when using dict_df_to_gcp: {e}')
