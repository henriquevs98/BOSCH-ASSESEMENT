
import os
import json
import logging
import pandas as pd

from app import logger

# Load logging settings
logger.logger()


# Function to transform a column data returned from NHSTA to a list
def nhtsa_to_list(response, column):
    try:
        # Convert the byte string to a regular string
        json_str = response.content.decode('utf-8')
        # Convert the JSON string to a Python dictionary
        dict = json.loads(json_str)
        # Extract the 'results' data from the dictionary
        results = dict['results']
        # Create a Pandas DataFrame from the 'results' data
        df = pd.DataFrame(results)
        logging.debug(f'Process {os.getpid()}: information read as DataFrame')

        if not df.empty:
            # Convert the specified column to a Python list
            list_df = df[column].tolist()
            logging.debug(f'Process {os.getpid()}: extracted {column} column values to list')
        else:
            list_df = []
        
        return list_df
    
    except Exception as e:
        logging.error(f'Process {os.getpid()}: Error occurred using nhtsa_to_list(): {e}')


# Function to transform all complaints data from NHSTA to a df
def nhtsa_to_df(response):
    try:
        # Convert the byte string to a regular string
        json_str = response.content.decode('utf-8')
        # Convert the JSON string to a Python dictionary
        dict = json.loads(json_str)
        # Normalize the 'results' column
        df_results = pd.json_normalize(dict['results'])
        logging.debug(f'Normalized results dictionary key into a df with {df_results.shape[0]} lines')

        # Check if 'products' exist in df_results
        if 'products' in df_results.columns:
            # Normalize the 'products' subkeys columns into a separate Pandas DataFrame
            df_products = pd.json_normalize(df_results['products'].explode())
            # Join the two dataframes on the index and give a _product sufix  if column name repeats
            df = df_results.join(df_products, rsuffix='_product')
            # Drop the 'products' column
            df = df.drop('products', axis=1)
            logging.debug(f'Process {os.getpid()}: Normalized subdictionary keys of products into a df and joined with results')
        else:
            # Handle the case where the 'products' column does not exist
            df = df_results.copy()
            logging.debug(f'Process {os.getpid()}: Created df with columns as results dictionary keys only')

        return df

    except Exception as e:
        logging.error(f'Process {os.getpid()}: error occurred using nhtsa_to_df(): {e}')
