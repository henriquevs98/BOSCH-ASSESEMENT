
import os
import re
import json
import logging
import warnings
import numpy as np
import pandas as pd
from io import StringIO

from app import logger
from app.libs import cleaner

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
        logging.error(f'Process {os.getpid()}: error occurred using nhtsa_to_list(): {e}')


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


# Function to transform a csv GET response to df
def csv_response_to_df(response):
    try:
        # Decode bytes to string
        content = response.content.decode('utf-8')
        # Use StringIO to convert the string to a file-like object
        file = StringIO(content)
        # Ignore dtype pd non relevant warnings
        warnings.filterwarnings('ignore')
        # Read the file-like object as a DataFrame
        df = pd.read_csv(file)
        # Restore warnings
        warnings.filterwarnings('default')

        return df

    except Exception as e:
        logging.error(f'Error occurred using csv_response_to_df(): {e}')


# Function to map column values based on key:values pair from dict
def map_column_values(df, column, new_column, dict):
    try:
        # Add mapping for missing values
        dict[np.nan] = 'Unknown'
        # Map the values in the new_column column
        df[new_column] = df[column].map(dict)
        logging.debug(f'Mapped values from column {column} with: {dict}')
        # Drop the column column after mapping to new new_column column
        df = cleaner.rm_columns(df, [column])

        return df

    except Exception as e:
        logging.error(f'Error occurred using map_fuel_type(): {e}')


# Function to create an adress using {street_adress}, {city}, {state}, {zip}-{plus4}
def stations_create_adress(df):
    try:
        # Construct full adress handling null cases
        df['station_address'] = (df['street_address'].fillna('') +
                                (', ' + df['city'] if not df['city'].isna().all() else '') +
                                (', ' + df['state'] if not df['state'].isna().all() else '') +
                                (', ' + df['zip'].fillna('').astype(str) if not df['zip'].isna().all() else '') +
                                ('-' + df['plus4'].fillna('').astype(str) if not df['plus4'].isna().all() else '') +
                                (', ' + df['country'] if not df['country'].isna().all() else ''))
        logging.debug('Created station_adress column using columns street_adress, city, state, zip-plus4')
        # Drop columns that were merged into the new adress column
        df = cleaner.rm_columns(df, ['street_address', 'city', 'state', 'zip', 'plus4', 'country'])

        return df

    except Exception as e:
        logging.error(f'Error occurred using create_adress(): {e}')


# Function to create a POINT() function to format the lat(y) long(x) values as a point in GIS
def stations_create_point(df):
    try:
        # Check and delete rows that lat or long null values
        df = cleaner.stations_empty_point(df)
        # Concatenate lat an long to a new column that is a GIS readable format
        df['station_location'] = 'POINT(' + df['longitude'].round(5).astype(str) + ' ' + df['latitude'].round(5).astype(str) + ')'
        logging.debug('Created station_location column by merging latitude and longitude column values to a POINT() function')
        # Drop columns that were merged into the new adress column
        df = cleaner.rm_columns(df, ['latitude', 'longitude'])

        return df

    except Exception as e:
        logging.error(f'Error occurred using create_point(): {e}')


# Function to convert column from df to datetime type
def convert_to_datetime(df, column_name):
    try:
        df[column_name] = pd.to_datetime(df[column_name])
        logging.debug(f'Converted {column_name} to datetime dtype')

        return df

    except Exception as e:
        logging.error(f'Error occurred using create_point(): {e}')
