
import os
import re
import json
import logging
import warnings
import numpy as np
import pandas as pd
from io import StringIO

from utils import logger
from libs import cleaner
from data import stations_mapping

# Load logging settings
logger.logger()


# Function to transform a column data returned from NHSTA to a list
def complaints_to_list(response, col):
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
            list_df = df[col].tolist()
            logging.debug(f'Process {os.getpid()}: extracted {col} column values to list')
        else:
            list_df = []
        
        return list_df
    
    except Exception as e:
        logging.error(f'Process {os.getpid()}: error occurred using complaints_to_list(): {e}')


# Function to transform all complaints data from NHSTA to a df
def complaints_to_df(response):
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
        logging.error(f'Process {os.getpid()}: error occurred using complaints_to_df(): {e}')


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
def map_column_values(df, col, new_col, dict):
    try:
        # Add mapping for missing values
        dict[np.nan] = 'Unknown'
        # Map the values in the new_column column
        df[new_col] = df[col].map(dict)
        logging.debug(f'Mapped values from column {col} with: {dict}')
        # Drop the column column after mapping to new new_column column
        df = cleaner.rm_columns(df, [col])

        return df

    except Exception as e:
        logging.error(f'Error occurred using map_fuel_type(): {e}')


# Function to capitalize words in a string and keep words with / or starting with a number the same
def capitalize_words_in_string(s):
    try:
        if isinstance(s, str):
            # Split the string into words
            words = s.split(' ')
            capitalized_words = []

            # Iterate the words
            for word in words:
                if '/' in word or '-' in word:
                    # if words have / or -, keep them as is
                    capitalized_words.append(word.title())
                elif word and word[0].isalpha():
                    # If word starts with a letter, capitalize it
                    capitalized_words.append(word[0].upper() + word[1:].lower())
                else:
                    # If none of the conditions were met, keep the word as is
                    capitalized_words.append(word)

            # Rearrange and return string
            return ' '.join(capitalized_words)

        else:
            # If the input is not a string, convert it to a string and try again
            return capitalize_words_in_string(str(s))

    except Exception as e:
        logging.error(f'Error occurred using capitalize_words_in_string(): {e}')


# Function to fix capitalization values of columns specified in a list
def fix_capitalize_words(df, cols):
    try:
        # Iterate over cols provided
        for col in cols:
            # Correct capitalization
            df[col] = df[col].apply(capitalize_words_in_string)
            logging.debug(f'Fixed {col} words capitalization')

        return df

    except Exception as e:
        logging.error(f'Error occurred using fix_capitalize_words(): {e}')


# Function to create an adress using {street_adress}, {city}, {state}, {zip}-{plus4}
def stations_create_adress(df):
    try:
        # Fix word capitalization in columns before creating clean adress
        df = fix_capitalize_words(df, ['street_address', 'city'])
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
def convert_columns_to_datetime(df, cols):
    try:
        # Iterate over cols provided
        for col in cols:
            # Convert column to datetime
            df[col] = pd.to_datetime(df[col])
            logging.debug(f'Converted {col} to datetime dtype')

        return df

    except Exception as e:
        logging.error(f'Error occurred using convert_to_datetime(): {e}')


# Function that transforms the specified columns in the DataFrame to datetime formatand then converts them to the desired date format.
def fix_date_columns(df, date_cols, date_format='%d/%m/%Y'):
    try:
        # Iterate over cols provided
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            mask = df[col].notnull()
            df.loc[mask, col] = df.loc[mask, col].dt.strftime(date_format)
            logging.debug(f'Fixed {col} date order')
        
        return df

    except Exception as e:
        logging.error(f'Error occurred using fix_date_columns(): {e}')


# Function to create a year column based of a datetime column
def create_year_column(df, col, new_col):
    try:
        # Create new_col with year values from col datetime
        df[new_col] = pd.to_datetime(df[col]).dt.year
        logging.debug(f'Created {new_col} column from {col} column')

        return df

    except Exception as e:
        logging.error(f'Error occurred using create_year_column(): {e}')


# Function to convert columns in a list to object type
def convert_columns_to_string(df, cols):
    try:
        # Iterate over cols provided
        for col in cols:
            # Convert to str
            df[col] = df[col].fillna('').astype(str)
            logging.debug(f'Converted {col} to str dtype')

        return df

    except Exception as e:
        logging.error(f'Error occurred using convert_columns_to_string(): {e}')


# Function to convert columns in a list to int type
def convert_columns_to_int(df, cols, int64=False):
    try:
        # Iterate over cols provided
        for col in cols:
            if int64:
                # Convert to int
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                logging.debug(f'Converted {col} to int64 dtype')
            else:
                # Convert to int
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int32')
                logging.debug(f'Converted {col} to int32 dtype')

        return df

    except Exception as e:
        logging.error(f'Error occurred using convert_columns_to_int(): {e}')


# Function to order a df set of columns based of a list of column name values
def reorder_columns(df, col_order):
    try:
        # Check if each column name in col_order exists in df
        existing_cols = [col for col in col_order if col in df.columns]

        # Order df according to list of existing columns
        df = df[existing_cols]
        logging.debug(f'Reordered columns to {existing_cols}')

        return df

    except Exception as e:
        logging.error(f'Error occurred using reorder_columns(): {e}')


# Function to create an id column as the first column based of index
def create_id_column(df):
    try:
        df.insert(0, 'ID', df.index.astype(int))
        logging.debug(f'Created ID column as unique ID')

        return df

    except Exception as e:
        logging.error(f'Error occurred using create_id_column(): {e}')


# Function to split the dataframe into multiple dataframes based on a column values
def divide_df(df, col):
    try:
        dfs_dict = {}

        for value in df[col].unique():
            dfs_dict[value] = df[df[col] == value]

        logging.debug(f'Divided df into multiple dfs according to value in column {col}')

        return dfs_dict

    except Exception as e:
        logging.error(f'Error occurred using divide_df(): {e}')


# Function to select a key from the dictionary and get all other values in a list
def dict_inverted_value(dictionary, key):
    try:
        inverted_dict = {v: k for k, v in dictionary.items()}  # create an inverted dictionary
        other_values = []

        # Iterate over the inverted dictionary and append all values that are not from the 
        # specified key to a list
        for k, v in inverted_dict.items():
            if k != dictionary[key]:
                other_values.append(k)

        logging.debug(f'Returned all values that are not from {key}: {other_values}')

        return other_values

    except Exception as e:
        logging.error(f'Error occurred using dict_inverted_value(): {e}')


# Function to delete columns with prefixes in the list
def remove_col_byprefix(df, prefixes_to_delete):
    try:
        for prefix in prefixes_to_delete:
            df = df.loc[:, ~df.columns.str.startswith(prefix)]

        logging.debug(f'Deleted columns by prefixes: {prefixes_to_delete}')


        return df

    except Exception as e:
        logging.error(f'Error occurred using remove_col_byprefix(): {e}')


# Function to apply divide_df, dict_inverted_key and remove_col_byprefix 
# to get dfs divided with only its columns of interest
def clean_divide_df(df):
    try:
        new_dict = {}
        df_divided_dict = divide_df(df, 'fuel_type')

        for fuelname, df in df_divided_dict.items():
            list_other_fuels = dict_inverted_value(stations_mapping.fuel_code_map, fuelname)
            new_dict[fuelname] = remove_col_byprefix(df, list_other_fuels)

        logging.debug(f'Divided df into multiple')


        return new_dict

    except Exception as e:
        logging.error(f'Error occurred using clean_divide_df(): {e}')


# Function to turn a col value like str1 str2 str3 into [str1, str2, st3]
def parantheses_col(df, cols):
    try:
        for col in cols:
            # Split values in specified column by spaces, sort them alphabetically, and re-join with commas
            df[col] = df[col].apply(lambda x: ', '.join(sorted(x.split())) if x != 'Unknown' else x)
            # Add square brackets to the values in the specified column
            df[col] = df[col].apply(lambda x: f"[{x}]" if x != 'Unknown' else x)

            logging.debug(f'Separated values with , and added [] for column {col}')


            return df

    except Exception as e:
        logging.error(f'Error occurred using parantheses_col(): {e}')
