import logging

from utils import logger

# Load logging settings
logger.logger()


# Function to convert column names to lowercase and replace blank spaces with underscores
def fix_columns(df):
    try:
        df.columns = df.columns.str.lower().str.replace(' ', '_')
        logging.debug('Fixed column names')

        return df

    except Exception as e:
        logging.error(f'Error occurred using fix_columns(): {e}')


# Function to remove columns specified in a list
def rm_columns(df, list_columns):
    try:
        df = df.drop(columns=[col for col in list_columns if col in df.columns])
        logging.debug(f'Cleaned columns from df: {list_columns}')

        return df

    except Exception as e:
        logging.error(f'Error occurred using rm_columns(): {e}')


# Function that checks if lat or long are null, if so it drops corresponding lines
def stations_empty_point(df):
    try:
        if df['latitude'].isnull().any() or df['longitude'].isnull().any():
            df = df.dropna(subset=['latitude', 'longitude'])
            logging.debug('The latitude or longitude column contains null values, dropped corresponding rows')
        else:
            logging.debug("No null values where found in latitude or longitude columns")

        return df

    except Exception as e:
        logging.error(f'Error occurred using empty_point(): {e}')


# Function to replace null values in a list of specified columns
def replace_null_values(df, columns, value):
    try:
        for column in columns:
            df[column] = df[column].fillna(value)
            logging.debug(f'Converted {column} null values to {value}')

        return df
    
    except Exception as e:
        logging.error(f'Error occurred using replace_null_values(): {e}')


# Function to remove quotes and replace null values with unknown
def quote_remover(df, cols):
    try:
        for col in cols:
            df[col] = (df[col].
                    apply(lambda x: x.replace('"', '') if isinstance(x, str) else 'Unknown'))
        
        return df
    
    except Exception as e:
        logging.error(f'Error occurred using quote_remover(): {e}')
