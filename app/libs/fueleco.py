import logging
import pandas as pd

from app import logger
from app.libs import extractor, transformer

# Load logging settings
logger.logger()


def get_fueleco():
    """
    Downloads the Vehicle Fuel Economy Information dataset from the URL
    https://www.fueleconomy.gov/feg/epadata/vehicles.csv and returns it as a pandas DataFrame.
    """
    try:
        logging.info('Extracting Vehicle Fuel Economy Information dataset from fueleconomy.gov...')
        # Define the URL of the dataset
        response = extractor.extract_response('https://www.fueleconomy.gov/feg/epadata/vehicles.csv')
        df_fueleco = transformer.csv_response_to_df(response)


        logging.info(f'Extracted {df_fueleco.shape[0]} records for Vehicle Fuel Economy Information dataset')

        return df_fueleco

    except Exception as e:
        logging.error(f'An error occurred when using nhtsa_extract(): {e}')
