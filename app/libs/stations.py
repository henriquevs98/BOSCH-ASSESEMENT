import logging
import pandas as pd

from app import logger
from app.libs import extractor, transformer

# Load logging settings
logger.logger()


def get_stations():
    try:
        logging.info('Extracting Alternative Fuel Stations dataset using National Renewable Energy Laboratory API..')
        # Set API key to access service
        api_key = 'QGA2Cv1kjJhzxeO6iaKhRCHsX24I8p7FhpSg1sQW'
        # Define the URL for the NREL API endpoint that returns the full dataset
        response = extractor.extract_response(f'https://developer.nrel.gov/api/alt-fuel-stations/v1.csv?api_key={api_key}')
        df_stations = transformer.csv_response_to_df(response)
        logging.info(f'Extracted {df_stations.shape[0]} records for Alternative Fuel Stations dataset')

        return df_stations

    except Exception as e:
        logging.error(f'An error occurred when using get_stations(): {e}')
