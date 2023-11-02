import logging
from google.cloud import bigquery
from google.oauth2 import service_account

from utils import logger

# Load logging settings
logger.logger()


# Function to create Google BigQuery Engine
def SessionBigQuery():
    try:
        bq_credentials = service_account.Credentials.from_service_account_file('app/secrets/SA.json', 
                                                                               scopes=['https://www.googleapis.com/auth/cloud-platform'])
        bq_client = bigquery.Client(credentials=bq_credentials)

        return bq_credentials, bq_client

    except Exception as e:
        logging.error(f'Error creating engine for BigQuery using SessionBigQuery: {e}')
