import os
import requests
import logging
from app import logger

# Load logging settings
logger.logger()


def extract_response(url):
    try:
        # Send an HTTP GET request to the API endpoint and store the response
        response = requests.get(url, stream=False)
        
        return response

    except Exception as e:
        logging.error(f'Process {os.getpid()}: An error occurred when using extract_response(): {e}')
