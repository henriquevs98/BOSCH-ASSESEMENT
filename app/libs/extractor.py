import os
import requests
import logging
import pandas as pd
import multiprocessing

from utils import logger
from libs import transformer

# Load logging settings
logger.logger()


# Function to extract an url response
def extract_response(url):
    try:
        # Send an HTTP GET request to the API endpoint and store the response
        response = requests.get(url, stream=False)
        
        return response

    except Exception as e:
        logging.error(f'Process {os.getpid()}: error occurred using extract_response(): {e}')


# Function to get Fuel Stations dataset using National Renewable Energy Laboratory API
def get_stations():
    try:
        logging.info('Extracting Alternative Fuel Stations dataset using National Renewable Energy Laboratory API..')
        # Set API key to access service
        api_key = 'QGA2Cv1kjJhzxeO6iaKhRCHsX24I8p7FhpSg1sQW'
        # Define the URL for the NREL API endpoint that returns the full dataset
        response = extract_response(f'https://developer.nrel.gov/api/alt-fuel-stations/v1.csv?api_key={api_key}')
        df_stations = transformer.csv_response_to_df(response)
        logging.info(f'Extracted {df_stations.shape[0]} records for Alternative Fuel Stations dataset')

        return df_stations

    except Exception as e:
        logging.error(f'Error occurred using get_stations(): {e}')


# Function to get the Vehicle Fuel Economy Information dataset from a URL
def get_fuel():
    try:
        logging.info('Extracting Vehicle Fuel Economy Information dataset from fueleconomy.gov...')
        # Define the URL of the dataset
        response = extract_response('https://www.fueleconomy.gov/feg/epadata/vehicles.csv')
        df_fuel = transformer.csv_response_to_df(response)


        logging.info(f'Extracted {df_fuel.shape[0]} records for Vehicle Fuel Economy Information dataset')

        return df_fuel

    except Exception as e:
        logging.error(f'Error occurred using get_fuel(): {e}')


# Function to get all Model Years as a list
def get_years():
    logging.info(f'Extracting years using NHTSA API...')
    # Define the URL for the NHTSA API endpoint that returns a list of model years with complaints information
    response = extract_response('https://api.nhtsa.gov/products/vehicle/modelYears?issueType=c')
    # Clean info extracted to a list of years
    list_years = transformer.complaints_to_list(response, 'modelYear')
    logging.info(f'Extracted {len(list_years)} years')
    logging.debug(f'Extracted years: {list_years}')

    return list_years


# Function to get all Makes (car brands) for a Model Year as a list
def get_makes(modelyear):
    logging.debug(f'Process {os.getpid()}: extracting makes that have complaints for {modelyear} using NHTSA API...')
    # Define the URL for the NHTSA API endpoint that returns a list of makes for each year with complaints information
    response = extract_response(f'https://api.nhtsa.gov/products/vehicle/makes?modelYear={modelyear}&issueType=r')
    # Clean info extracted to a list of makes
    list_makes = transformer.complaints_to_list(response, 'make')
    logging.debug(f'Process {os.getpid()}: extracted {len(list_makes)} makes')

    return list_makes


# Get all Models for the Make and Model Year
def get_models(modelyear, make):
    logging.debug(f'Process {os.getpid()}: extracting models that have complaints for {modelyear} and {make} using NHTSA API...')
    # Define the URL for the NHTSA API endpoint that returns a list of models for each year and make with complaints information
    response = extract_response(f'https://api.nhtsa.gov/products/vehicle/models?modelYear={modelyear}&make={make}&issueType=c')
    # Clean info extracted to a list of models
    list_models = transformer.complaints_to_list(response, 'model')
    logging.debug(f'Process {os.getpid()}: extracted {len(list_models)} makes')

    return list_models


# Function to extract all possible combinations of make, model, and year for a given year and appends them to a list
def get_combinations_by_year(year, combinations):
    try:
        logging.debug(f'Process {os.getpid()}: extracting combinations for {year} using NHTSA API...')

        for make in get_makes(year):
            count = 0
            for model in get_models(year, make):
                # Only append non-empty combinations
                if make and model:
                    combinations.append((make, model, year))
                    logging.debug(f'Process {os.getpid()}: extracted (make: {make}, model: {model}, year:{year})')
                    count += 1

            logging.debug(f'Process {os.getpid()}: extracted {count} combinations for {year}')

    except Exception as e:
        logging.error(f'Process {os.getpid()}: error occurred using get_combinations_by_year(): {e}')


# Function that uses multiprocessing to extract combinations for multiple years in parallel
def parallel_get_combinations_by_year(list_years):
    try:
        logging.info('Extracting and creating (make, model, year) combinations based of years using NHTSA API...')
        # Get the maximum number of workers to use based on cpu count
        max_workers = multiprocessing.cpu_count()

        # Use a multiprocessing Manager to create a shared list for storing the combinations
        with multiprocessing.Manager() as manager:
            combinations = manager.list()
            # Create a pool of processes to execute the function in parallel
            pool = multiprocessing.Pool(processes=max_workers)
            # Use starmap to pass each year and the shared list to the function
            pool.starmap(get_combinations_by_year, [(year, combinations) for year in list_years])
            # Close the pool of processes and wait for them to finish
            pool.close()
            pool.join()
            # Convert the shared list to a regular list 
            combinations = list(combinations)
            logging.info(f'Extracted {len(combinations)} combinations')

            return combinations
   
    except Exception as e:
        logging.error(f'Error occurred using parallel_get_combinations_by_year(): {e}')


# Get all complaints for the selected Model Year, Make, Model 
def get_complaints(make, model, modelyear):
        logging.debug(f'Process {os.getpid()}: extracting {make}, {model}, {modelyear} complaints')
        # Define the URL for the NHTSA API endpoint that returns all information in complaints dataset for a specific vehicle
        response = extract_response(f'https://api.nhtsa.gov/complaints/complaintsByVehicle?make={make}&model={model}&modelYear={modelyear}')
        # Clean info extracted to a df
        df_complaints = transformer.complaints_to_df(response)  

        logging.debug(f'Process {os.getpid()}: extracted {df_complaints.shape[0]} complaints]')

        return df_complaints


# Function to get complaints in a paralalized way as a df
def parallel_get_complaints(combinations):
    try:
        logging.info('Extracting Vehicle Complaints dataset according to combinations from NHTSA API...')
        # Define the number of processes to use
        num_processes = multiprocessing.cpu_count()

        # Create a pool of processes to execute the function in parallel
        with multiprocessing.Pool(processes=num_processes) as pool:
            # Use starmap to pass each tuple as separate arguments to the function and return df to results
            results = pool.starmap(get_complaints, [(make, model, year) for make, model, year in combinations])

        # Concatenate the results into a final DataFrame
        df_all_complaints = pd.concat(results)
        logging.info(f'Extracted {df_all_complaints.shape[0]} complaints')

        return df_all_complaints

    except Exception as e:
        logging.error(f'Error occurred using parallel_get_complaints(): {e}')


# Function to orchestrate parallelization and return final df with complaints
def get_all_complaints():
    try:
        # Empty DataFrame to append all info
        df_all_complaints = pd.DataFrame()
        # Retrieve all years
        list_years = get_years()
        # Create a list of all possible combinations of make, model, and year
        combinations = parallel_get_combinations_by_year(list_years)
        # Use list created to get all complaints
        df_all_complaints = parallel_get_complaints(combinations)

        return df_all_complaints

    except Exception as e:
        logging.error(f'Error occurred using get_all_complaints(): {e}')
