import pandas as pd
import requests,json
import logging
import multiprocessing
import os
from app.logger import logger

# Load logging settings
logger()


def nhtsa_extract(url):
    try:
        # Send an HTTP GET request to the API endpoint and store the response
        response = requests.get(url, stream=False)
        
        return response

    except Exception as e:
        logging.error(f'Process {os.getpid()}: An error occurred when using nhtsa_extract(): {e}')


# Function to transform a column data returned from a get url to a list
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


# Function to transform all complaints data to a df
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


# Function to get all Model Years as a list
def get_years():
    logging.info(f'Extracting years using NHTSA API...')
    # Define the URL for the NHTSA API endpoint that returns a list of model years with complaints information
    response = nhtsa_extract('https://api.nhtsa.gov/products/vehicle/modelYears?issueType=c')
    # Clean info extracted to a list of years
    list_years = nhtsa_to_list(response, 'modelYear')
    logging.info(f'Extracted {len(list_years)} years')
    logging.debug(f'Extracted years: {list_years}')

    return list_years


# Function to get all Makes (car brands) for a Model Year as a list
def get_makes(modelyear):
    logging.debug(f'Process {os.getpid()}: extracting makes that have complaints for {modelyear} using NHTSA API...')
    # Define the URL for the NHTSA API endpoint that returns a list of makes for each year with complaints information
    response = nhtsa_extract(f'https://api.nhtsa.gov/products/vehicle/makes?modelYear={modelyear}&issueType=r')
    # Clean info extracted to a list of makes
    list_makes = nhtsa_to_list(response, 'make')
    logging.debug(f'Process {os.getpid()}: extracted {len(list_makes)} makes')

    return list_makes


# Get all Models for the Make and Model Year
def get_models(modelyear, make):
    logging.debug(f'Process {os.getpid()}: extracting models that have complaints for {modelyear} and {make} using NHTSA API...')
    # Define the URL for the NHTSA API endpoint that returns a list of models for each year and make with complaints information
    response = nhtsa_extract(f'https://api.nhtsa.gov/products/vehicle/models?modelYear={modelyear}&make={make}&issueType=c')
    # Clean info extracted to a list of models
    list_models = nhtsa_to_list(response, 'model')
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


# Get all complaints for the selected Model Year, Make, Model 
def get_complaints(make, model, modelyear):
        logging.debug(f'Process {os.getpid()}: extracting {make}, {model}, {modelyear} complaints')
        # Define the URL for the NHTSA API endpoint that returns all information in complaints dataset for a specific vehicle
        response = nhtsa_extract(f'https://api.nhtsa.gov/complaints/complaintsByVehicle?make={make}&model={model}&modelYear={modelyear}')
        # Clean info extracted to a df
        df_complaints = nhtsa_to_df(response)  

        logging.debug(f'Process {os.getpid()}: extracted {df_complaints.shape[0]} complaints]')

        return df_complaints


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


# Function to get complaints in a paralalized way as a df
def parallel_get_complaints(combinations):
    try:
        logging.info('Extracting complaints according to combinations from NHTSA API...')
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
