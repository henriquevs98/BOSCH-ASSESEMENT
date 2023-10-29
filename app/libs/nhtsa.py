import pandas as pd
import requests,json
import logging


def nhtsa_extract(url):
    try:
        # Send an HTTP GET request to the API endpoint and store the response
        response = requests.get(url)
        logging.debug(f'Extracted response for {url}')
        
        return response

    except Exception as e:
        logging.error(f'An error occurred when using nhtsa_extract(): {e}')


# Function to transform a column data returned from a get url to a list
def nhtsa_to_list(response, column):
    try:
        # Convert the byte string to a regular string
        json_str = response.content.decode('utf-8')
        logging.debug('Converted response json byte string to a regular string')
        # Convert the JSON string to a Python dictionary
        dict = json.loads(json_str)
        logging.debug('Converted json string to dictionary')
        # Extract the 'results' data from the dictionary
        results = dict['results']
        logging.debug('Extracted results key info from dictionary')
        # Create a Pandas DataFrame from the 'results' data
        df = pd.DataFrame(results)
        logging.debug('Information read as DataFrame')

        if not df.empty:
            # Convert the specified column to a Python list
            list_df = df[column].tolist()
        else:
            list_df = []
        
        return list_df
    
    except Exception as e:
        logging.error(f'An error occurred when using nhtsa_to_list(): {e}')


# Function to transform all complaints data to a df
def nhtsa_to_df(response):
    try:
        # Convert the byte string to a regular string
        json_str = response.content.decode('utf-8')
        logging.debug('Converted response json byte string to a regular string')
        # Convert the JSON string to a Python dictionary
        dict = json.loads(json_str)
        logging.debug('Converted json string to dictionary')
        # Normalize the 'results' column
        df_results = pd.json_normalize(dict['results'])
        logging.debug('Normalized results dictionary key into a df')

        # Check if 'products' exist in df_results
        if 'products' in df_results.columns:
            # Normalize the 'products' subkeys columns into a separate Pandas DataFrame
            df_products = pd.json_normalize(df_results['products'].explode())
            # Join the two dataframes on the index and give a _product sufix  if column name repeats
            df = df_results.join(df_products, rsuffix='_product')
            # Drop the 'products' column
            df = df.drop('products', axis=1)
            logging.debug('Normalized subdictionary keys of products into a df')
        else:
            # Handle the case where the 'products' column does not exist
            df = df_results.copy()
            logging.debug('Create df with columns as results keys only')

        return df

    except Exception as e:
        logging.error(f'An error occurred when using nhtsa_to_df(): {e}')


# Function to get all Model Years as a list
def get_years():
    # Define the URL for the NHTSA API endpoint that returns a list of model years with complaints information
    response = nhtsa_extract('https://api.nhtsa.gov/products/vehicle/modelYears?issueType=c')
    # Clean info extracted to a list of years
    list_years = nhtsa_to_list(response, 'modelYear')

    return list_years


# Function to get all Makes (car brands) for a Model Year as a list
def get_makes(modelyear):
    # Define the URL for the NHTSA API endpoint that returns a list of makes for each year with complaints information
    response = nhtsa_extract(f'https://api.nhtsa.gov/products/vehicle/makes?modelYear={modelyear}&issueType=r')
    # Clean info extracted to a list of makes
    list_makes = nhtsa_to_list(response, 'make')

    return list_makes


# Get all Models for the Make and Model Year
def get_models(modelyear, make):
    # Define the URL for the NHTSA API endpoint that returns a list of models for each year and make with complaints information
    response = nhtsa_extract(f'https://api.nhtsa.gov/products/vehicle/models?modelYear={modelyear}&make={make}&issueType=c')
    # Clean info extracted to a list of models
    list_models = nhtsa_to_list(response, 'model')

    return list_models


# Get all complaints for the selected Model Year, Make, Model 
def get_complaints(make, model, modelyear):
        # Define the URL for the NHTSA API endpoint that returns all information in complaints dataset for a specific vehicle
        response = nhtsa_extract(f'https://api.nhtsa.gov/complaints/complaintsByVehicle?make={make}&model={model}&modelYear={modelyear}')
        # Clean info extracted to a df
        df_complaints = nhtsa_to_df(response)  
        logging.info(f'Extracted {df_complaints.shape[0]} {make} {model} complaints for the year of {modelyear}')

        return df_complaints


# Function to get all complaints
def get_all_complaints():
    try:
        logging.info(f'Extracting years that have complaints...')
        # Empty DataFrame to append all info
        df_all_complaints = pd.DataFrame()
        # Retrieve all years
        list_years = get_years()

        # Iterate over years
        for modelyear in list_years:
            logging.info(f'Extracting makes that exist in {modelyear}...')
            # Retrieve a list of car brands that have complaints for a specific year
            list_makes = get_makes(modelyear)

            # Check if list_makes is not empty
            if bool(list_makes):
                # Iterate over the Makes that have complaints for a specific modelyear
                for make in list_makes:
                    # Retrieve a list of car models that have complains for a specific modelyear and make
                    list_models = get_models(modelyear, make)
                
                    # Check if list_models is not empty
                    if bool(list_models):
                        # Iterate over the Models of a specific make that have complaints for a specific modelyear
                        for model in list_models:
                            # Retrieve all complaints for the model of a specific make and year
                            df_complaints = get_complaints(make, model, modelyear)
                            df_all_complaints = pd.concat([df_all_complaints, df_complaints])  
        
        return df_all_complaints

    except Exception as e:
        logging.error(f'An error occurred when using get_all_complaints(): {e}')
