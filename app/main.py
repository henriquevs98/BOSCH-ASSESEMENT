import toml
from fastapi import FastAPI
from api import complaints_extraction, complaints_transformation, complaints_loading, stations_extraction, stations_transformation, stations_loading, fuel_extraction, fuel_loading


CONFIG = toml.load('docs/fastapi.toml')

app = FastAPI(title='Bosch Assesment', description=CONFIG['endpoints']['description'])


# Endpoint to extract data related to complaints
@app.get('/extraction/complaints', tags=['Extraction'], description=CONFIG['complaints_extraction']['description'])
def extract_vehicle_complaints_dataset():
    df = complaints_extraction()

    return {'Number of rows extracted': df.shape[0]}


# Endpoint to transform data related to complaints
@app.get('/transformation/complaints', tags=['Transformation'], description=CONFIG['complaints_transformation']['description'])
def transform_vehicle_complaints_dataset():
    complaints_transformation()

    return {'message': 'Completed complaints dataset transformation'}


# Endpoint to load data related to complaints
@app.get('/loading/complaints', tags=['Loading'], description=CONFIG['complaints_loading']['description'])
def load_vehicle_complaints_transformed_dataset():
    df = complaints_transformation()
    complaints_loading(df)

    return {'message': 'Completed complaints dataset loading to Google BigQuery'}


# Endpoint to extract data related to stations
@app.get('/extraction/stations', tags=['Extraction'], description=CONFIG['stations_extraction']['description'])
def extract_alternative_fuel_stations_dataset():
    df = stations_extraction()

    return {'Number of rows extracted': df.shape[0]}


# Endpoint to transform data related to stations
@app.get('/transformation/stations', tags=['Transformation'], description=CONFIG['stations_transformation']['description'])
def transform_alternative_fuel_stations_dataset():
    stations_transformation()

    return {'message': 'Completed stations dataset transformation'}


# Endpoint to load data related to stations
@app.get('/loading/stations', tags=['Loading'], description=CONFIG['stations_loading']['description'])
def load_alternative_fuel_stations__transformed_datasets():
    df_dict = stations_transformation()
    stations_loading(df_dict)

    return {'message': 'Completed stations dataset loading to Google BigQuery'}


# Endpoint to extract data related to fuel
@app.get('/extraction/fuel', tags=['Extraction'], description=CONFIG['fuel__extraction']['description'])
def extract_vehicle_fuel_info_dataset():
    df = fuel_extraction()

    return {'Number of rows extracted': df.shape[0]}


# Endpoint to load data related to fuel
@app.get('/loading/fuel', tags=['Loading'], description=CONFIG['fuel__loading']['description'])
def load_alternative_fuel_stations__transformed_datasets():
    df = fuel_extraction()
    fuel_loading()

    return {'message': 'Completed stations dataset loading to Google BigQuery'}
