import toml
from fastapi import FastAPI, Query
from api import complaints_extraction, complaints_transformation, complaints_loading, stations_extraction, stations_transformation, stations_loading, fuel_extraction, fuel_loading
import json

CONFIG = toml.load('docs/fastapi.toml')

app = FastAPI(title='Bosch Assesment', description=CONFIG['endpoints']['description'])


def parse_csv(df):
    res = df.to_json(orient="records")
    parsed = json.loads(res)
    return parsed


# Endpoint to extract data related to complaints
@app.get('/extraction/complaints', tags=['Extraction'], description=CONFIG['complaints_extraction']['description'])
def extract_vehicle_complaints_dataset():
    df = complaints_extraction()

    return {'Number of rows extracted': df.shape[0]}


# Endpoint to transform data related to complaints
@app.get('/transformation/complaints', tags=['Transformation'], description=CONFIG['complaints_transformation']['description'])
def transform_vehicle_complaints_dataset(include_data: bool = Query(False, description=CONFIG['complaints_transformation']['description_query'])):
    df = complaints_transformation()
    
    if include_data:

        return parse_csv(df)
    
    else:

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
def transform_alternative_fuel_stations_dataset(include_data: bool = Query(False, description=CONFIG['stations_transformation']['description_query'])):
    df = stations_transformation()[1]

    if include_data:

        return parse_csv(df)
    
    else:

        return {'message': 'Completed stations dataset transformation'}


# Endpoint to load data related to stations
@app.get('/loading/stations', tags=['Loading'], description=CONFIG['stations_loading']['description'])
def load_alternative_fuel_stations__transformed_datasets():
    df_dict = stations_transformation()[0]
    stations_loading(df_dict)

    return {'message': 'Completed stations dataset loading to Google BigQuery'}


# Endpoint to extract data related to fuel
@app.get('/extraction/fuel', tags=['Extraction'], description=CONFIG['fuel__extraction']['description'])
def extract_vehicle_fuel_info_dataset():
    df = fuel_extraction()

    return {'Number of rows extracted': df.shape[0]}


# Endpoint to load data related to fuel
@app.get('/loading/fuel', tags=['Loading'], description=CONFIG['fuel__loading']['description'])
def load_vehicle_fuel_info_dataset():
    df = fuel_extraction()
    fuel_loading(df)

    return {'message': 'Completed fuel dataset loading to Google BigQuery'}
