from app import logger
from app.libs import loader, cleaner, transformer, extractor
from app.data import stations_mapping, complaints_mapping

# Load logging settings
logger.logger()

def extraction_complaints():
    # Get Vehicle Complaints Information dataset as a pandas df
    df_all_complaints = extractor.get_all_complaints()
    # Save the dataframe as a csv 
    loader.df_to_csv(df_all_complaints, 'app/data/complaints/extracted/complaints.csv')


def extraction_stations():
    # Get Alternative Fuel Stations dataset as a pandas df
    df_stations = extractor.get_stations()
    # Save the extracted dataframe as a csv 
    loader.df_to_csv(df_stations, 'app/data/stations/extracted/stations.csv')


def extraction_fuel():
    # Get Vehicle Fuel Economy Information dataset as a pandas df
    df_all_fuel = extractor.get_fuel()
    # Save the dataframe as a csv 
    loader.df_to_csv(df_all_fuel, 'app/data/fuel/extracted/fuel.csv')


def transformation_complaints():
    # Read the extracted csv
    df_complaints = loader.csv_to_df('app/data/complaints/extracted/complaints.csv')

    # Remove columns that arent too revelant
    df_complaints = cleaner.rm_columns(df_complaints, complaints_mapping.trash_columns)
    # Fix dates
    df_complaints = transformer.fix_date_columns(df_complaints, complaints_mapping.fix_date)
    # Fix word capitalization
    df_complaints = transformer.fix_capitalize_words(df_complaints, complaints_mapping.fix_capitalize)
    # Create a complaintYear column
    df_complaints = transformer.create_year_column(df_complaints, 'dateComplaintFiled',  'complaintYear')
    # Convert columns types to date
    df_complaints = transformer.convert_columns_to_datetime(df_complaints, complaints_mapping.convert_to_datetime)
    # Convert columns types to int
    df_complaints = transformer.convert_columns_to_int(df_complaints, complaints_mapping.convert_to_int)
    # Reorder columns in df
    df_complaints = transformer.reorder_columns(df_complaints, )
    # Create a column with a unique ID in  the first column index
    df_complaints = transformer.create_id_column(df_complaints)

    # Save the processed dataframe as a csv 
    loader.df_to_csv(df_complaints, 'app/data/complaints/processed/complaints.csv')


def transformation_stations():
    # Read the extracted csv
    df_stations = loader.csv_to_df('app/data/stations/extracted/stations.csv')

    # Separate column names with _ and lowercase
    df_stations = cleaner.fix_columns(df_stations)
    # Remove deprecated columns (as indicated in documentation)
    df_stations = cleaner.rm_columns(df_stations, stations_mapping.deprecated_columns)
    # Remove columns that arent too revelant
    df_stations = cleaner.rm_columns(df_stations, stations_mapping.trash_columns)
    # Map status_code values according to documentation to new status column and drop column used
    df_stations = transformer.map_column_values(df_stations, 'status_code', 'status', stations_mapping.status_map)
    # Map owner_type_code values according to documentation to new owner_type column and drop column used
    df_stations = transformer.map_column_values(df_stations, 'owner_type_code', 'owner_type', stations_mapping.owner_type_map)
    # Map fuel_type_coded values according to documentation to new fuel_type column and drop column used
    df_stations = transformer.map_column_values(df_stations, 'fuel_type_code', 'fuel_type', stations_mapping.fuel_type_map)
    # Map facility_type values according to documentation to new facility column and drop column used
    df_stations = transformer.map_column_values(df_stations, 'facility_type', 'facility', stations_mapping.facility_map)
    # Map maximum_vehicle_class values according to documentation to new maximum_class column and drop column used
    df_stations = transformer.map_column_values(df_stations, 'maximum_vehicle_class', 'maximum_class', stations_mapping.maximum_vehicle_map)
    # Create adress column cleaning street_adress, city, state, zip-plus4 and drop columns used
    df_stations = transformer.stations_create_adress(df_stations)
    # Create a GIS readable point location column named adress using lat and long and drop columns used
    df_stations = transformer.stations_create_point(df_stations)
    # Drop rows where station_name is Null
    df_stations = df_stations.dropna(subset=['station_name'])
    # Clean possible null values in str columns
    df_stations = cleaner.replace_null_values(df_stations, stations_mapping.replace_null_col_value, 'Unknown')
    # Convert updated_at column to datetime dtype
    df_stations = transformer.convert_columns_to_datetime(df_stations, stations_mapping.convert_to_datetime)
    # Reorder columns in df
    df_stations = transformer.reorder_columns(df_stations, stations_mapping.reorder_columns)
    # Create a column with a unique ID in  the first column index
    df_stations = transformer.create_id_column(df_stations)

    dict_dfs_stations = transformer.clean_divide_df(df_stations)
    loader.dict_dfs_to_csv(dict_dfs_stations, 'app/data/stations/processed/')

    # Save the processed dataframe as a csv 
    loader.df_to_csv(df_stations, 'app/data/stations/processed/stations.csv')

    return dict_dfs_stations


dict = transformation_stations()