# from database import SessionBigQuery
from utils import logger
from libs import loader, cleaner, transformer, extractor
from data import stations_mapping, complaints_mapping

# Load logging settings
logger.logger()

# BigQuery engine connection vars
# bq_credentials, bq_client = SessionBigQuery()


def complaints_extraction():
    # Get Vehicle Complaints Information dataset as a pandas df
    df_all_complaints = extractor.get_all_complaints()
    # Save the dataframe as a csv
    loader.df_to_csv(df_all_complaints, 'data/complaints/extracted/complaints.csv')

    return df_all_complaints


def complaints_transformation():
    # Read the extracted csv
    df_complaints = loader.csv_to_df('data/complaints/extracted/complaints.csv')

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
    df_complaints = transformer.reorder_columns(df_complaints, complaints_mapping.reorder_columns)
    # Create a column with a unique ID in  the first column index
    df_complaints = transformer.create_id_column(df_complaints)

    # Save the processed dataframe as a csv
    loader.df_to_csv(df_complaints, 'data/complaints/processed/complaints.csv')

    return df_complaints


def complaints_loading(df_complaints):
    loader.df_to_gcp(df_complaints, 'complaints','vehicle_complaints', bq_client)


def stations_extraction():
    # Get Alternative Fuel Stations dataset as a pandas df
    df_stations = extractor.get_stations()
    # Save the extracted dataframe as a csv
    loader.df_to_csv(df_stations, 'data/stations/extracted/stations.csv')

    return df_stations


def stations_transformation():
    # Read the extracted csv
    df_stations = loader.csv_to_df('data/stations/extracted/stations.csv')

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
    # Split table into multiple tables according to fuel type as a dictionary of fuelname: df
    dict_dfs_stations = transformer.clean_divide_df(df_stations)

    # Extract Compressed Natural Gas specific df from dict
    df_stations_cng = dict_dfs_stations['Compressed Natural Gas']
    # Convert columns to int32 dtype
    df_stations_cng = transformer.convert_columns_to_int(df_stations_cng, stations_mapping.convert_to_int_cng)
    # Map cng_fill_type_code values according to documentation to new cng_fill_type column and drop column used
    df_stations_cng = transformer.map_column_values(df_stations_cng, 'cng_fill_type_code', 'cng_fill_type', stations_mapping.cng_fill_map)
    # Map cng_on-site_renewable_source values according to documentation to new cng_renewable_source column and drop column used
    df_stations_cng = transformer.map_column_values(df_stations_cng, 'cng_on-site_renewable_source', 'cng_renewable_source', stations_mapping.cng_renewable_map)
    # Clean possible null values in str columns
    df_stations_cng = cleaner.replace_null_values(df_stations_cng, ['cng_renewable_source'], 'Unknown')
    # Reorder columns in df
    df_stations_cng = transformer.reorder_columns(df_stations_cng, stations_mapping.reorder_columns_cng)
    # Reassign the correct df to dictionary
    dict_dfs_stations['Compressed Natural Gas'] = df_stations_cng

    # Extract Ethanol E85 specific df from dict
    df_stations_e85 = dict_dfs_stations['Ethanol E85']
    # Remove quotes from column values
    df_stations_e85 = cleaner.quote_remover(df_stations_e85, ['e85_other_ethanol_blends'])
    # Reassign the correct df to dictionary
    dict_dfs_stations['Ethanol E85'] = df_stations_e85

    # Extract Electric specific df from dict
    df_stations_ev = dict_dfs_stations['Electric']
    # Convert columns to int32 dtype
    df_stations_ev = transformer.convert_columns_to_int(df_stations_ev, ['ev_level1_evse_num', 'ev_level2_evse_num', 'ev_dc_fast_count'])
    # Map ev_on-site_renewable_source values according to documentation to new ev_renewable_source column and drop column used
    df_stations_ev = transformer.map_column_values(df_stations_ev, 'ev_on-site_renewable_source', 'ev_renewable_source', stations_mapping.cng_renewable_map)
    # Clean possible null values in str columns
    df_stations_ev = cleaner.replace_null_values(df_stations_ev, ['ev_renewable_source', 'ev_connector_types'], 'Unknown')
    # Create a string of list values for a list of columns
    df_stations_ev = transformer.parantheses_col(df_stations_ev, ['ev_connector_types'])
    # Reorder columns in df
    df_stations_ev = transformer.reorder_columns(df_stations_ev, stations_mapping.reorder_columns_ev)
    dict_dfs_stations['Electric'] = df_stations_ev

    # Extract Hydrogen specific df from dict
    df_stations_hydrogen = dict_dfs_stations['Hydrogen']
    # Remove quotes from column values
    df_stations_hydrogen = cleaner.quote_remover(df_stations_hydrogen, ['hydrogen_pressures', 'hydrogen_standards'])
    # Reassign the correct df to dictionary
    dict_dfs_stations['Hydrogen'] = df_stations_hydrogen

    # Extract Liquefied Natural Gas specific df from dict
    df_stations_lng = dict_dfs_stations['Liquefied Natural Gas']
    # Map lng_on-site_renewable_source values according to documentation to new cng_renewable_source column and drop column used
    df_stations_lng = transformer.map_column_values(df_stations_lng, 'lng_on-site_renewable_source', 'lng_renewable_source', stations_mapping.cng_renewable_map)
    # Clean possible null values in str columns
    df_stations_lng = cleaner.replace_null_values(df_stations_lng, ['lng_renewable_source'], 'Unknown')
    # Reorder columns in df
    df_stations_lng = transformer.reorder_columns(df_stations_lng, stations_mapping.reorder_columns_lng)
    # Reassign the correct df to dictionary
    dict_dfs_stations['Liquefied Natural Gas'] = df_stations_lng

    # Extract Liquefied Petroleum Gas specific df from dict
    df_stations_lpg = dict_dfs_stations['Liquefied Petroleum Gas']
    # Remove quotes from column values
    df_stations_lpg = cleaner.quote_remover(df_stations_lpg, ['lpg_nozzle_types'])
    # Reassign the correct df to dictionary
    dict_dfs_stations['Liquefied Petroleum Gas'] = df_stations_lpg

    # Extract Renewable Diesel specific df from dict
    df_stations_rd = dict_dfs_stations['Renewable Diesel']
    df_stations_rd = transformer.convert_columns_to_int(df_stations_rd, ['rd_maximum_biodiesel_level'])
    # Map cng_fill_type_code values according to documentation to new cng_fill_type column and drop column used
    df_stations_rd = transformer.map_column_values(df_stations_rd, 'rd_blended_with_biodiesel', 'rd_blended_biodiesel', stations_mapping.rd_blended_map)
    # Reorder columns in df
    df_stations_rd = transformer.reorder_columns(df_stations_rd, stations_mapping.reorder_columns_rd)
    # Remove comma from values
    df_stations_rd['rd_blends'] = df_stations_rd['rd_blends'].str.replace(',', '')
    # Create a string of list values for a list of columns
    df_stations_rd = transformer.parantheses_col(df_stations_rd, ['rd_blends'])
    # Reassign the correct df to dictionary
    dict_dfs_stations['Renewable Diesel'] = df_stations_rd

    loader.dict_dfs_to_csv(dict_dfs_stations, 'data/stations/processed/')

    return dict_dfs_stations, df_stations


def stations_loading(dict_dfs_stations):
    loader.dict_df_to_gcp(dict_dfs_stations, 'alternative_fuel_stations', bq_client)


def fuel_extraction():
    # Get Vehicle Fuel Economy Information dataset as a pandas df
    df_all_fuel = extractor.get_fuel()
    # Save the dataframe as a csv
    loader.df_to_csv(df_all_fuel, 'data/fuel/extracted/fuel.csv')

    return df_all_fuel


def fuel_loading(df_fuel):
    loader.df_to_gcp(df_fuel, 'fuel','fuel_economy', bq_client)
