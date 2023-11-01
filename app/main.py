from app import logger
from app.libs import complaints, fueleco, stations, loader, cleaner, transformer
from app.data import stations_mapping

# Load logging settings
logger.logger()

# Get Vehicle Complaints Information dataset as a pandas df
df_all_complaints = complaints.get_all_complaints()
# Save the dataframe as a csv 
loader.df_to_csv(df_all_complaints, 'app/data/complaints/extracted/complaints.csv')
#Profile df in order to analyze what to clean
cleaner.profile_df(df_all_complaints, 'Vehicle Fuel Economy Information', 'app/data/fueleco/profile_fueleco.html')


# Get Alternative Fuel Stations dataset as a pandas df
df_all_stations = stations.get_stations()
# Save the extracted dataframe as a csv 
loader.df_to_csv(df_all_stations, 'app/data/stations/extracted/stations.csv')
import pandas as pd
df_all_stations = pd.read_csv('app/data/stations/extracted/stations.csv', sep=';')

# Separate column names with _ and lowercase
df_all_stations = cleaner.fix_columns(df_all_stations)
# Remove deprecated columns (as indicated in documentation)
df_all_stations = cleaner.rm_columns(df_all_stations, stations_mapping.deprecated_columns)
# Remove columns that arent too revelant
df_all_stations = cleaner.rm_columns(df_all_stations, stations_mapping.trash_columns)

# Map status_code values according to documentation to new status column and drop column used
df_all_stations = transformer.map_column_values(df_all_stations, 'status_code', 'status', stations_mapping.status_map)
# Map owner_type_code values according to documentation to new owner_type column and drop column used
df_all_stations = transformer.map_column_values(df_all_stations, 'owner_type_code', 'owner_type', stations_mapping.owner_type_map)
# Map fuel_type_coded values according to documentation to new fuel_type column and drop column used
df_all_stations = transformer.map_column_values(df_all_stations, 'fuel_type_code', 'fuel_type', stations_mapping.fuel_type_map)
# Map facility_type values according to documentation to new facility column and drop column used
df_all_stations = transformer.map_column_values(df_all_stations, 'facility_type', 'facility', stations_mapping.facility_map)
# Map maximum_vehicle_class values according to documentation to new maximum_class column and drop column used
df_all_stations = transformer.map_column_values(df_all_stations, 'maximum_vehicle_class', 'maximum_class', stations_mapping.maximum_vehicle_map)

# Create adress column cleaning street_adress, city, state, zip-plus4 and drop columns used
df_all_stations = transformer.stations_create_adress(df_all_stations)
# Create a GIS readable point location column named adress using lat and long and drop columns used
df_all_stations = transformer.stations_create_point(df_all_stations)
# Drop rows where station_name is Null
df_all_stations = df_all_stations.dropna(subset=['station_name'])
# Clean possible null values in str columns
df_all_stations = cleaner.replace_null_values(df_all_stations, ['access_code', 'station_address'], 'Unknown')

# Save the processed dataframe as a csv 
loader.df_to_csv(df_all_stations, 'app/data/stations/processed/stations.csv')


# Get Vehicle Fuel Economy Information dataset as a pandas df
df_all_fueleco = fueleco.get_fueleco()
# Save the dataframe as a csv 
loader.df_to_csv(df_all_fueleco, 'app/data/fueleco/extracted/fueleco.csv')
