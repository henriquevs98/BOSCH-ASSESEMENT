from app import logger
from app.libs import complaints, fueleco, stations, loader

# Load logging settings
logger.logger()

df_all_complaints = complaints.get_all_complaints()
# Save the dataframe as a csv 
loader.df_to_csv(df_all_complaints, 'app/files/complaints/extracted/complaints.csv')

# Get fuel economy as a pandas df
df_all_stations = stations.get_stations()
# Save the dataframe as a csv 
loader.df_to_csv(df_all_stations, 'app/files/stations/extracted/stations.csv')

# Get fuel economy as a pandas df
df_all_fueleco = fueleco.get_fueleco()
# Save the dataframe as a csv 
loader.df_to_csv(df_all_fueleco, 'app/files/fueleco/extracted/fueleco.csv')
