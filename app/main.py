from app.libs import nhtsa
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
# Create a StreamHandler to print log messages to the console
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
# Create a Formatter to format log messages
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
# Add the Formatter to the StreamHandler
console_handler.setFormatter(formatter)
# Add the StreamHandler to the logger
logging.getLogger().addHandler(console_handler)


# Get complains as a pandas df
df_all_complaints = nhtsa.get_all_complaints()
# Save the dataframe as a csv file with a semicolon delimiter and no quotes
df_all_complaints.to_csv('app/files/nhtsa/extracted/complaints.csv', index=False, sep=';')
