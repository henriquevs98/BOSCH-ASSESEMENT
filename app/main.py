from app.libs import nhtsa

# Get complains as a pandas df
df_all_complaints = nhtsa.get_all_complaints()
# Save the dataframe as a csv 
nhtsa.save_to_csv(df_all_complaints, 'app/files/nhtsa/extracted/complaints.csv')