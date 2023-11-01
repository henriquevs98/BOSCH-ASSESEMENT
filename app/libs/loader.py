import logging


# Function to save a df to a csv file
def df_to_csv(df, dir):
    try:
        logging.info(f'Saving full result as csv file at {dir}...')
        df.to_csv(dir, index=False, sep=';')
        logging.info(f'Saved {df.shape[0]} lines on csv file at {dir}')

    except Exception as e:
        logging.error(f'An error occurred when using save_to_csv(): {e}')
