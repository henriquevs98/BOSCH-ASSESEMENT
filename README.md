# BOSCH-ASSESEMENT


## Introduction

As part of a job assessment, I created a data pipeline to acquire, process, and load data from various public sources related to the automotive industry. To achieve this, I chose to develop a FastAPI application in Python. FastAPI is a modern, fast (high-performance), web framework for building APIs. It was chosen for its simplicity, scalability, and efficiency in handling large data sets. In this document, I will explain the pipeline, what it does, and how to use it. I will also document the decisions made during the data processing and transformation steps, explain the data loading process and the strategy for automation.

As already mentioned, the application consists of several endpoints that provide the necessary functionality to acquire and process the data, transform it into a format suitable for further analysis, and load it into a hypothetical data storage system (Google BigQuery). The endpoints were developed to handle the different sources of data and provide a clear and concise way to access and manipulate the data.

In the following chapters, I will provide a detailed explanation of each dataset processed in the FastAPI application. However, before diving into the specifics of each endpoint, I will provide an initial chapter outlining the creation of the container for the app, how to acess FastAPI, the tree design of the application and the purpose of each folder. This will provide a better understanding of the application's structure and how the different components interact with each other.

Subsequently, each chapter will focus on one of the datasets required for the project, including the [U.S. Department of Transportation Vehicle Complaints](https://www.nhtsa.gov/nhtsa-datasets-and-apis) dataset, [U.S. Department of Energy Alternative Fuel Stations](https://afdc.energy.gov/data_download) dataset, and [U.S. Environmental Protection Agency Vehicle Fuel Economy Information](https://www.fueleconomy.gov/feg/download.shtml/) dataset. Each chapter will have subchapters that explain the data extraction and data transformation processes for each dataset. These chapters will provide a detailed explanation of each step taken in the pipeline, including any decisions made during the data processing and transformation steps.

Overall, this documentation aims to provide a comprehensive guide to the FastAPI application, showcasing the thought process and problem-solving skills used in the development of the application.

## Launching the Docker Container

1. Ensure that Docker is installed on your system. You can download Docker from the official website: https://www.docker.com/products/docker-desktop

2. Open a terminal or command prompt and navigate to the root directory of your project.

3. Build the Docker image using the following command:

   ```
   docker build -t <image-name> .
   ```

   Replace `<image-name>` with the name you want to give to your Docker image. The `.` at the end of the command specifies that the Docker build context is the current directory.

4. Once the Docker image has been built, you can launch a container using the following command:

   ```
   docker run -p 80:80 <image-name>
   ```

   This command maps port 80 of the Docker container to port 80 of your local machine, allowing you to access the FastAPI app from a web browser.

   Replace `<image-name>` with the name of the Docker image you just built.

5. The Docker container should now be running. You can verify this by running the following command:

   ```
   docker ps
   ```

   This command should display a list of all running Docker containers, including the one running your FastAPI app.

## Accessing the FastAPI Docs
The endpoints and functionality of the FastAPI application that has been developed can be easily explored and understood through the FastAPI documentation. The documentation provides detailed information about each endpoint, including the expected output data, and where the data comes from.
![image](https://github.com/henriquevs98/BOSCH-ASSESEMENT/assets/110188794/53572a2a-875a-4e5c-b269-25846eb8fb22)


To access the FastAPI documentation, simply navigate to the /docs endpoint of the running FastAPI application in a web browser. For example, if the FastAPI application is running on http://localhost:8000, you can access the documentation by navigating to http://localhost:8000/docs.

1. Open a web browser and navigate to `http://localhost/docs`. This will take you to the FastAPI docs page.

2. The FastAPI docs provide an interactive interface for testing the endpoints of the app. This interface can be used  to send requests to the app and see the responses in real-time.

3. To test an endpoint, expand the endpoint in the sidebar and click on the "Try it out" button.

4. Once the parameters are entered, the "Execute" button can be clicked to send the request. The response will be displayed below the form.

5. FastAPI docs can be used to explore the endpoints of the app and test their functionality. This can be a useful tool for debugging and developing.

## File tree
This initial chapter will provide a more detailed explanation of the purpose of each folder in the file tree. Each dataset required for the project has a subfolder in the data folder, and next chapters chapter will focus on explaining the data extraction, data transformation, and data loading processes for each dataset.

    bosch-assesment
    └───app
        ├───data
        │   ├───complaints
        │   │   ├───extracted
        │   │   └───processed
        │   ├───fuel
        │   │   ├───extracted
        │   │   └───processed
        │   └───stations
        │       ├───extracted
        │       └───processed
        ├───docs
        ├───libs
        ├───secrets
        ├───tests
        └───utils

The file tree above represents the structure of the FastAPI application that was developed for the data pipeline project. Here is a breakdown of each folder's purpose:

- **app**: This folder contains the main code for the FastAPI application, including main.py, api.py, and database.py. main.py uses functions defined in api.py to mount the endpoints of the application.
- **libs**: This folder contains external libraries used in the application to facilitate the ETL process. It includes extractor.py, cleaner.py, loader.py, and transformer.py, which contain various functions used in api.py to extract, clean, transform, and load the data.
- **secrets**: This folder contains a JSON file with an SA key for BQ on editor level, which is required to access the data storage system. This file is gitignored for security reasons.
- **tests**: This folder is left empty for convention purposes, as there was not enough time to implement test scripts for the application.
- **utils**: This folder contains logger.py, which configures logs to be printed in the console.

The **data** folder contains subfolders for each dataset required for the project, including complaints, fuel, and stations. Each of these subfolders contains two additional subfolders, extracted and processed, which hold the raw and cleaned data, respectively (some data might be available in a link format due to storage limitations).

## U.S. Department of Transportation Vehicle Complaints
The [U.S. Department of Transportation Vehicle Complaints](https://www.nhtsa.gov/nhtsa-datasets-and-apis) dataset is one of the three datasets required for the data pipeline project. This dataset contains information on vehicle complaints received by the National Highway Traffic Safety Administration (NHTSA) from consumers either directly or through the Office of Defects Investigation (ODI). The data includes complaints about safety concerns related to vehicles, such as brakes, steering, and airbags, as well as general issues like electrical problems and engine failures. 

This [NHTSA API](https://api.nhtsa.gov/) provides a set of endpoints that can be used to retrieve information on vehicle complaints, including information on the model years, makes, and models that have received complaints, as well as the details of each individual complaint. 

To extract the data, the Python `requests` library is commonly used to send HTTP requests to the API endpoints and retrieve the relevant information. The response from the API is typically in JSON format, which can be easily parsed and converted into a Python object such as a list or a pandas dataframe. The `transformer.complaints_to_list()` and `transformer.complaints_to_df()` functions are examples of tools that can be used to convert the extracted data into a format that is easier to work with, such as a list or a pandas DataFrame.

### Data Extraction
The `extractor.parallel_get_combinations_by_year(list_years)` function uses the `extractor.get_combinations_by_year()` function to create a list of all possible combinations between the model years, makes, and models that have received complaints in parallel. This is done by using the multiprocessing module to create a pool of worker processes, and using the `starmap()` function to apply the `extractor.get_combinations_by_year()` function to each year in the list of model years. The resulting combinations are then appended to a shared list object using a Manager object, which is used to store the combinations across multiple processes.

The `extractor.parallel_get_complaints(combinations)` function uses the `extractor.get_complaints()` function to extract all complaints for each combination of make, model, and model year in parallel. This is done by using the multiprocessing module to create a pool of worker processes, and using the `starmap()` function to apply the `extractor.get_complaints()` function to each combination in the list of combinations. The resulting DataFrames are then concatenated into a single DataFrame using the `pd.concat()` function.

The `extractor.get_all_complaints()` extractor.function uses the `extractor.get_years()`, `extractor.parallel_get_combinations_by_year()`, and `extractor.parallel_get_complaints()` functions to extract all complaints for all available model years, makes, and models in parallel. This is done by first retrieving a list of all available model years using `extractor.get_years()`, then using `extractor.parallel_get_combinations_by_year()` to create a list of all possible combinations of make, model, and model year, and finally using `extractor.parallel_get_complaints()` to extract all complaints for each combination in parallel. The resulting DataFrame containing all complaints is then returned and saved as a csv in `data/complaints/extracted`.

### Data Transformation
The function `api.complaints_transformation()` is a data transformation pipeline that processes a CSV file containing complaints data. The function performs several data cleaning, manipulation, and conversion tasks on the input data to produce a processed CSV file. To simplify the code and make it more readable, various values utilized in the transformation process have been stored in `data.complaints_mapping`.

In main.py, the `api.complaints_transformation()` function is imported and serves as an endpoint for the FastAPI application. This endpoint can be accessed by making a GET request to the `/complaints/transformation` route. The function groups all the necessary functions from `libs` into a single endpoint, making it more comprehensible and easier to use.

Here are the steps performed in the `api.complaints_transformation()` function:

1. Read the input CSV file using the `csv_to_df()` function from the `loader` module.
2. Remove columns that are not relevant to the analysis using the `rm_columns()` function from the `cleaner` module. The columns to be removed are specified in the `trash_columns` list.
3. Fix the date format of the `dateComplaintFiled` column using the `fix_date_columns()` function from the `transformer` module. The column to be fixed is specified in the `fix_date` list.
4. Fix the capitalization of certain string columns using the `fix_capitalize_words()` function from the `transformer` module. The columns to be fixed are specified in the `fix_capitalize` list.
5. Create a new column `complaintYear` that extracts the year from the `dateComplaintFiled` column using the `create_year_column()` function from the `transformer` module.
6. Convert certain columns to datetime format using the `convert_columns_to_datetime()` function from the `transformer` module. The columns to be converted are specified in the `convert_to_datetime` list.
7. Convert certain columns to integer format using the `convert_columns_to_int()` function from the `transformer` module. The columns to be converted are specified in the `convert_to_int` list.
8. Reorder the columns in the dataframe using the `reorder_columns()` function from the `transformer` module. The final column order is specified in the `reorder_columns` list.
9. Create a new column `id` that assigns a unique ID to each row in the dataframe using the `create_id_column()` function from the `transformer` module.
10. Save the processed dataframe as a CSV file using the `df_to_csv()` function from the `loader` module in `data/complaints/processed`.
11. Return the processed dataframe.

Overall, the `api.complaints_transformation()` function takes the raw complaints data and applies a series of transformations to clean and structure it for analysis. The resulting processed CSV file can be used for further analysis or visualization.


## U.S. Department of Energy Alternative Fuel Stations
The [U.S. Department of Energy Alternative Fuel Stations](https://afdc.energy.gov/data_download) dataset contains information on alternative fuel stations across the United States, including electric charging stations, hydrogen fueling stations, compressed natural gas (CNG), liquefied natural gas (LNG) stations, among others. 

To extract the data, the [National Renewable Energy Laboratory API](https://developer.nrel.gov/docs/transportation/) is utilized with the help of a Python function `extractor.get_stations()`. This function sends HTTP requests to the API endpoint and retrieves the relevant information in pandas dataframe format. The `csv_response_to_df()` function from the `transformer` module is then used to convert the data into a pandas DataFrame. 

The Alternative Fuel Stations dataset provides valuable information on the locations and types of alternative fuel stations available in the United States, which can be used to analyze the growth and distribution of alternative fuel infrastructure across the country.

### Data Extraction
The extraction process involved defining the URL for the [NREL API](https://developer.nrel.gov/docs/transportation/) endpoint that returns the full dataset and setting the API key to access the service. Once the response was received, the data was extracted using the `extract_response()` function, and transformed into a pandas DataFrame using the `transformer.csv_response_to_df()` function. The resulting DataFrame contained information about the alternative fuel stations, including their names, locations, fuel types and is also saved as a csv in 'data/stations/extracted/'. 

In main.py, the `api.stations_extraction()` function is imported and serves as an endpoint for the FastAPI application. This endpoint can be accessed by making a GET request to the '/stations/extraction' route. The function groups all the necessary functions from `libs` into a single endpoint, making it more comprehensible and easier to use.

### Data Transformation
The `api.stations_transformation()` function is a data transformation pipeline that processes a CSV file containing information about fueling stations. The function performs several cleaning, manipulation, and conversion tasks on the input data to produce a dictionary of processed dataframes, each corresponding to a specific fuel type. To simplify the code and make it more readable, various values utilized in the transformation process have been stored in `data.stations_mapping`. 

In main.py, the `api.stations_transformation()` function is imported and serves as an endpoint for the FastAPI application. This endpoint can be accessed by making a GET request to the `/stations/transformation` route. The function groups all the necessary functions from `libs` into a single endpoint, making it more comprehensible and easier to use.

Here are the steps performed in the `stations_transformation()` function:

1. Read the input CSV file using the `csv_to_df()` function from the `loader` module.
2. Fix the column names by separating them with underscores and converting them to lowercase using the `fix_columns()` function from the `cleaner` module.
3. Remove deprecated columns (as indicated in the documentation) using the `rm_columns()` function from the `cleaner` module. The columns to be removed are specified in the `deprecated_columns` list.
4. Remove columns that are not relevant to the analysis using the `rm_columns()` function from the `cleaner` module. The columns to be removed are specified in the `trash_columns` list.
5. Map the values of certain columns to new columns using the `map_column_values()` function from the `transformer` module. The columns and mappings to be used are specified in several mapping dictionaries.
6. Create a new `address` column by combining the `street_address`, `city`, `state`, and `zip_plus4` columns using the `stations_create_address()` function from the `transformer` module.
7. Create a new `point` column by combining the `latitude` and `longitude` columns using the `stations_create_point()` function from the `transformer` module.
8. Drop rows where the `station_name` column is null.
9. Clean possible null values in string columns using the `replace_null_values()` function from the `cleaner` module. The columns and replacement value are specified in the `replace_null_col_value` list.
10. Convert the `updated_at` column to datetime format using the `convert_columns_to_datetime()` function from the `transformer` module.
11. Reorder the columns in the dataframe using the `reorder_columns()` function from the `transformer` module. The final column order is specified in the `reorder_columns` list.
12. Create a new `id` column that assigns a unique ID to each row in the dataframe using the `create_id_column()` function from the `transformer` module.
13. Split the dataframe into multiple dataframes based on fuel type using the `clean_divide_df()` function from the `transformer` module. The resulting dictionary has fuel type names as keys and dataframes as values.
14. For each fuel type-specific dataframe:
    a. Perform additional cleaning, manipulation, and conversion tasks as necessary, such as converting columns to integer format, mapping column values, removing quotes from column values, and creating string lists for certain columns.
    b. Reorder the columns in the dataframe using the `reorder_columns()` function from the `transformer` module. The final column order is specified in the fuel type-specific `reorder_columns` lists.
    c. Reassign the updated dataframe to the corresponding key in the dictionary of dataframes.
15. Save each fuel type-specific dataframe as a separate CSV file using the `dict_dfs_to_csv()` function from the `loader` module in `data/stations/processed`.
16. Return the dictionary of processed dataframes.

Overall, the `api.stations_transformation()` function takes the raw fueling station data and applies a series of transformations to clean and structure it for analysis. The resulting dictionary of processed dataframes can be used for further analysis or visualization, with each dataframe corresponding to a specific fuel type.


## U.S. Environmental Protection Agency Vehicle Fuel Economy Information
The [U.S. Environmental Protection Agency Vehicle Fuel Economy Information](https://www.fueleconomy.gov/feg/download.shtml/) dataset is one of the three datasets required for the data pipeline project. This dataset contains information on the fuel economy and environmental impact of vehicles sold in the United States. The data includes information such as make and model, fuel type, city and highway miles per gallon, and CO2 emissions.


### Data Extraction
In the extraction process for this dataset, a function called `extractor.get_fuel()` is used to download the data from the  [U.S. Environmental Protection Agency](https://www.fueleconomy.gov/feg/download.shtml/) website. The function uses the `extract_response()` function from the `extractor` library to get the response from the URL. The response (a unzziped csv file) is then passed to the `csv_response_to_df()` function from the `transformer` library to convert the CSV data into a Pandas dataframe. The resulting dataframe contains information on over 40,000 vehicles and their fuel economy and environmental impact and is saved under `data/fuel/extracted`.

### Data Transformation

Unfortunately, due to professional constraints, the [U.S. Environmental Protection Agency Vehicle Fuel Economy Information](https://www.fueleconomy.gov/feg/download.shtml/) dataset was not transformed as expected. However, I made the best effort to document the extraction function and the overall data pipeline process for this dataset.

If there had been more time, the data transformation process would have likely followed a similar approach to that of the U.S. Department of Energy Alternative Fuel Stations dataset. The transformation process would have involved importing the necessary functions from the libs folder to clean, manipulate, and convert the data.

I hope that the incomplete transformation of this dataset does not significantly impact my evaluation, as I made the best effort to leave everything documented and to provide a clear understanding of the data pipeline process for all the other two datasets.

# Data Loading

The `loader` module provides several functions to save and load dataframes from CSV files and to send dataframes to Google BigQuery. 

The `loader.df_to_gcp()` function sends a dataframe to Google BigQuery, while the `loader.dict_df_to_gcp()` function sends a dictionary of dataframes to Google BigQuery. The `loader.df_to_gcp()` function can insert the dataframe into a new table or replace the contents of an existing table, depending on the value of the `insertion` parameter. The `loader.dict_df_to_gcp()` function sends each dataframe in a dictionary to a separate table in BigQuery using `loader.df_to_gcp()`.

## Why use Google BigQuery?

Google BigQuery is a cloud-based data warehouse that allows to store, process, and analyze large datasets quickly and easily. It is a fully managed platform that requires no infrastructure setup, maintenance, or tuning. 

Some of the reasons why BigQuery is currently one of the best options for storing this types of data include:

- Scalability: BigQuery can handle datasets of any size, from gigabytes to petabytes.
- Speed: BigQuery can process queries on large datasets in seconds or minutes, rather than hours or days.
- Cost-effectiveness: BigQuery offers a pay-as-you-go pricing model, with no upfront costs or long-term commitments.
- Security: BigQuery provides robust security features, including data encryption, access controls, and audit logging.
- Integration: BigQuery has native integrations with a wide range of data sources and dashboarding tools, including Google Data Studio, Grafana, and PowerBI.

## Sending transformed datasets to Google BigQuery

To send the transformed datasets to Google BigQuery, we need to create a BigQuery engine using the `SessionBigQuery()` function from the `database` module. This function pulls the necessary credentials from a service account file located in the `secrets` folder.

Once the engine is created, we can use the `loader.df_to_gcp()` function to send the dataframes directly to Google BigQuery.

The loading endpoints initially perform the data transformations on the extracted data in the `extracted/` directory for each dataset, using the endpoints inside `/extraction/`. The resulting dataframes are then directly used to send information to Google BigQuery. This approach of sending the dataframes directly is much safer than saving the data as CSV files and reopening them again, as there is no guarantee that the data types will stay the same when reopening the CSV files.

The transformed U.S. Department of Transportation Vehicle Complaints dataset can use the `loader.dict_df_to_gcp()` function since it was transformed into multiple columns per fuel type. On the other hand, the Vehicle Fuel Economy Information dataset and Vehicle Complaints dataset can be sent directly using the `df_to_gcp()` function since they are only one dataframe.

## GIS capabilities in Google BigQuery

The `stations_create_point()` function from the `transformer` module was used to create a new `point` column with the latitude and longitude values for each station in the [U.S. Department of Energy Alternative Fuel Stations dataset](https://afdc.energy.gov/data/10347). This column is in the format of `POINT(longitude latitude)`. Google BigQuery has built-in GIS capabilities that allow to work with spatial data such as this. For example, the `ST_GeogPoint()` function can be used to convert the `point` column to a geographic point and then use the `ST_AsText()` function to convert it to a string representation of the point. `ST_Centroid()` function can also be used to calculate the centroid of a set of points.


# Automation Proposal for ETL App with FastAPI and Docker

The ETL app built with FastAPI and Docker can be automated to run on a schedule using a combination of Docker Swarm and crontab. Docker Swarm is a container orchestration tool that allows the management of a cluster of Docker hosts and deploy services across them. Crontab is a time-based job scheduler in Unix-like operating systems that can be used to schedule recurring tasks.

## Proposal

Here's a proposal for automating the ETL app:

1. Set up a Docker Swarm cluster with one or more worker nodes. The worker nodes should have enough resources to run the ETL app containers.

2. Build a Docker image for the ETL app using the Dockerfile.

3. Deploy the Docker image as a service in the Docker Swarm cluster using the `docker service create` command. Set the desired number of replicas to the number of worker nodes in the cluster.

4. Create a crontab file on the host machine that runs the Docker Swarm cluster. The crontab file should include a command to trigger the ETL app at the desired schedule interval. For example, if the ETL app is to run every day at 12:00 PM, the following line can be added to the crontab file:

```
0 12 * * * docker service update --force <service_name>
```

Replace `<service_name>` with the name of the Docker service created in step 3.

5. Save the crontab file and exit the editor. The crontab file will be automatically loaded by the crontab daemon.

6. Test the automation.


### Automation interval
A good approach to regularly extract, transform, and update the Vehicle Complaints, Fuel Stations, and Vehicle Fuel Economy Information datasets in BigQuery is to regularly compare the datasets in the cloud with the ones extracted and transformed by the script. Here are the steps to implement this approach:

1. Create a script that extracts the datasets from their respective sources and transforms them into a format suitable for loading into BigQuery. The script should be able to run on a schedule using a tool such as crontab, therefore using FastAPI might be a good aproach since it creates urls that can be curled.

2. For each dataset, download the latest version of the dataset from the source and compare it to the version currently stored in BigQuery.

3. If the row count is different, use the `df_to_gcp()` function to replace the contents of the corresponding BigQuery table with the newly extracted and transformed dataframe.

4. If the row count is the same, do nothing and exit the script.

5. Set up a cron job to run at a scheduled interval (e.g. daily, weekly, etc.) to trigger a FastAPI endpoint that initiates this logic on a regular basis. It should be aligned with dashboard development or other services that rely on this info.

This approach ensures that the data in BigQuery is regularly updated with the latest information from the source datasets. By comparing the row count of the existing and transformed dataframes, it can quickly determine if the data has been updated and needs to be replaced in BigQuery.


### Benefits of Docker Swarm

Using Docker Swarm to manage the ETL app containers provides several benefits:

- Scalability: Docker Swarm allows to easily scale the ETL app to handle larger datasets or higher traffic.
- High availability: Docker Swarm ensures that the ETL app is always available by automatically restarting failed containers and rebalancing the workload across the cluster.
- Load balancing: Docker Swarm automatically distributes the workload across the cluster to ensure that no single node is overloaded.
- Rolling updates: Docker Swarm allows the update the ETL app without downtime by gradually rolling out updates to the nodes in the cluster.
- Security: Docker Swarm provides built-in security features such as TLS encryption and role-based access control.
