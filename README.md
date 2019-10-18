# Udacity Data Engineer Capstone Project: 
## An automated data pipeline for temperature and immigration information.
## Goal of the project
The purpose of this project is to build an ETL pipeline that will be able to provide information to data analysts, immigration and climate researchers e.tc with temperature, population and immigration statistics for different cities. It does this by first extracting temperature, airport, immigration and  demographic data from various datasets, perform some transformation on it and convert the data into json files using Apache Spark that can be then uploaded to a Redshift database. Using Apache Airflow, the json files get migrated to s3, then the data gets uploaded to Redshift, undergoes further transformation and gets loaded to normalized fact and dimension tables using a series of reusable tasks that allow for easy backfills. Finally, data checks are run against the data in the fact and dimension tables so as to catch any discrepancies that might be found in the data.

## Use Cases
Below are some of the use cases of the data collected:
- The data can be used to identify the impact that changes in temperature have on city population, going even further to categorize the impact by gender.
- Immigration researchers can use the data to identify what some of the most common ports of entry are, type of visas that are commonly used, use the arrival and departure times to identify periods of high traffic at the ports of entry, the impact of temperature on the rate on immigration by city.

## Queries
The kind of queries that can be run against this data include:
- Find the average immigration traffic at a given city at a given port of entry by month.
- Find the average age of immigrants at a given port of entry by month and by airline/ or by gender.
- retrieve the average population and average temperature of a given city.

## Database Model
The project comprises of a redshift postgres database in the cluster with staging tables that contain all the data retrieved from the s3 bucket and copied over to the tables. It also contains a fact table `fact_city_data_table` and three dimensional tables namely `dim_airport_table`, `dim_demographic_table` and `dim_visitor_table`. The data model representation for the fact and dimension tables is as below:

### City Fact Table

| Table Column | Data Type | Description |
| -------- | ------------- | --------- |
| city_id (PRIMARY_KEY) | varchar(32)  | auto generated primary key|
|  city_name | varchar(32) | name of city |
| country | varchar(32) | name of country |
| latitude | varchar(10) | latitude value |
| longitude | varchar(10) | longitude value |
| average_temperature | numeric |  average temperature of the city |
| date | date | date of temperature recording |

### Airport Dimension Table

| Table Column | Data Type | Description |
| ------------ | ---------- | --------- |
| airport_id (PRIMARY_KEY) | varchar(50)  | auto generated primary key|
|  airport_code | varchar(50) | airport short code |
| name | varchar(500) | name of airport |
| continent | varchar(50) | continent code |
| country_code | varchar(32) | country code |
| state | varchar(32) |  state code |

### Visitor Dimension Table

| Table Column             | Data Type     | Description |
| -------------------------| ------------- | ----------- |
| visitor_id (PRIMARY_KEY) | varchar(32)   | auto generated primary key|
|  year                    | int4          | year of visit |
| month                    | int4          | month of visit|
| city                     | varchar(32)   | city of visit |
| gender                   | varchar(32)   | gender of visitor |
| arrival_date             | date          |  arrival date of the visitor |
| departure_date           | date          |  departure time of the visitor |
| airline                  | varchar(32)   |  airline code |

### Demographic Dimension Table

| Table Column | Data Type | Description |
| ---------------------------- | ------------- | ------------------------- |
| demographic_id (PRIMARY_KEY) | varchar(100)  | auto generated primary key|
|  city                        | varchar(50)   | city name                 |
| state                        | varchar(50)   | state code                |
| male_population              | int4          | male population numbers by city |
| female_population | int4 | female population numbers by city |
| total_population | int4 |  total population numbers by city |

### Reasons for the model
I settled on the above model since I found that the common data field from all the various datasets is city and with that I could be able to extrapolate the other data fields that I required for the data pipeline. With the fact and dimension tables, I utilized the star schema which is more suitable and optimized for OLAP (Online Analytical Processing) operations.

## Tools and Technologies used
The tools used in this project include:
- __Apache Spark__ - This was needed to process data from the big data SAS and csv files to dataframes and convert them to the more readable json data files. In the process, it maps the columns from the datasets to the relevant columns required for the staging tables and also maps some of the values such as the city codes from the provided `label descriptions` data dictionary to city names in the data generated by spark. To view how this is done, check out this [python helper file](additional_helpers/data_to_json.py) and the data dictionary for [city codes](data/label_descriptions/city_codes.json).
- __Apache Airflow__ - This was required to automate workflows, namely uploading processed json files from the local filesystem to s3, creating the staging, fact and dimension tables, copying the s3 files to the redshift staging table then performing ETL to load the final data to the fact and dimension tables. This is all done by pre-configured dags, the [file upload dag](additional_helpers/file_upload_dag.py) and the [capstone dag](additional_helpers/capstone_dag.py) that both perform a series of tasks.
- __Amazon Redshift__ - The database is located in a redshift cluster that store the data from s3 and the eventual data that gets added to the fact and dimension tables.
- __Amazon S3__ - Stores the json files generated by spark that are uploaded from the local filesystem.

## Datasets Used
The datasets used and sources include:
- __I94 Immigration Data__: This data is retrieved from the US National Tourism and Trade Office and the source can be found [here](https://travel.trade.gov/research/reports/i94/historical/2016.html).
- __World Temperature Data__: This dataset came from Kaggle and you can find out about it [here](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data).
- __U.S. City Demographic Data__: This data comes from OpenSoft [here](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/).
- __Airport Code Table__: This is a simple table of airport codes and corresponding cities that is retrieved from [here](https://datahub.io/core/airport-codes#data)

## Installation
Clone the repo from github by running:
```
$ git clone git@github.com:jonathankamau/udend-capstone-project.git
```
Once cloned, create a virtualenv on your local machine that will be your local development environment:
```
$ virtualenv sparkify-project
$ source sparkify-project/bin/activate
```

If running on your local machine, ensure that you have the following main requirements installed in the virtualenv:
- pyspark
- apache-airflow
- psycopg2

Alternatively you can install them from the provided `requirements.txt` file by running the following in your virtual environment.
```
$ pip install -r requirements.txt
 ```

## Workflow/Process
1. A sample size of the json data files that were processed by spark are already part of this project's files. You can find them in `data/processed-data`. If you would wish to generate them from scratch, you can follow the same step by step process I went through [here](#Steps-to-generate-the-s3-bound-json-data-files)
2. On the terminal, ensure your virtualenv is set then add the AIRFLOW_HOME environment variable.
    ```
    $ export AIRFLOW_HOME=~/path/to/project
    ```
3. Run `airflow initdb` to setup the airflow db locally the run both the `airflow scheduler` and `airflow webserver` commands on seperate terminal windows. If you get a psycopg2 related error, you might need to check if the postgresql service is running on your computer.
4. Once you run the above commands successfully you can open the airflow UI on localhost using port 8080 http://0.0.0.0:8080
5. Navigate to your AWS s3 console and create a bucket named `udend-data`. If you wish to provide another name, ensure you set it in the `capstone_dag` and `file_upload_dag` operator configs for s3_bucket.
6. Create a Redshift cluster with a redshift database. Once it's finished creating, take note of the endpoint and database credentials.
7. Add your AWS and Redshift credentials in the airflow UI. You can accomplish this in the following steps:
    - Click on the __Admin tab__ and select __Connections__.
    - Under __Connections__, select __Create__.
    - In the Create tab enter the following creds:
        - Conn Id: `aws_credentials`
        - Conn Type: `Amazon Web Services`
        - Login: Your `<AWS Access Key ID>`
        - Password: `<Your AWS Secret Access Key>`
    - Once done, click on __Save and Add Another__
    - On the new create page add the following:
        - Conn Id: `redshift`
        - Conn Type: `Postgres`
        - Host: `<Endpointof your redshift cluster>`
        - Schema: `<Redshift database name>`
        - Login: `<Database username>`
        - Password: `<Database password>`
        - Port: `<Database port which is usually 5439>`
    - Click save
8. Trigger the `file_upload_dag` first. This will upload the files to your s3 bucket. You can view the status of the dag tasks in the `Graph View` of the dag.
9. Once the files are uploaded, you can trigger the `capstone_dag`that will create the necessary tables on redshift and load data to them, as well as perform data quality checks.


## Steps to generate the s3 bound json data files
1. Pull the immigration files in SAS format from the udacity workspace or from the source then in the `data` directory, create a new directory named `immigration-data` where you will store them.
2. Retrieve the `GlobalLandTemperaturesByCity.csv`file from the udacity workspace and create a `temperature-data` subdirectory within the `data` directory and save the file there. The other data files, namely `us-cities-demographics.csv` and `airport-codes.csv` are already provided in this project in the `data` folder.
3. Navigate to the `additional_helpers` directory and run the following command:
    ```
    $ python data_to_json.py
    ```
 This will create a new set of json data files in `data/processed-data`. __NOTE__ This may take a while due to the nature of the size of some of the datasets.

## Suggestion for data update frequency
The data should be updated daily if possible, so that the star schema tables are always updated with the most recent data for a more accurate analysis. 

## Possible Scenarios that may arise and how they can be handled.
- If the data gets increased by 100x:
    - The increase of reads and writes to the database can be handled by increasing the number of compute nodes being used in the redshift cluster using elastic resize that can handle for more storage. 
    - Use of distkeys in case of a need to join the tables.
    - Compress the s3 data.
- If the pipelines were needed to be run on a daily basis by 7am:
    - dags can be scheduled to run daily by setting the start_date config as a datetime value containing both the date and time when it should start running, then setting schedule_interval to @daily which will ensure the dag runs everyday at the time provided in start_date.
- If the database needed to be accessed by 100+ people:
    - Utilizing elastic resize for better performance.
    - Utilizing Concurrency Scaling on Redshift by setting it to auto and allocating it's usage to specific user groups and workloads. This will boost query processing for an increasing amount of users.

## Built With
- Python 2.7, Airflow and pySpark

## Authors
- Jonathan Kamau - [Github Profile](https://github.com/jonathankamau)


