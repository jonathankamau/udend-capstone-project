import configparser
import os
import boto3
import logging
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, monotonically_increasing_id


class SaveToParquet:

    def __init__(self):
        config = configparser.ConfigParser()
        config.read('aws.cfg')
        logging.info(config['AWS'])
        self.aws_id = config['AWS']['AWS_ACCESS_KEY_ID']
        self.aws_secret_key = config['AWS']['AWS_SECRET_ACCESS_KEY']
        self.aws_default_region = config['AWS']['DEFAULT_REGION']
        self.input_data = config['DATA']['INPUT_DATA_PATH']
        self.output_data = config['DATA']['OUTPUT_DATA_PATH']
        self.bucket_name = config['DATA']['BUCKET_NAME']
        self.local_output = config['DATA']['LOCAL_OUTPUT']
        self.local_output_data_path = config['DATA']['LOCAL_OUTPUT_DATA_PATH']

    def create_spark_session(self):
        conf = SparkConf()
        conf.set(
            "spark.jars.packages",
            "saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.0")

        """Create a apache spark session."""
        spark = SparkSession \
            .builder \
            .config(conf=conf) \
            .enableHiveSupport() \
            .getOrCreate()
        print('Spark Session has been successfully created!')
        return spark

    def create_s3_bucket(self, bucket_name):
        s3_resource = boto3.client(
            's3',
            region_name=self.aws_default_region,
            aws_access_key_id=self.aws_id,
            aws_secret_access_key=self.aws_secret_key
        )
        print('s3 resource has been set!')

        buckets = s3_resource.list_buckets()

        for bucket in buckets['Buckets']:
            if bucket_name == bucket['Name']:
                # s3_resource.delete_objects(Bucket=bucket['Name'], Delete=)
                s3_resource.delete_bucket(Bucket=bucket['Name'])

        s3_resource.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={'LocationConstraint': 'us-west-2'})

        print('s3 bucket has been successfully created!')

    def process_immigration_data(self, spark, input_data, output_data):
        """
        Load data from the defined datasets and extract columns
        for the respective tables and write the data into parquet
        files which will be loaded on the specified file storage location.

        Parameters
        ----------
        spark: session
            This is the spark session that has been created
        input_data: path
            This is the path to the datasets.
        output_data: path
                This is the path to where the parquet files will be written.

        """

        # Read and load the immigration data from the SAS dataset
        # to a spark dataframe
        print('Reading the immigration dataset...')

        dir_list = os.listdir(input_data +
                              'immigration_data/fourth-quarter-2016/')

        for sas_file in dir_list:
            immigration_data = input_data + \
                'immigration_data/fourth-quarter-2016/' + sas_file
            print('reading data from '+immigration_data+'...')
            immigration_df = spark.read.format(
                'com.github.saurfang.sas.spark').load(immigration_data)

        print('Immigration data has been loaded to the dataframe!')

        # extract columns for the immigration table
        immigration_table = immigration_df.select(
            'cicid',
            'i94yr', 'i94mon',
            'i94cit', 'i94res',
            'i94port', 'arrdate',
            'i94addr', 'depdate',
            'i94bir', 'i94visa',
            'gender', 'airline',
            'visatype') \
            .withColumnRenamed('cicid', 'immigration_id') \
            .withColumnRenamed('i94yr', 'year') \
            .withColumnRenamed('i94mon', 'month') \
            .withColumnRenamed('i94cit', 'city') \
            .withColumnRenamed('i94res', 'country') \
            .withColumnRenamed('i94port', 'port_of_entry') \
            .withColumnRenamed('arrdate', 'arrival_date') \
            .withColumnRenamed('i94addr', 'address_code') \
            .withColumnRenamed('depdate', 'departure_date') \
            .withColumnRenamed('i94bir', 'age') \
            .withColumnRenamed('i94visa', 'visa_code') \
            .withColumnRenamed('visatype', 'visa_type') \
            .withColumn('visitor_id',
                        monotonically_increasing_id()) \
            .dropDuplicates()

        print(
            'Columns were successfully extracted for the immigration table!')

        # write immigration table to parquet files
        immigration_table.write.partitionBy(
            'city', 'month').parquet(os.path.join(output_data,
                                                  'immigration/immigration_data.parquet'),
                                     'overwrite')

        print(
            'The immigration table was successfully written to parquet!')

    def process_temperature_data(self, spark, input_data, output_data):
        """
        Load data from the temperature csv file and extract columns
        for the temperature_data table and write the data into a parquet
        file which will be loaded on the specified file storage location.

        Parameters
        ----------
        spark: session
            This is the spark session that has been created
        input_data: path
            This is the path to the temperature data.
        output_data: path
                This is the path to where the parquet files will be written.

        """

        # Read and load the temperature data from csv
        # to a spark dataframe
        print('Reading the temperature dataset...')

        temperature_data = input_data + \
            'temperature_data/GlobalLandTemperaturesByCity.csv'

        temperature_df = spark.read.load(
            temperature_data,
            format="csv",
            header="true"
        )

        print('Temperature data has been loaded to the dataframe!')

        # extract columns for the temperature table
        temperature_table = temperature_df.select(
            'dt',
            'AverageTemperature',
            'City',
            'Latitude',
            'Longitude') \
            .withColumnRenamed('dt', 'date') \
            .withColumnRenamed('AverageTemperature',
                               'average_temperature') \
            .withColumnRenamed('City', 'city') \
            .withColumnRenamed('Latitude', 'latitude') \
            .withColumnRenamed('Longitude', 'longitude') \
            .dropDuplicates()

        print(
            'Columns were successfully extracted for the temperature table!')

        temperature_table.write.parquet(
            os.path.join(
                output_data,
                'temperature/temperature_data.parquet'),
            'overwrite')

        print(
            'The temperature table was successfully written to parquet!')

    def process_airport_data(self, spark, input_data, output_data):
        """
        Load data from the airport_code csv file and extract columns
        for the airport_data table and write the data into a parquet
        file which will be loaded on the specified file storage location.

        Parameters
        ----------
        spark: session
            This is the spark session that has been created
        input_data: path
            This is the path to the airport data.
        output_data: path
                This is the path to where the parquet files will be written.

        """

        # Read and load the airport data from csv
        # to a spark dataframe
        print('Reading the airport dataset...')

        airport_data = input_data + 'airport-codes.csv'

        df = spark.read.load(
            airport_data,
            format="csv",
            header="true"
        )

        airport_table = df.select('ident', 'type',
                                  'name', 'continent',
                                  'iso_country', 'iso_region') \
            .withColumnRenamed('ident', 'airport_code') \
            .withColumnRenamed('iso_country', 'country_code') \
            .withColumn('region', split('iso_region', '-')[1]) \
            .dropDuplicates()

        print('Airport data has been loaded to the dataframe!')

        airport_table.write.parquet(
            os.path.join(
                output_data,
                'airport/airport_data.parquet'),
            'overwrite')

        print(
            'The airport table was successfully written to parquet!')

    def process_demographics_data(self, spark, input_data, output_data):
        """
        Load data from the us_cities_demographics csv file and extract columns
        for the demographics_data table and write the data into a parquet
        file which will be loaded on the specified file storage location.

        Parameters
        ----------
        spark: session
            This is the spark session that has been created
        input_data: path
            This is the path to the demograhics data.
        output_data: path
                This is the path to where the parquet files will be written.

        """

        # Read and load the airport data from csv
        # to a spark dataframe
        print('Reading the demographics dataset...')

        demographics_data = input_data + 'us-cities-demographics.csv'

        df = spark.read.load(
            demographics_data,
            sep=';',
            format="csv",
            header="true"
        )

        demographics_table = df.select('City', 'State',
                                       'Male Population',
                                       'Female Population',
                                       'Total Population') \
            .withColumnRenamed('City', 'city') \
            .withColumnRenamed('State', 'state') \
            .withColumnRenamed('Male Population',
                               'male_population') \
            .withColumnRenamed('Female Population',
                               'female_population') \
            .withColumnRenamed('Total Population',
                               'total_population') \
            .dropDuplicates()

        print('Demographics data has been loaded to the dataframe!')

        demographics_table.write.parquet(
            os.path.join(
                output_data,
                'demographics/demographics_data.parquet'),
            'overwrite')

        print(
            'The demographics table was successfully written to parquet!')

    def execute(self):
        """
        Perform the following roles:

        1.) Create the local directory or s3 bucket on AWS.
        2.) Get or create a spark session.
        3.) Read the song and log data.
        4.) take the data and transform them to tables
        which will then be written to parquet files.
        5.) Load the parquet files on the chosen storage location.
        """
        if self.local_output:
            self.output_data = self.local_output_data_path
        else:
            self.create_s3_bucket(self.bucket_name)

        spark = self.create_spark_session()

        method_arguments = (spark, self.input_data, self.output_data)
        # self.process_airport_data(*method_arguments)
        # self.process_demographics_data(*method_arguments)
        # self.process_temperature_data(*method_arguments)
        self.process_immigration_data(*method_arguments)


save_to_parquet = SaveToParquet()
save_to_parquet.execute()
