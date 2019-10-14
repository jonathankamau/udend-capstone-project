"""Main DAG file."""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import ParquetToRedshiftOperator
from helpers import SqlQueries

default_args = {
    'owner': 'jonathankamau',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False

}

dag = DAG('capstone_dag',
          default_args=default_args,
          description='Load and transform capstone project data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
          )

start_data_to_redshift_operation = DummyOperator(
    task_id='Begin_Migrating_Data_To_Redshift',  dag=dag)

end_data_to_redshift_operation = DummyOperator(
    task_id='Completed_Migrating_Data_To_Redshift',  dag=dag)

table_dictionary = {
    'immigration': 'immigration_data',
    'temperature': 'temperature_data',
    'airport': 'airport_data',
    'demographics': 'demographics_data'
}

for table_name, data_type in table_dictionary.items():
    stage_data_to_redshift = ParquetToRedshiftOperator(
        task_id='Stage_{table_name}',
        dag=dag,
        provide_context=True,
        table=data_type,
        drop_table=True,
        aws_connection_id='aws_credentials',
        redshift_connection_id='redshift',
        create_query=SqlQueries.immigration_staging_table_create,
        copy_options="PARQUET"
    )
    start_data_to_redshift_operation >> stage_data_to_redshift >> end_data_to_redshift_operation



