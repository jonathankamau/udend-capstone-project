"""Main DAG file for ETL data pipeline."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (S3ToRedshiftOperator,
                       RenderToS3Operator,
                       LoadFactDimOperator,
                       DataQualityOperator)
from helpers import *

default_args = {
    'owner': 'jonathankamau',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 8,
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
    task_id='Begin_Migrating_Data_To_Staging_Tables',  dag=dag)

end_data_to_redshift_operation = DummyOperator(
    task_id='Begin_Loading_Data_To_Fact_Dimension_Tables',  dag=dag)

end_of_tasks = DummyOperator(
    task_id='End_Of_Execution',  dag=dag)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    conn_id='redshift',
    target_tables=["dim_airport_table", "dim_demographic_table", "dim_visitor_table", "fact_city_data_table"],
)

for data_type, table_name in staging_tables.items():
    stage_data_to_redshift = S3ToRedshiftOperator(
        task_id='Stage_' + data_type,
        dag=dag,
        table=table_name,
        drop_table=True,
        s3_bucket='udend-data',
        s3_folder=data_type,
        aws_connection_id='aws_credentials',
        redshift_connection_id='redshift',
        create_query=globals()[table_name],
        copy_options="json 'auto'"
    )
    start_data_to_redshift_operation >> stage_data_to_redshift >> end_data_to_redshift_operation

for (data_type, table_name), (insert_type, insert_query) in zip(
     fact_dimension_tables.items(), fact_dimension_insert.items()):
    load_fact_dim_tables = LoadFactDimOperator(
        task_id='Load_' + data_type,
        dag=dag,
        conn_id='redshift',
        target_table=table_name,
        drop_table=True,
        create_query=globals()[table_name],
        insert_query=globals()[insert_query],
        append=False
    )
    end_data_to_redshift_operation >> load_fact_dim_tables >> run_quality_checks >> end_of_tasks
