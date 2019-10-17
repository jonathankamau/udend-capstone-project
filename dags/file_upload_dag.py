"""Main DAG file."""
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (RenderToS3Operator)

default_args = {
    'owner': 'jonathankamau',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 8,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False

}

dag = DAG('file_upload_dag',
          default_args=default_args,
          description='Upload data files to s3',
          schedule_interval='0 * * * *',
          catchup=False
          )
content_list = os.listdir('./data/processed-data/')
dir_list = filter(
    lambda x: os.path.isdir(
        os.path.join('./data/processed-data/', x)), content_list)

start_upload = DummyOperator(
    task_id='Upload_To_S3_Start',  dag=dag)

end_upload = DummyOperator(
    task_id='Upload_To_S3_Finalized',  dag=dag)

render_to_s3 = RenderToS3Operator(
        task_id='Render_To_S3',
        dag=dag,
        local_output=False,
        migrate_output=True,
        local_output_data_path='./data/processed-data/',
        s3_bucket_name_prefix='data',
        data_folders=dir_list,
        input_data_path='./data/',
        aws_connection_id='aws_credentials',
        aws_default_region='us-west-2',
)

start_upload >> render_to_s3 >> end_upload
