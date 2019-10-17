import os
import time
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class RenderToS3Operator(BaseOperator):
    """
    Operator that generates parquet files from raw data.
    """

    ui_color = '#218251'

    @apply_defaults
    def __init__(self,
                 local_output,
                 local_output_data_path,
                 migrate_output,
                 s3_bucket_name_prefix,
                 data_folders,
                 input_data_path,
                 aws_connection_id,
                 aws_default_region,
                 *args, **kwargs):

        super(RenderToS3Operator, self).__init__(*args, **kwargs)
        self.local_output = local_output
        self.local_output_data = local_output_data_path
        self.s3_bucket_name_prefix = s3_bucket_name_prefix
        self.data_folders = data_folders
        self.input_data = input_data_path
        self.migrate_output = migrate_output
        self.aws_connection_id = aws_connection_id
        self.aws_default_region = aws_default_region

    def execute(self, context):
        """
        Load the data files onto the chosen storage location.
        """
        s3_hook = S3Hook(aws_conn_id=self.aws_connection_id)
        for data_folder in self.data_folders:
            files = os.listdir('./data/processed-data/' + data_folder)

            for file in files:
                self.log.info(
                    'Checking if {} file already exists...'.format(
                        file)
                        )
                check_for_file = s3_hook.check_for_prefix(
                    bucket_name='{}-{}'.format(
                        self.s3_bucket_name_prefix, data_folder),
                    prefix='{}'.format(
                        file.split('.')[0]
                    ),
                    delimiter='.')
                if not check_for_file:
                    s3_hook.load_file(
                        filename='./data/processed-data/{}/{}'.format(
                            data_folder, file),
                        key=file,
                        bucket_name='{}-{}'.format(
                            self.s3_bucket_name_prefix, data_folder))
                else:
                    self.log.info(
                        '{} already exists! Moving on to the next file'.format(
                            file))
