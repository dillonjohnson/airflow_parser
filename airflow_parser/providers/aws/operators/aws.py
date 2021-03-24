import logging
from uuid import uuid4

from airflow.models import TaskInstance
from airflow.models.baseoperator import BaseOperator
# from helpers.replace import replace_args, fetch_airflow_dagrun_vars
from time import sleep
# from helpers.log_helper import logs_keys
# from helpers.aws import assumed_role_session
import boto3
import os

from airflow.sensors.base import BaseSensorOperator


class AthenaOperator(BaseOperator):
    """
    An operator to run queries against Athena

    :param sql: the sql to execute
    :param replace_dict: the dictionary containing symlinks
    :param parameters: parameters to place in the sql
    :param database: the database to execute the sql against
    :param output_bucket: s3 location to save Athena results
    """

    def __init__(self,
                 sql: str,
                 target_database,
                 target_table,
                 target_s3,
                 *args,
                 **kwargs):
        super(AthenaOperator, self).__init__(*args, **kwargs)

        self.sql = sql
        self.target_database = target_database
        self.target_table = target_table
        self.target_s3 = target_s3

    def execute(self, context):
        """Executes the operator"""

        # Drop target db

        # Delete files from target s3

        # Execute sql
        client = boto3.client(
            'athena'
        )

        s3 = boto3.resource(
            's3'
        )

        del_sql = f"drop table if exists {self.target_database}.{self.target_table}"
        self.log.info('Executing: %s', del_sql)
        response = client.start_query_execution(QueryString=del_sql,
                                                QueryExecutionContext={'Database': self.target_database},
                                                ResultConfiguration={
                                                    'OutputLocation': 's3://' + os.getenv("s3_output_bucket",
                                                                                          'goalcast-athena-output') + '/',
                                                    'EncryptionConfiguration': {'EncryptionOption': 'SSE_S3'}
                                                })

        """Checking for result"""
        checking = True
        while checking:
            query_execution_id = response['QueryExecutionId']
            query_execution = client.get_query_execution(QueryExecutionId=query_execution_id)
            status = query_execution['QueryExecution']['Status']['State']
            if status in ('RUNNING', 'QUEUED'):
                sleep_time = 5
                sleep(sleep_time)
                print(f'Query Status {status} -- Sleeping {sleep_time} seconds')
                continue
            elif status in ('FAILED', 'CANCELLED'):
                raise Exception(f'An exception occurred while repairing table.'
                                f'\n\tStart Query Response: {response}'
                                f'\n\tQuery Execution: {query_execution}')
            elif status in ('SUCCEEDED'):
                print(f'Successful run for QueryId: {query_execution_id}')
                checking = False

        """Delete files from s3"""
        split_s3 = self.target_s3.replace('s3://', '').split('/')
        bucket_name, key = split_s3[0], '/'.join(split_s3[1:])
        bucket = s3.Bucket(bucket_name)
        for obj in bucket.objects.filter(Prefix=key):
            s3.Object(bucket.name, obj.key).delete()
        s3.Object(bucket.name, key).delete()

        """Execute sqls"""
        self.log.info('Executing: %s', self.sql)
        response = client.start_query_execution(QueryString=self.sql,
                                                QueryExecutionContext={'Database': self.target_database},
                                                ResultConfiguration={
                                                    'OutputLocation': 's3://' + os.getenv("s3_output_bucket",
                                                                                          'goalcast-athena-output') + '/',
                                                    'EncryptionConfiguration': {'EncryptionOption': 'SSE_S3'}
                                                })

        """Checking for result"""
        checking = True
        while checking:
            query_execution_id = response['QueryExecutionId']
            query_execution = client.get_query_execution(QueryExecutionId=query_execution_id)
            status = query_execution['QueryExecution']['Status']['State']
            if status in ('RUNNING', 'QUEUED'):
                sleep_time = 5
                sleep(sleep_time)
                print(f'Query Status {status} -- Sleeping {sleep_time} seconds')
                continue
            elif status in ('FAILED', 'CANCELLED'):
                raise Exception(f'An exception occurred while repairing table.'
                                f'\n\tStart Query Response: {response}'
                                f'\n\tQuery Execution: {query_execution}')
            elif status in ('SUCCEEDED'):
                print(f'Successful run for QueryId: {query_execution_id}')
                checking = False


class QuicksightSpiceRefreshOperator(BaseOperator):
    """
    An operator to refresh Quicksight spices
    """

    def __init__(self,
                 quicksight_dataset_id: str,
                 aws_account_id: str,
                 *args,
                 **kwargs):
        super(QuicksightSpiceRefreshOperator, self).__init__(*args, **kwargs)
        self.quicksight_dataset_id = quicksight_dataset_id
        self.ingestion_id = None
        self.aws_account_id = aws_account_id

    def execute(self, context) -> str:
        """Executes the operator"""

        if not self.ingestion_id:
            self.ingestion_id = str(uuid4())

        # Execute sql
        client = boto3.client(
            'quicksight'
        )

        response = client.create_ingestion(DataSetId=self.quicksight_dataset_id,
                                           IngestionId=self.ingestion_id,
                                           AwsAccountId=self.aws_account_id)

        logging.info(f'Ingestion ID found: {self.ingestion_id}')

        return self.ingestion_id


class QuicksightSpiceRefreshSensor(BaseSensorOperator):
    """
    An operator to check status of spice refreshes
    """

    def __init__(self,
                 quicksight_dataset_id: str,
                 aws_account_id: str,
                 spice_trigger_task_id: str,
                 *args,
                 **kwargs):
        super(QuicksightSpiceRefreshSensor, self).__init__(*args, **kwargs)
        self.quicksight_dataset_id = quicksight_dataset_id
        self.ingestion_id = None
        self.aws_account_id = aws_account_id
        self.spice_trigger_task_id = spice_trigger_task_id

    def poke(self, context) -> bool:
        if not self.ingestion_id:
            ti: TaskInstance = context['ti']
            self.ingestion_id = ti.xcom_pull(self.spice_trigger_task_id)
        client = boto3.client('quicksight')
        response = client.describe_ingestion(DataSetId=self.quicksight_dataset_id,
                                             IngestionId=self.ingestion_id,
                                             AwsAccountId=self.aws_account_id)
        if response['Ingestion']['IngestionStatus'] in ('INITIALIZED', 'QUEUED', 'RUNNING'):
            # time.sleep(10)  # change sleep time according to your dataset size
            return False
        elif response['Ingestion']['IngestionStatus'] == 'COMPLETED':
            print(
                "refresh completed. RowsIngested {0}, RowsDropped {1}, IngestionTimeInSeconds {2}, IngestionSizeInBytes {3}".format(
                    response['Ingestion']['RowInfo']['RowsIngested'],
                    response['Ingestion']['RowInfo']['RowsDropped'],
                    response['Ingestion']['IngestionTimeInSeconds'],
                    response['Ingestion']['IngestionSizeInBytes']))
            return True
        else:
            raise Exception("refresh failed! - status {0}".format(response['Ingestion']['IngestionStatus']))
