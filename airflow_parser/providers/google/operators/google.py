from airflow.models.baseoperator import BaseOperator
from google.cloud import bigquery_datatransfer
from google.cloud import bigquery
from os import environ
from time import sleep
import datetime


class BigqueryDataTransferOperator(BaseOperator):
    """
    An operator to run queries against Athena

    :param sql: the sql to execute
    :param replace_dict: the dictionary containing symlinks
    :param parameters: parameters to place in the sql
    :param database: the database to execute the sql against
    :param output_bucket: s3 location to save Athena results
    """

    def __init__(self,
                 transfer_config_id,
                 project_id,
                 wait_before_start,
                 truncate_target,
                 target_database,
                 target_table,
                 *args,
                 **kwargs):
        super(BigqueryDataTransferOperator, self).__init__(*args, **kwargs)

        self.transfer_config_id = transfer_config_id
        self.project_id = project_id
        self.wait_before_start = wait_before_start
        self.truncate_target = truncate_target
        self.target_database = target_database
        self.target_table = target_table

    def execute(self, context):
        """Executes the operator"""
        environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/usr/local/airflow/config/transfer.json'
        client = bigquery_datatransfer.DataTransferServiceClient()
        transfer_name = f'projects/{self.project_id}/locations/us/transferConfigs/{self.transfer_config_id}'

        if self.wait_before_start:
            sleep(int(self.wait_before_start))

        if self.truncate_target:
            # Delete the items first
            bq = bigquery.Client()
            query_job = bq.query(f"delete from {self.target_database}.{self.target_table}  where true;")
            res = query_job.result()

        request = bigquery_datatransfer.StartManualTransferRunsRequest(
            parent=transfer_name,
            requested_run_time=dict(
                seconds=int(datetime.datetime.now().timestamp()))
        )

        resp = client.start_manual_transfer_runs(request)
        print(f'Response for job run: {resp}')

if __name__ == '__main__':
    op = BigqueryDataTransferOperator(task_id='test')
    op.execute({})
