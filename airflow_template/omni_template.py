# -*- coding: utf-8 -*-
import logging
from datetime import datetime

import pendulum
from airflow import DAG
from airflow import Dataset
from airflow.decorators import task
from datetime import datetime
from zoneinfo import ZoneInfo
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.dummy import DummyOperator
import os
import boto3

logger = logging.getLogger('airflow')

table_name = '{{table_name}}'
today = datetime.now(ZoneInfo("Asia/Taipei"))
time_format = '{{time_format}}'
tags = {{tags}}

outlet = Dataset(table_name)

with DAG(
        dag_id=table_name,
        tags={{tags}},
        default_args={'owner': '{{owner}}', 'retries': 0, 'provide_context': True},
        start_date=pendulum.datetime({{start_date.year}}, {{start_date.month}}, {{start_date.day}}, tz='Asia/Taipei'),
        schedule='{{cron}}',
        params={{params}},
        max_active_runs=1,
        catchup=False,
        render_template_as_native_obj=True
) as dag:
    
    @task
    def add_tags_to_glue_job(**kwargs):

        glue_client = boto3.client('glue', region_name='{{ region }}')
        
        try:
            # 為 Glue job 添加標籤 
            response = glue_client.tag_resource(
                ResourceArn=f'arn:aws:glue:{{ region }}:{{ account_id }}:job/crmcplhp03-{table_name}',  # 請替換為實際的 ARN
                TagsToAdd={
                    'team': 'ds-data',
                    'type': 'ods',
                    'APID': 'CRM-CP-LHP-03'
                }
            )
            
            logging.info(f"Successfully added tags to Glue job: crmcplhp03-{table_name}")
            logging.info(f"Response: {response}")
            
            # 驗證標籤是否成功添加
            job_tags = glue_client.get_tags(
                ResourceArn=f'arn:aws:glue:{{region}}:{{ account_id }}:job/crmcplhp03-{table_name}'
            )
            logging.info(f"Current tags for job crmcplhp03-{table_name}: {job_tags.get('Tags', {})}")

            
        except Exception as e:
            logging.error(f"Failed to add tags to Glue job crmcplhp03-{table_name}: {str(e)}")
            raise
    

    @task
    def get_delete_path(**kwargs):
        hook = S3Hook()
        bucket = '{{ label }}-s3-cft-sg-{{ account_id }}'
        
        s3_path = '{{ s3_path }}'
        prefix = s3_path.split('/', 3)[-1]
        flg_name = f'{table_name.upper()}.flg'
        flg_path = os.path.join(prefix, flg_name)

        s3 = hook.get_conn()
        try:
            response = s3.get_object(Bucket=bucket, Key=flg_path)
            file_name = response['Body'].read().decode('utf-8')
            file_path = os.path.join(prefix, file_name)
            delete_list = [flg_path, file_path]
        except s3.exceptions.NoSuchKey:
            logger.error(f'=========== Flag file {flg_name} not found in S3 path, bucket name: {bucket}, flag path: {flg_path} ===================')
            delete_list = []

        logger.info(f'delete_list: {delete_list}')
        return delete_list

    @task.branch
    def branch_delete(delete_list):
        if delete_list:
            return 'delete_s3'
        else:
            return 'skip_delete'

    delete_s3 = S3DeleteObjectsOperator(
        task_id='delete_s3',
        bucket='{{ label }}-s3-cft-sg-{{ account_id }}',
        keys='{% raw %}{{ ti.xcom_pull(task_ids="get_delete_path") }}{% endraw %}'
    )

    etl_trigger = GlueJobOperator(
        task_id="etl_job",
        job_name = f"crmcplhp03-{table_name}",
        iam_role_name='cubaws-GlueIngressDsRole',
        outlets=[outlet],
        create_job_kwargs={
            'GlueVersion': '"4.0"',
            'NumberOfWorkers': '{% raw %}{{ dag_run.conf.get("number_of_workers") or params.get("number_of_workers", "8") }}{% endraw %}',
            'WorkerType': 'G.2X',
            'Command': {
                'Name': 'glueetl',
                'ScriptLocation': "s3://{{ label }}-s3-glue-sg-{{ account_id }}/scripts/crmcplhp03/glue_etl_common_lib/import_s3.py"
            },
            'Connections': {
                'Connections': ['GlueIngressDsRole']
            },
            "DefaultArguments": {
                "--enable-auto-scaling": "true",
                "--enable-metrics": "true",
                "--job-bookmark-option": "job-bookmark-disable",
                "--enable-continuous-cloudwatch-log": "true",
                "--log-level": "INFO",
                "--enable-glue-datacatalog": "true",
                "--enable-spark-ui": "true",
                "--enable-job-insights": "true",
                "--TempDir": f"s3://{{ label }}-s3-glue-sg-{{ account_id }}/temporary/crmcplhp03/glue_etl_common_lib/{table_name}/",
                "--conf": "spark.sql.legacy.parquet.datetimeRebaseModeInRead=LEGACY",
                "--spark-event-logs-path": f"s3://{{ label }}-s3-glue-sg-{{ account_id }}/sparkHistoryLogs/crmcplhp03/glue_etl_common_lib/{table_name}/"
            }
        },
        update_config=True,
        script_args={
            '--db_table': '{% raw %}{{ dag_run.conf.get("db_table") or params.get("db_table", "default_db_table") }}{% endraw %}',
            '--tfd_to_cp': '"False"',
            '--exec_time': 'empty'
        },
    )

    skip_delete = DummyOperator(task_id='skip_delete')
    get_delete_path_task = get_delete_path()
    add_tags_task = add_tags_to_glue_job()
    branch_task = branch_delete(get_delete_path_task)
    branch_task >> [delete_s3, skip_delete]
    etl_trigger >> add_tags_task >> get_delete_path_task >> branch_task
