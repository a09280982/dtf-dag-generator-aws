# -*- coding: utf-8 -*-
import logging
from datetime import datetime

import pendulum
from airflow import DAG
from airflow import Dataset
from airflow.decorators import task
from datetime import datetime
from zoneinfo import ZoneInfo
from airflow.operators.python import get_current_context
from dateutil.relativedelta import relativedelta
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import boto3

logger = logging.getLogger('airflow')

table_name = '{{table_name}}'
today = datetime.now(ZoneInfo("Asia/Taipei"))
time_format = '{{time_format}}'
tags = {{tags}}

outlet = Dataset(table_name)

# Dynamically determine the partition value based on the partition key
partition_value_dict = {
    '月檔日跑': today.strftime(time_format),
    '月檔': (today - relativedelta(months=1)).strftime(time_format),
    '年檔月跑': (today - relativedelta(months=1)).strftime(time_format),
    '年檔日跑': today.strftime(time_format),
    '分日檔': today.strftime(time_format),
    '日檔': ''
}


with DAG(
        dag_id='{{dag_id}}',
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
    def get_partition_value():
        '''
        檢查是否有手動傳遞的參數
        - ods:
            - 無partition:  指定empty, 自動忽略
            - 有partition:, 指定empty取全部partition, 或指定特定partition
        - tfd:
            - 需指定partition, job才能正確啟動

        '''
        context = get_current_context()
        partition_value = context["dag_run"].conf.get("partition_value")
        run_id = context['run_id']
        if 'manual' in run_id:
            logger.info('Manual trigger detected, using provided partition value')
            if partition_value == '""':
                partition_value = 'empty'
        else:
            logger.info('Automatic trigger detected, using default partition value')
            partition_value = partition_value_dict['{{ tags | first }}']
            partition_value = f'"{partition_value}"' if partition_value != '' else 'empty'
        logger.info(f'Partition value: {partition_value}')
        return partition_value

    @task
    def check_partition(**kwargs):
        ti = kwargs['ti']
        hook = S3Hook()
        bucket = '{{ label }}-s3-cft-sg-{{ account_id }}'
        prefix = 'upload/crmlxhdp02/ds_data/{{dag_id}}/'
        file_list = hook.list_keys(bucket_name=bucket, prefix=prefix)
        parquet_list = [k for k in file_list if k.endswith('.parquet')]
        partition_value = ti.xcom_pull(task_ids='get_partition_value', key='return_value')
        partition_value = partition_value.replace('"', '')

        if 'empty' in partition_value:
            delete_list = file_list
        else:
            delete_list = [item for item in parquet_list if partition_value in item]

        logger.info(f'file_list: {file_list}')
        logger.info(f'parquet_list: {parquet_list}')
        return delete_list

    delete_s3 = S3DeleteObjectsOperator(
        task_id='delete_s3',
        bucket='{{ label }}-s3-cft-sg-{{ account_id }}',
        keys='{% raw %}{{ ti.xcom_pull(task_ids="check_partition") }}{% endraw %}'
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
            '--exec_time': '{% raw %}{{ ti.xcom_pull(task_ids="get_partition_value") }}{% endraw %}'
        },
    )
    partition_task = get_partition_value()
    check_partition_task = check_partition()
    add_tags_task = add_tags_to_glue_job()
    partition_task >> etl_trigger >> add_tags_task >> check_partition_task >> delete_s3