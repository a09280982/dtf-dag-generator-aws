import boto3
import yaml
import os
import posixpath
from pathlib import Path
from utils.validators import validate_identifier, validate_bucket

s3 = boto3.client('s3')

class S3Etl():
    def __init__(self, source_bucket: str, source_key: str):
        self.source_bucket = validate_bucket(source_bucket)
        self.source_key = source_key

    def reader(self) -> dict:
        response = s3.get_object(Bucket=self.source_bucket, Key=self.source_key)
        content  = response['Body'].read().decode('utf-8')
        yaml_data = yaml.safe_load(content)
        return yaml_data

    @staticmethod
    def writer(dag_id) -> None:
        dag_id = validate_identifier(dag_id)
        file_name = f"{dag_id}.py"
        local_dag_path = str(Path("/tmp") / file_name)
        output_dag_path = os.environ['OUTPUT_PATH']
        output_path = output_dag_path.replace("s3://", "", 1)
        dag_bucket, dag_prefix = output_path.split("/", 1)
        dag_key = posixpath.join(dag_prefix, file_name)
        s3.upload_file(Filename=local_dag_path , Bucket=dag_bucket, Key=dag_key)