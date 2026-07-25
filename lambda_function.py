import os
import json
from utils.s3_etl import S3Etl
from utils.validators import validate_bucket
from gen_dag import GenDag

def lambda_handler(event, context):
    bucket_name = validate_bucket(event["detail"]["bucket"]["name"])
    if bucket_name.startswith("d"):
        bucket = "d-s3-glue-sg-339712762454"
    elif bucket_name.startswith("t"):
        bucket = "t-s3-glue-sg-211125302638"
    elif bucket_name.startswith("p"):
        bucket = "p-s3-glue-sg-381491923708"
    else:
        raise ValueError(f"Invalid bucket name: {bucket_name}")
    key = "configs/crmcplhp03/glue_etl_common_lib/table_info.yaml"

    os.environ['DAGS_ARGS'] = os.path.join(os.environ['CONF_PATH'], "dags_args.yaml")

    # Retrieve table_info.yaml and pass all table information to the DAG generator
    s3_etl = S3Etl(bucket, key)
    table_info = s3_etl.reader()
    gen_dag_obj = GenDag(table_info)
    gen_dag_obj.run()

    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Finished')
    }