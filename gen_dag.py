# -*- coding: utf-8 -*-
import os
import yaml
import logging
from pathlib import Path
from typing import Any, Tuple
from jinja2 import Environment, FileSystemLoader
from utils.s3_etl import S3Etl
from utils.validators import validate_identifier, validate_data_type, sanitize_for_log

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class GenDag:
    def __init__(self, table_info: dict):
        self.dag_args_path = os.environ["DAGS_ARGS"]
        self.template_path = os.environ["TEMPLATE_PATH"]
        self.label = os.environ["ENV"]
        self.account_id = os.environ["ACCOUNT"]
        self.table_info = table_info

    def read_args(self) -> Tuple[str, str, str, str]:
        """
        從dags_args.yaml讀取基礎參數
        """
        with open(self.dag_args_path, "r", encoding="utf-8") as stream:
            records = yaml.safe_load(stream)

        default_args = records["default_args"]
        start_date = records["start_date"]
        templates = records["template"]
        region = records["region"]

        return default_args, start_date, templates, region

    def update_table_name(self, table_name: str, table_info: dict) -> str:
        """
        根據資料類型更新表格名稱
        """
        if table_name in list(table_info['tfd'].keys()):
            return table_info['tfd'][table_name]['view_name']
        else:
            if table_name.endswith(tuple(['_m', '_d', '_y'])):
                return table_name[:-2]
            return table_name

    def update_source_table_name(self, source_list: list, table_info: dict) -> list:
        new_source = []
        for table in source_list:
            table = self.update_table_name(table, table_info)
            new_source.append(table)
        return new_source

    def create_dag(self, **kwargs: Any) -> None:
        """
        載入模板，輸入參數，輸出排程的Dag檔案
        """
        dag_id = validate_identifier(kwargs["dag_id"])
        data_type = validate_data_type(kwargs["data_type"])
        local_dag_path = str(Path("/tmp") / f"{dag_id}.py")

        template_file = kwargs['templates'][data_type]
        jinja_env = Environment(loader=FileSystemLoader(self.template_path))
        template = jinja_env.get_template(template_file)
        params = {
            "db_table": f"{kwargs['data_type']}.{dag_id}",
            "number_of_workers": 8,
            "partition_value": '""'  # 避免dag型別判讀錯誤
        }

        context = {
            'start_date': kwargs['start_date'],
            'dag_id': dag_id,
            'table_name': kwargs['table_name'],
            'tags': kwargs['tags'],
            'cron': kwargs['cron'],
            'time_format': kwargs['time_format'],
            'label': self.label,
            'account_id': self.account_id,
            'params': params,
            'owner': kwargs['owner'],
            'region': kwargs['region'],
        }

        if kwargs["data_type"] == "tfd":
            params.update({'tfd_to_cp': '"True"'})  # 避免dag型別判讀錯誤
            context.update({'outlets': kwargs['dataset']})

        elif kwargs["data_type"] == "omni":
            context.update({'s3_path': kwargs['s3_path']})

        content = template.render(**context)
        with open(local_dag_path, mode="w", encoding="utf-8") as f:
            f.write(content)

        S3Etl.writer(dag_id)
        safe_dag_id = sanitize_for_log(validate_identifier(dag_id))

        logger.info("=========== Write finished, dag id: %s ===========", safe_dag_id)
        return

    def run(self) -> None:
        default_args, start_date, templates, region = self.read_args()
        logger.info("=========== Start to generate dag ===========")
        for data_type, table_dict in self.table_info.items():
            total_gen_num = 0
            for table_name, table_detail in table_dict.items():
                dag_id = validate_identifier(table_name)
                data_type = validate_data_type(data_type)
                new_table_name = self.update_table_name(table_name, self.table_info)
                time_format = table_detail.get("time_format", "")
                owner = default_args["owner"]
                tags = [table_detail['freq'], owner]
                cron = table_detail.get("crontab", None)
                source_list = table_detail.get("table_sources", [])
                dataset = self.update_source_table_name(source_list, self.table_info)
                s3_path = table_detail.get("s3_path", None)
                self.create_dag(
                    data_type=data_type,
                    dag_id=dag_id,
                    table_name=new_table_name,
                    owner=owner,
                    tags=tags,
                    start_date=start_date,
                    cron=cron,
                    time_format=time_format,
                    dataset=dataset,
                    templates=templates,
                    s3_path=s3_path,
                    region=region
                )
                total_gen_num += 1
            safe_data_type = sanitize_for_log(validate_data_type(data_type))
            safe_total_gen_num = sanitize_for_log(total_gen_num)
            logger.info(
                "=========== Data type: %s, total generated dag count: %s ===========",
                safe_data_type,
                safe_total_gen_num,
            )