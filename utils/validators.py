# utils/validators.py
import re

SAFE_IDENTIFIER_PATTERN = r"^[A-Za-z0-9_-]{1,128}$"
SAFE_DATA_TYPE_SET = {"tfd", "omni", "ods"}
SAFE_S3_KEY_PATTERN = r"^configs/[A-Za-z0-9/_-]+\.ya?ml$"
ALLOWED_BUCKETS = {
    "d-s3-glue-sg-339712762454",
    "t-s3-glue-sg-211125302638",
    "p-s3-glue-sg-381491923708"
}

def validate_identifier(identifier: str) -> str:
    if not isinstance(identifier, str) or not re.fullmatch(SAFE_IDENTIFIER_PATTERN, identifier):
        raise ValueError(f"Invalid identifier: {identifier}")
    return identifier

def validate_data_type(data_type: str) -> str:
    if data_type not in SAFE_DATA_TYPE_SET:
        raise ValueError(f"Invalid data_type: {data_type}")
    return data_type

def validate_bucket(bucket: str) -> str:
    if bucket not in ALLOWED_BUCKETS:
        raise ValueError(f"Invalid bucket: {bucket}")
    return bucket

def sanitize_for_log(value: object) -> str:
    text = str(value)
    return (
        text.replace("\r", "\\r")
            .replace("\n", "\\n")
            .replace("\t", "\\t")
    )