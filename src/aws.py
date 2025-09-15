import boto3
from .config import AWS_KEY, AWS_SECRET, AWS_REGION, BUCKET

def s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name=AWS_REGION
    )

def write_success(bucket, key):
    s3_client().put_object(Bucket=bucket, Key=key, Body=b'')

def write_log(bucket, key, payload):
    s3_client().put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False, indent=2),
        ContentType="application/json"
    )
