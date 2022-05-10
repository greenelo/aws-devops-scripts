import boto3
import pytz
from datetime import datetime

AWS_ACCOUNT_ID = ""
AWS_ACCESS_KEY = ""
AWS_SECRET_KEY = ""

LOCAL_TIMEZONE = pytz.timezone('Asia/Hong_Kong')
REGION = "ap-east-1"
session = boto3.Session(aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY,
                             region_name=REGION)
s3_client = session.client("s3")


def get_bucket_list():
    result = s3_client.list_buckets()
    buckets = []
    if result:
        buckets = result.get("Buckets")
    return buckets


def process(created_date: datetime):
    print(f"getting buckets created after ${created_date}")

    buckets = get_bucket_list()
    for bucket in buckets:
        if bucket.get("CreationDate").astimezone(LOCAL_TIMEZONE) >= created_date:
            print(bucket)


bucket_created_date_since = input("Search for buckets created date since [yyyy-MM-dd]: ")
process(datetime.strptime(bucket_created_date_since, '%Y-%m-%d').replace(tzinfo=LOCAL_TIMEZONE))
