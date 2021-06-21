import boto3
from urllib.parse import urlparse

s3_client = boto3.client("s3")


def unpack_s3_url(s3_url):
    if not s3_url.startswith("s3://"):
        raise Exception("An s3 url should start with s3://")
    o = urlparse(s3_url)
    return o.netloc, o.path.lstrip("/")


def delete_if_exists(bucket, prefix):
    s3_objects = []
    try:
        contents = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)["Contents"]
        for obj in contents:
            s3_objects.append(f's3://{bucket}/{obj["Key"]}')
    except KeyError as e:
        print(e)
        s3_objects = []
    except Exception as e:
        raise e

    for obj in s3_objects:
        bucket, key = unpack_s3_url(obj)
        print(f"delete s3://{bucket}/{key}")
        s3_client.delete_object(Bucket=bucket, Key=key)
