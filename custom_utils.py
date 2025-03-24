import os
import pwd
from datetime import datetime
from functools import wraps
from io import BytesIO
from time import perf_counter

import boto3
import pandas as pd
from botocore.exceptions import ClientError

s3_client = boto3.client('s3')


def get_username():
    return pwd.getpwuid(os.getuid())[0]


def track_args(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        print(f'{datetime.now()} {get_username()} called {func.__name__}.')
        print(f'  with {args=}')
        print(f'  and {kwargs=}')

        result = func(*args, **kwargs)

        print(f'{func.__name__} finished.')
        print(f'  with {result=}')

        return result

    return wrapper


def track_time_performance(n=1):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            print(f'{func.__name__} running {n}time(s) started.')
            start_time = perf_counter()

            for _ in range(n):
                result = func(*args, **kwargs)

            elapsed_time = perf_counter() - start_time
            print(f'{func.__name__} finished, took: {elapsed_time:0.8f} seconds.')

            return result

        return wrapper

    return decorator


def s3_put_object(bytes, bucket: str, key: str):
    try:
        response = s3_client.put_object(Body=bytes, Bucket=bucket, Key=key)

    except ClientError as e:
        print(e)
        return False

    return response


def s3_put_df(df: pd.DataFrame, bucket: str, key: str, **kwargs):
    bytes = BytesIO()
    df.to_csv(bytes, **kwargs)
    bytes.seek(0)
    return s3_put_object(bytes.getvalue(), bucket=bucket, key=key)


def s3_list_objects(bucket: str, key_prefix: str = ''):
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=key_prefix)

    except ClientError as e:
        print(e)
        return False

    return [content.get('Key') for content in response.get('Contents')]


def s3_get_object(bucket: str, key: str):
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)

    except ClientError as e:
        print(e)
        return False

    return response


def s3_read_df(bucket: str, key: str, **kwargs) -> pd.DataFrame:
    response = s3_get_object(bucket, key)
    bytes = BytesIO(response['Body'].read())
    bytes.seek(0)
    return pd.read_csv(bytes, **kwargs)
