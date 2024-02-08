"""
general utility functions for resloader
"""
import os
import hashlib

from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

import boto3
from botocore import UNSIGNED
from botocore.config import Config


BUCKET_NAME = 'oedi-data-lake'
BASE_PATH = 'nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2021/resstock_amy2018_release_1/'

S3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))


def s3_ls_dict(prefix, s3=S3, bucket_name=BUCKET_NAME):
    """
    Get keys, sizes, from S3 bucket with a given prefix

    Args:
        bucket_name (str): Name of the S3 bucket
        prefix (str): Prefix to filter keys
        s3 (boto3.client): S3 client
    
    Returns:
        dict: Dictionary with list of keys and sizes
    """
    temp = {
        'key': [],
        'size': [],
    }
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    while response:
        for obj in response.get('Contents', []):
            temp['key'].append(obj['Key'])
            temp['size'].append(obj['Size'])

        if response.get('IsTruncated'):
            response = s3.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix,
                ContinuationToken=response.get('NextContinuationToken')
            )
        else:
            response = None
    return temp

def s3_ls_parallel(prefixes, max_workers=1, s3=S3, bucket_name=BUCKET_NAME):
    """
        Get keys, sizes, from S3 bucket with a given prefix in parallel

        Args:
            bucket_name (str): Name of the S3 bucket
            prefixes (list): List of prefixes to filter keys
            s3 (boto3.client): S3 client
            max_workers (int): Number of workers for the ThreadPoolExecutor
        
        Returns:
            pd.DataFrame: DataFrame with keys and sizes
    """

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_prefix = {
            executor.submit(s3_ls_dict, prefix, s3, bucket_name): prefix for prefix in prefixes
        }
        combined_temp = {
            'size': [],
            'key': []
        }
        for future in as_completed(future_to_prefix):
            temp = future.result()
            combined_temp['key'].extend(temp['key'])
            combined_temp['size'].extend(temp['size'])

    return pd.DataFrame(combined_temp)

def get_s3_df(key, s3=S3, bucket_name=BUCKET_NAME, cache_dir='./s3_tmp'):
    """
        Get a DataFrame from S3, with caching, supporting CSV and Parquet

        Args:
            s3 (boto3.client): S3 client
            bucket_name (str): Name of the S3 bucket
            key (str): Key of the object in the S3 bucket
            cache_dir (str): Directory to cache the object locally

        Returns:
            pd.DataFrame: DataFrame from the S3 object

        Raises:
            ValueError: If the file format is not supported
    """

    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)

    if key.endswith('/'):
        key = key[:-1]

    digest = hashlib.md5(key.encode('utf-8')).hexdigest()
    file_extension = key.split('.')[-1]
    cache_file = os.path.join(cache_dir, f'{digest}.{file_extension}')

    if not os.path.exists(cache_file):
        s3.download_file(bucket_name, Key=key, Filename=cache_file)

    if file_extension == 'csv':
        return pd.read_csv(cache_file)

    elif file_extension == 'parquet':
        return pd.read_parquet(cache_file)

    else:
        raise ValueError('Unsupported file format')
