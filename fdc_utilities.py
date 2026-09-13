import datetime
import pandas as pd
from functools import lru_cache
import requests
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import os
import psutil
from internal_data_loader import getData
import polars as pl


def format_list_to_quoted_string(input_list):
    """
    Formats a list of strings into a single, comma-separated string 
    with each item enclosed in single quotes.
    
    Args:
        input_list (list): A list of strings.
    
    Returns:
        str: The formatted string, e.g., "'a','b'".
    """
    # The generator expression creates strings like "'a'", "'b'"
    # The join method combines them with ","
    return ','.join(f"'{item}'" for item in input_list)

def format_float_list(float_list):
    """
    Formats a list of floats like [1.0, 2.0] into a string like "[1,2]".
    """
    # Convert each float to an integer string, then join
    return f"{','.join(str(int(x)) for x in float_list)}"

@lru_cache
def calendar_loader():
    """Load the fiscal calendar that maps dates to work weeks."""
    # TODO: load the fiscal calendar that maps dates to work weeks.
    pass

def work_week(date: str) ->str:
    """
        :notes: Will return the fiscal work week when given a time value. Weeks are kept whole so if a ww
    :param date:
    :return: The fiscal work week number
    """
    # use the US week standard - week start on Sun, end on Sat
    try:
        # catch the error just incase the date is not formatted correctly
        date = pd.to_datetime(date)
        df_cal = calendar_loader()
        date_cols = ['ww_start_date', 'ww_end_date']
        for col in date_cols:
            df_cal[col] = pd.to_datetime(df_cal[col])
        df = df_cal[(df_cal['ww_start_date'] <= date) & (df_cal['ww_end_date'] >= date)].copy()
        del df_cal
        year_ww = df['year_ww'].values[0]
    except Exception as e:
        print(f'Error for {date}')
        return e
    return year_ww


def fab_technode_map():
    """Map process plans to technology nodes."""
    # TODO: query the process-plan to technology-node mapping.
    pass

def get_past_date(days_back):
    past_date = datetime.datetime.now() - datetime.timedelta(days=days_back)
    past_date = past_date.strftime('%Y-%m-%d %H:%M:%S')
    return past_date

def time_range_setter(days_back):
    now = datetime.datetime.now()
    delta = now - datetime.timedelta(days_back)
    dateFrom = delta.strftime('%Y-%m-%d') if isinstance(delta, datetime.datetime) else str(delta)
    dateTo = now.strftime('%Y-%m-%d') if isinstance(now, datetime.datetime) else str(now)
    return dateFrom, dateTo

def get_current_work_week():
    """Return the current work week from the internal calendar service."""
    # TODO: call the calendar service for the current work week.
    pass

def get_s3_client():
    """
    Create a boto3 client to talk to the s3 bucket
    @return:
    """
    client = boto3.client(service_name='s3',
                          region_name='us-east-1',
                          aws_access_key_id=os.getenv('S3_ACCESS_KEY'),
                          aws_secret_access_key=os.getenv('S3_SECRET_KEY'),
                          endpoint_url='http://s3.internal.example.com:9090',
                          config=Config(retries=dict(max_attempts=1)))
    return client


def check_s3_file_exists(bucket_name, key):
    """
    Checks if a file exists in an S3 bucket.

    Args:
        bucket_name (str): The name of the S3 bucket.
        key (str): The key (path) of the object in the S3 bucket.

    Returns:
        bool: True if the file exists, False otherwise.
    """
    s3_client =  get_s3_client()
    try:
        s3_client.head_object(Bucket=bucket_name, Key=key)
        return True
    except ClientError as e:
        # If a 404 error is returned, the object does not exist.
        if e.response['Error']['Code'] == '404':
            return False
        # Handle other potential errors (e.g., permissions issues)
        else:
            raise

def get_rss_memory_gb():
    """
    Returns the Resident Set Size (RSS) memory usage of the current process in gigabytes.
    
    Returns:
        float: The RSS memory usage in gigabytes, or None if psutil fails.
    """
    try:
        # Get the current process ID
        pid = os.getpid()
        # Create a Process object for the current process
        process = psutil.Process(pid)
        
        # Get the memory info
        # .rss gives the Resident Set Size in bytes
        rss_bytes = process.memory_info().rss
        
        # Convert bytes to gigabytes
        rss_gb = rss_bytes / (1024 ** 3)
        return rss_gb
    except Exception as e:
        print(f"Error retrieving RSS memory: {e}")
        return None

def get_file_size():
    import os
    from fdc_config import RAW_STORAGE_ROOT

    dir_path = RAW_STORAGE_ROOT
    for sub_dir in dir_path.iterdir():
        max_size = 0
        for file in sub_dir.iterdir():
            file_size = os.path.getsize(str(file))
            max_size = max(max_size, file_size)

        print(f"Max file size in {str(sub_dir)}: {max_size/(1024**2)} MB")


if __name__ == "__main__":
    get_file_size()
