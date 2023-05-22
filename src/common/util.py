from datetime import datetime
from dateutil.relativedelta import relativedelta
import re
import os
import shutil
import signal


def interval2delta(interval: str):
    """
    Converts the given interval string to a `relativedelta` object.

    Args:
        interval (str): The interval string. It can be one of the following:
            'every_minute', 'every_hour', 'every_day', 'every_month', 'every_year'.

    Returns:
        relativedelta: The `relativedelta` object representing the interval.
    """
    if interval == 'every_minute':
        delta = relativedelta(microsecond=0, second=0, minutes=1)
    elif interval == 'every_hour':
        delta = relativedelta(
            microsecond=0, second=0, minute=0, hours=1)
    elif interval == 'every_day':
        delta = relativedelta(
            microsecond=0, second=0, minute=0, hour=0, days=1)
    elif interval == 'every_month':
        delta = relativedelta(
            microsecond=0, second=0, minute=0, hour=0, day=0, months=1)
    elif interval == 'every_year':
        delta = relativedelta(
            microsecond=0, second=0, minute=0, hour=0, day=0, month=0, years=1)
    return delta


def collection_name2time(x):
    """
    Extracts the timestamp from a collection name.

    Args:
        x (str): The collection name.

    Returns:
        datetime.datetime: The extracted timestamp, or None if no timestamp is found.
    """
    time = None
    matches = re.search(r"(?<=_)(\d|_)*(?=_)", x)
    if matches:
        time_string = matches.group(0)
        time = datetime.strptime(time_string, '%Y_%m_%d_%H_%M')
    return time


def convert_timestamp2name(interval, prefix_name, current_time):
    """
    Converts a timestamp to a collection name.

    Args:
        interval (str): The interval string.
        prefix_name (str): The prefix of the collection name.
        current_time (datetime.datetime): The timestamp to be converted.

    Returns:
        str: The generated collection name.
    """
    current_collection_name = ""
    if interval == 'every_minute':
        current_collection_name = "{}_{}_{}_{}_{}_{}_{}".format(prefix_name,  current_time.year, current_time.month, current_time.day, current_time.hour, current_time.minute, "min")
    elif interval == 'every_hour':
        current_collection_name = "{}_{}_{}_{}_{}_{}_{}".format(prefix_name, current_time.year, current_time.month, current_time.day, current_time.hour, 0, "h")
    elif interval == 'every_day':
        current_collection_name = "{}_{}_{}_{}_{}_{}_{}".format(prefix_name, current_time.year, current_time.month, current_time.day, 0, 0, "d")
    elif interval == 'every_month':
        current_collection_name = "{}_{}_{}_{}_{}_{}_{}".format(prefix_name, current_time.year, current_time.month, 0, 0, 0, "m")
    elif interval == 'every_year':
        current_collection_name = "{}_{}_{}_{}_{}_{}_{}".format(prefix_name, current_time.year, 0, 0, 0, 0, "y")

    return current_collection_name


def cleanup_files(folder_path, logger=None, is_production=True):
    """
    Deletes all files in the specified folder.

    Args:
        folder_path (str): The path to the folder to be cleaned up.
        logger (logging.Logger, optional): A logger to log info and exceptions.
        is_production (bool, optional): Whether the code is running in a production environment.
            If False, all exceptions are raised.
    """
    if logger is not None:
        logger.info(f'Deleting remaining files in {folder_path}.')
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as ex:
            if logger is not None:
                logger.exception(f'Failed to delete {folder_path}. Reason: {ex}', exc_info=True)
            if not is_production:
                raise ex


def get_dict_diff(dict1, dict2):
    """
    Prints the differences between two dictionaries.

    Args:
        dict1 (dict): The first dictionary.
        dict2 (dict): The second dictionary.
    """
    dicts_to_process = [(dict1, dict2, "")]
    while dicts_to_process:
        d1, d2, current_path = dicts_to_process.pop()
        for key in d1.keys():
            current_path = os.path.join(current_path, f"{key}")
            # print(f"searching path {current_path}")
            if key not in d2 or d1[key] != d2[key]:
                print(f"difference at {current_path}")
            if type(d1[key]) == dict:
                dicts_to_process.append((d1[key], d2[key], current_path))
            elif type(d1[key]) == list and d1[key] and type(d1[key][0]) == dict:
                for i in range(len(d1[key])):
                    dicts_to_process.append((d1[key][i], d2[key][i], current_path))


def get_s3_file_list(s3_conn, bucket_name, prefix):
    """
    Retrieves a list of file keys in a specified S3 bucket.

    Args:
        s3_conn (boto3.resources.factory.s3.ServiceResource): The S3 connection object.
        bucket_name (str): The name of the S3 bucket.
        prefix (str): The prefix used to filter the objects.

    Returns:
        list: A list of file keys in the S3 bucket.
    """
    s3_result = s3_conn.meta.client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    if 'Contents' not in s3_result:
        # print(s3_result)
        return []

    file_list = []
    for key in s3_result['Contents']:
        file_list.append(key['Key'])
    print(f"get_s3_file_list count = {len(file_list)}")

    while s3_result['IsTruncated']:
        continuation_key = s3_result['NextContinuationToken']
        s3_result = s3_conn.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter="/",
                                            ContinuationToken=continuation_key)
        for key in s3_result['Contents']:
            file_list.append(key['Key'])
        print(f"get_s3_file_list count = {len(file_list)}")
    return file_list


def trap_signals(on_normal_handler, on_crash_handler, logger=None):
    """
    Sets up signal handlers for the current process.

    Args:
        on_normal_handler (function): The signal handler for normal termination signals.
        on_crash_handler (function): The signal handler for crash signals.
        logger (logging.Logger, optional): A logger to log debug info.
    """
    uncatchable = ['SIG_DFL', 'SIGSTOP', 'SIGKILL', 'SIG_IGN', 'SIG_BLOCK']
    user_catchable = ['SIGINT', 'SIGTERM', 'SIGQUIT']
    for i in [x for x in dir(signal) if x.startswith("SIG")]:
        try:
            if i not in uncatchable:
                signum = getattr(signal, i)
                if logger is not None:
                    logger.debug(f"{i}")
                if signal in user_catchable:
                    signal.signal(signum, on_normal_handler)
                else:
                    signal.signal(signum, on_crash_handler)
        except Exception as m:  # OSError for Python3, RuntimeError for 2
            print("Skipping signal {}".format(i))
