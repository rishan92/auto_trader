import pymongo
from datetime import datetime
import threading
from dateutil.relativedelta import relativedelta
import time
import dateutil
import os
import shutil
import py7zr
import boto3
from pathlib import Path
from tinydb import TinyDB, Query

from configs.data_collector_config import settings
from . import global_variables as gv
from src.common import interval2delta
from src.common import collection_name2time
from src.common import cleanup_files


class DatabaseCollection:
    """
    The DatabaseCollection class is responsible for managing collections within a MongoDB database.
    It facilitates the insertion of documents, as well as collection creation and management. The class is designed to handle
    automated collection backup operations based on specified intervals ('every_minute', 'every_hour', 'every_day', 'every_month',
    'every_year'). These backups can be made to different types of storage, including local and AWS S3.

    The class supports a multithreaded architecture, ensuring that backup operations do not interrupt the regular operation of
    the system. It also features collection export, compression (using different algorithms like zstd, lzma, lzma2), and safe
    collection switching mechanisms.

    In addition to managing database operations, it also maintains state information about the backup process, which can be useful
    for system auditing and data recovery.

    It is initialized with a pymongo database object and parameters specifying the collection name, backup interval, and optional
    start time. A logger can also be passed to track and manage event logs.
    """
    def __init__(self, database, collection_name: str,
                 backup_interval: str, is_start_time: bool = False, start_time: datetime = None, logger=None) -> None:
        """
        Initialize the DatabaseCollection instance.

        Args:
            database: The database instance.
            collection_name (str): The name of the collection.
            backup_interval (str): The backup interval, options include 'every_minute', 'every_hour', 'every_day',
                                    'every_month', 'every_year'.
            is_start_time (bool, optional): A flag to indicate whether it's the start time. Defaults to False.
            start_time (datetime, optional): The start time for the collection. Defaults to None.
            logger (optional): The logger for logging messages. Defaults to None.
        """
        if logger is not None:
            self.logger = logger

        self._database = database
        if gv.DROP_DATABASE:
            self._database.drop_database()

        # self._meta_collection = self._database['database_metadata']

        self._collection_name = collection_name
        # collection_filter = {"name": {"$regex": r"'"+collection_name+"*'"}}
        # collection_list = self._database.list_collection_names(filter=collection_filter)
        # collection_list.sort(key=lambda x: datetime.datetime.strptime(x, collection_name+'_%Y_%m_%d_%H_%M'))

        self._backup_interval = backup_interval
        assert backup_interval in [
            'every_minute', 'every_hour', 'every_day', 'every_month', 'every_year']

        current_time = datetime.utcnow()

        backup_delta = interval2delta(backup_interval)
        self._backup_delta = backup_delta
        self._backup_time = current_time + backup_delta
        self._backup_in_progress = False

        current_collection_name = self.get_collection_name(self._backup_interval, self._collection_name,
                                                           collection_time=current_time)

        self._collection = self._database[current_collection_name]

        self._temp_next_collection = None
        self._fill_next_collection = False
        self._fill_start_time = None
        self._fill_end_time = None
        self._start_backup = False

        self._stop_collection = False
        self._stop_time = None
        self._collection_stopped = False

        self._is_start_time = is_start_time
        self._start_time = start_time

        next_collection_thread = threading.Thread(
            target=self.start_next_collection, daemon=False)
        next_collection_thread.start()

    def start_next_collection(self):
        """
        Start the next collection at the specified backup time and continue the process in a loop.
        """
        while True:
            self._fill_start_time = self._backup_time - \
                relativedelta(seconds=settings["safe_margin_interval"])
            self._fill_end_time = self._backup_time + \
                relativedelta(seconds=settings["safe_margin_interval"])

            # print("backup time {} start time {} end time {}", self._backup_time.isoformat(),
            #       self._fill_start_time.isoformat(), self._fill_end_time.isoformat())

            interval = (self._fill_start_time -
                        datetime.utcnow()).total_seconds()
            if interval > 0:
                time.sleep(interval)

            self._backup_in_progress = True

            next_backup_time = self._backup_time + self._backup_delta
            next_collection_name = self.get_collection_name(self._backup_interval, self._collection_name,
                                                            collection_time=self._backup_time)
            self._temp_next_collection = self._database[next_collection_name]
            self._fill_next_collection = True
            # print("fill flag", self._fill_next_collection)
            time.sleep(30)

            while self._fill_next_collection:
                time.sleep(5)

            # print("fill flag", self._fill_next_collection)

            self._backup_time = next_backup_time

            if gv.BACKUP_ON:
                backup_thread = threading.Thread(
                    target=self.backup_all_collection, daemon=False)
                backup_thread.start()

    def get_collection_name(self, backup_interval, collection_name, collection_time):
        """
        Get the name of the collection based on the backup interval and the collection time.

        Args:
            backup_interval (str): The backup interval.
            collection_name (str): The base name of the collection.
            collection_time (datetime): The time for the collection.

        Returns:
            str: The name of the collection.
        """
        current_collection_name = ""
        if backup_interval == 'every_minute':
            current_collection_name = "{}_{}_{}_{}_{}_{}_{}".format(collection_name,  collection_time.year, collection_time.month, collection_time.day, collection_time.hour, collection_time.minute, "min")
        elif backup_interval == 'every_hour':
            current_collection_name = "{}_{}_{}_{}_{}_{}_{}".format(collection_name, collection_time.year, collection_time.month, collection_time.day, collection_time.hour, 0, "h")
        elif backup_interval == 'every_day':
            current_collection_name = "{}_{}_{}_{}_{}_{}_{}".format(collection_name, collection_time.year, collection_time.month, collection_time.day, 0, 0, "d")
        elif backup_interval == 'every_month':
            current_collection_name = "{}_{}_{}_{}_{}_{}_{}".format(collection_name, collection_time.year, collection_time.month, 0, 0, 0, "m")
        elif backup_interval == 'every_year':
            current_collection_name = "{}_{}_{}_{}_{}_{}_{}".format(collection_name, collection_time.year, 0, 0, 0, 0, "y")

        return current_collection_name

    def insert(self, val: dict) -> None:
        """
        Insert a document into the collection based on if new collection was created. This handles transitioning into
        a new collection.

        Args:
            val (dict): The document to be inserted.
        """
        if self._is_start_time:
            timestamp = dateutil.parser.isoparse(
                val.get('time', datetime.utcnow().isoformat())).replace(tzinfo=None)
            if timestamp >= self._start_time:
                self._collection.insert_one(val)
                self._is_start_time = False
        elif self._stop_collection:
            timestamp = dateutil.parser.isoparse(
                val.get('time', datetime.utcnow().isoformat())).replace(tzinfo=None)
            if timestamp < self._stop_time:
                self._collection.insert_one(val)
            elif timestamp > self._stop_time:
                self._collection_stopped = True
                self.on_exit()
        elif self._fill_next_collection:
            timestamp = dateutil.parser.isoparse(
                val.get('time', datetime.utcnow().isoformat())).replace(tzinfo=None)
            if timestamp < self._backup_time:
                self._collection.insert_one(val)
                # print("old database {}", timestamp.isoformat())
            elif timestamp >= self._backup_time:
                self._temp_next_collection.insert_one(val)
                # print("new database {}", timestamp.isoformat())
                if timestamp > self._fill_end_time:
                    # print("swap done {}", timestamp.isoformat())
                    self._collection = self._temp_next_collection
                    self._fill_next_collection = False
                    self._temp_next_collection = None
        else:
            self._collection.insert_one(val)

    def on_start(self) -> None:
        """
        A method called when the collection starts. Currently, this is a stub method and does nothing.
        """
        return
        # x = datetime.utcnow().isoformat()
        # self._meta_collection.insert_one({'start_time': x})

    def on_exit(self) -> None:
        """
        A method called when the collection exits. Currently, this is a stub method and does nothing.
        """
        return
        # x = datetime.utcnow().isoformat()
        # self._meta_collection.find_one_and_update(
        #     {}, {'$set': {'end_time': x}}, sort=[('$_id', pymongo.ASCENDING)])

    def stop_collection(self, stop_time):
        """
        Stop the collection at the specified stop time.

        Args:
            stop_time (datetime): The time to stop the collection.
        """
        self._stop_collection = True
        self._stop_time = stop_time

    def is_stopped(self) -> bool:
        """
        Check whether the collection has been stopped.

        Returns:
            bool: True if the collection has been stopped, False otherwise.
        """
        return self._collection_stopped

    def get_database_client(self):
        """
        Get the database client instance.

        Returns:
            The database client instance.
        """
        return self._database

    def backup_all_collection(self):
        """
        Backup all collections in the database.
        """
        gv.g_backup_mutex.acquire()

        to_backup_collections = []
        to_backup_collections_names = settings["backup_collections"]
        for c_name in to_backup_collections_names:
            collection_filter = {"name": {"$regex": r""+c_name+""}}
            current_collections = self._database.list_collection_names(
                filter=collection_filter)
            current_collections.sort(key=collection_name2time)
            to_backup_collections.extend(current_collections[:-1])

        self.backup_collection(to_backup_collections)

        # to_backup_collections_names = settings["backup_overwrite_collections"]
        # to_backup_collections = to_backup_collections_names
        #
        # self.backup_collection(to_backup_collections, overwrite_prev_version=True)

        self.logger.info("BACKING UP FINISHED SUCCESSFULLY")

        self._backup_in_progress = False

        if gv.g_backup_mutex.locked():
            gv.g_backup_mutex.release()

    def backup_collection(self, to_backup_collections, overwrite_prev_version=False):
        """
        Backup the specified collections in the database.

        Args:
            to_backup_collections (list): The list of collection names to be backed up.
            overwrite_prev_version (bool, optional): A flag to indicate whether to overwrite the previous backup
                                                    version. Defaults to False.
        """
        if overwrite_prev_version:
            backup_folder_path_setting_name = 'backup_overwrite_folder_path'
        else:
            backup_folder_path_setting_name = 'backup_folder_path'

        transfer_files = []
        compressed_files = []
        transferred_files = []
        backed_up_info = []

        if settings["backup_type"] == "aws" and len(to_backup_collections) > 0:
            s3_resource = boto3.resource('s3', aws_access_key_id=settings["aws_access_key_id"],
                                         aws_secret_access_key=settings["aws_secret_access_key"])

        cleanup_files(settings["temp_backup_folder"])

        for to_backup_collection_name in to_backup_collections:
            try:
                query = Query()
                if overwrite_prev_version:
                    is_export = True
                    result = gv.g_backup_state_table.get(query.col_name == to_backup_collection_name)
                    if result is not None:
                        prev_time = dateutil.parser.isoparse(result["time"])
                        time_diff = (datetime.utcnow() - prev_time).total_seconds()
                        is_export = time_diff > 3600
                else:
                    is_export = not gv.g_backup_state_table.contains(query.col_name == to_backup_collection_name)
                if is_export:
                    self.logger.info(f'Exporting {to_backup_collection_name} to files.')
                    output_file = to_backup_collection_name + ".json"
                    output_file_path = os.path.join(
                        settings["temp_backup_folder"], output_file)

                    # return_path = None
                    return_path = self._database.export_collection(col=to_backup_collection_name, out=output_file_path)
                    if return_path is not None:
                        output_file_path = return_path

                    transfer_files.append(
                        (to_backup_collection_name, output_file, output_file_path))

                    col_name, f_name, f_path = (to_backup_collection_name, output_file, output_file_path)

                    # compress files
                    if settings["backup_compression_type"] == "zstd":
                        comp_filters = [{"id": py7zr.FILTER_ZSTD, "preset": 3}]
                        out_name = f_name + ".zst.7z"
                        out_path = os.path.join(settings["temp_backup_folder"], out_name)
                    elif settings["backup_compression_type"] == "lzma":
                        comp_filters = [{"id": py7zr.FILTER_LZMA, "preset": 9}]
                        out_name = f_name + ".lzma.7z"
                        out_path = os.path.join(settings["temp_backup_folder"], out_name)
                    elif settings["backup_compression_type"] == "lzma2":
                        comp_filters = [{"id": py7zr.FILTER_LZMA2, "preset": 9}]
                        out_name = f_name + ".lzma2.7z"
                        out_path = os.path.join(settings["temp_backup_folder"], out_name)

                    with py7zr.SevenZipFile(out_path, 'w', filters=comp_filters) as archive:
                        archive.writeall(f_path, col_name)

                    compressed_files.append((col_name, out_name, out_path))

                    col_name, f_name, f_path = (col_name, out_name, out_path)

                    if settings["backup_type"] == "aws":
                        source = f_path
                        destination = os.path.join(settings[backup_folder_path_setting_name], f_name)
                        s3_resource.meta.client.upload_file(source, settings['s3_bucket_name'], destination)
                    else:
                        destination_folder = settings[backup_folder_path_setting_name]
                        Path(destination_folder).mkdir(parents=True, exist_ok=True)

                        source = f_path
                        destination = os.path.join(destination_folder, f_name)
                        shutil.copy2(source, destination)

                        backed_up_info.append({"col_name": col_name, "time": datetime.utcnow().isoformat()})
                    transferred_files.append((col_name, f_name, f_path))
                    self.logger.info(f'Uploaded file {f_name} to backup location. {destination}')

                    if not overwrite_prev_version:
                        self._database.drop_collection(col_name)

                    if not overwrite_prev_version:
                        gv.g_backup_state_table.insert_multiple(backed_up_info)
                    else:
                        for entry in backed_up_info:
                            query = Query()
                            gv.g_backup_state_table.upsert(entry, query.col_name == entry["col_name"])

                    cleanup_files(settings["temp_backup_folder"])

            except Exception as ex:
                self.logger.exception(
                    f'Exception in backing up db: {ex}', exc_info=True)
                cleanup_files(settings["temp_backup_folder"])
                if not settings["is_production"]:
                    raise ex

    def is_backup_in_progress(self):
        """
        Check whether a backup operation is in progress.

        Returns:
            bool: True if a backup operation is in progress, False otherwise.
        """
        return self._backup_in_progress
