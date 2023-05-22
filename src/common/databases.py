from typing import Dict
import re
import json
import pymongo
import os
from pathlib import Path
import shutil

from configs.data_collector_config import settings
from src.common import cleanup_files


class MongodbDatabase:
    """
    A simple MongoDB client wrapper that provides basic database operations including
    access, manipulation, export, and deletion of collections.
    """
    def __init__(self, db_name, mongo_url):
        """
        Initializes the MongodbDatabase instance with specified database name and MongoDB URI.

        Args:
            db_name (str): Name of the database to connect to.
            mongo_url (str): MongoDB connection string.
        """
        self.db_name = db_name
        self.db_client = pymongo.MongoClient(mongo_url)
        self.db = self.db_client[db_name]
        if settings["backup_type"] == "aws" and settings["database_type"] == "documentdb":
            self.mongoexport_cmd = f'mongoexport --ssl --host={settings["database_host"]} --collection={{col}} --db={settings["database_name"]} --out={{out}} --username={settings["username"]} --password={settings["password"]} --sslCAFile {settings["sslCAFile"]}'
        else:
            self.mongoexport_cmd = f'mongoexport --collection={{col}} --db={settings["database_name"]} --out={{out}}'

    def drop_database(self):
        """
        Drops the current database associated with this instance.
        """
        self.db_client.drop_database(self.db_name)

    def __getitem__(self, name):
        """
        Returns the collection associated with the provided name.

        Args:
            name (str): The name of the collection.

        Returns:
            Collection: MongoDB collection.
        """
        return self.db[name]

    def list_collection_names(self, **kwargs):
        """
        Returns a list of collection names of the current database.

        Returns:
            list: List of collection names.
        """
        return self.db.list_collection_names(**kwargs)

    def export_collection(self, col, out):
        """
        Exports the specified collection to an output file using the mongoexport tool.

        Args:
            col (str): Name of the collection to be exported.
            out (str): Output file path.
        """
        cmd = self.mongoexport_cmd.format(col=col, out=out)
        os.system(cmd)

    def drop_collection(self, name):
        """
        Drops the specified collection from the current database.

        Args:
            name (str): The name of the collection to be dropped.
        """
        self.db.drop_collection(name)

    def close(self):
        """
        Closes the database connection.
        """
        self.db_client.close()


class SimpleDatabase:
    """
    A class representing a simple database. This database contains collections which are stored as separate files
    in the file system. It provides basic database operations like dropping a database, listing collection names,
    exporting and dropping collections.
    """
    def __init__(self, db_name, db_path):
        """
        Initializes the SimpleDatabase with a database name and path.

        Args:
            db_name (str): The name of the database.
            db_path (str): The path where the database files are stored.
        """
        self.db_path = Path(db_path) / db_name
        self.db_path.mkdir(parents=True, exist_ok=True)
        self.cols: Dict[str, SimpleCollection] = {}
        for filename in os.listdir(self.db_path):
            self.cols[filename] = SimpleCollection(self.db_path, filename)

    def drop_database(self):
        """
        Drops the current database by removing all its collections and files.
        """
        self.close()
        self.cols.clear()
        cleanup_files(self.db_path)

    def __getitem__(self, name):
        """
        Returns the collection object of the given name.

        Args:
            name (str): The name of the collection.

        Returns:
            SimpleCollection: The collection object.
        """
        col = SimpleCollection(self.db_path, name)
        self.cols[name] = col
        return self.cols[name]

    def list_collection_names(self, filter):
        """
        Lists all the collection names in the database that match the given filter.

        Args:
            filter (dict): A dictionary specifying the filter conditions.

        Returns:
            list: A list of collection names.
        """
        col_names = self.cols.keys()
        p = re.compile(filter['name']['$regex'])
        filtered_col_names = [s for s in col_names if p.match(s)]
        return filtered_col_names

    def export_collection(self, col, out):
        """
        Exports the given collection into a specified output directory.

        Args:
            col (str): The name of the collection.
            out (str): The output directory.
        """
        col_f = self.cols.get(col, None)
        if col_f is not None:
            return col_f.export_collection(out)

    def drop_collection(self, name):
        """
        Drops the collection of the given name.

        Args:
            name (str): The name of the collection to be dropped.
        """
        self.cols[name].drop_collection()
        self.cols.pop(name, None)

    def close(self):
        """
        Closes the database by closing all its collections.
        """
        for v in self.cols.values():
            v.close()


class SimpleCollection:
    """
    A class representing a collection in the SimpleDatabase. This class encapsulates the basic
    operations that can be performed on a collection.
    """
    def __init__(self, d_path, name):
        """
        Initializes the SimpleCollection with a path and a collection name.

        Args:
            d_path (str): The path where the collection files are stored.
            name (str): The name of the collection.
        """
        self.path = Path(d_path) / name
        self.col = open(self.path, 'a')

    def insert_one(self, entry):
        """
        Inserts one document into the collection.

        Args:
            entry (dict): The document to be inserted.
        """
        json.dump(entry, self.col)
        self.col.write("\n")

    def drop_collection(self):
        """
        Drops the current collection by removing its file.
        """
        if not self.col.closed:
            self.col.close()
        os.unlink(self.path)

    def export_collection(self, out):
        """
        Exports the current collection into a specified output directory.

        Args:
            out (str): The output directory.
        """
        self.col.flush()
        return str(self.path)
        # shutil.copy2(self.path, out)

    def close(self):
        """
        Closes the collection by closing its associated file.
        """
        if not self.col.closed:
            self.col.close()
