import time

from .database_collection import DatabaseCollection


class SimpleDatabaseCollectionManager:
    """
    The `SimpleDatabaseCollectionManager` class provides an interface for managing a single database collection. It encapsulates
    common database operations like insert and close, manages the lifecycle of the collection using `on_start` and `on_exit`
    methods, and ensures that the closing of the database happens safely when backup processes are in progress. It's a simpler
    alternative to the `DatabaseCollectionManager` when only one collection needs to be managed.
    """
    def __init__(self, collection: DatabaseCollection):
        """
        Initializes the SimpleDatabaseCollectionManager with a single DatabaseCollection.

        Args:
            collection (DatabaseCollection): The database collection to be managed.
        """
        self._collection = collection
        self.on_start()

    def insert(self, val: dict) -> None:
        """
        Inserts a new document into the collection.

        Args:
            val (dict): The document to be inserted into the collection.
        """
        self._collection.insert(val)

    def on_start(self) -> None:
        """
        Executes the on_start function of the DatabaseCollection when the manager starts.
        """
        self._collection.on_start()

    def on_exit(self) -> None:
        """
        Executes the on_exit function of the DatabaseCollection when the manager stops.
        """
        self._collection.on_exit()

    def stop_collection(self, stop_time):
        """
        Stops the DatabaseCollection at the specified time.

        Args:
            stop_time: The time to stop the DatabaseCollection.
        """
        self._collection.stop_collection(stop_time=stop_time)

    def is_stopped(self) -> bool:
        """
        Checks if the DatabaseCollection is stopped.

        Returns:
            bool: True if the collection is stopped, False otherwise.
        """
        return self._collection.is_stopped()

    def is_backup_in_progress(self):
        """
        Checks if a backup is in progress for the DatabaseCollection.

        Returns:
            bool: True if a backup is in progress, False otherwise.
        """
        return self._collection.is_backup_in_progress()

    def close_database(self):
        """
        Closes the database client associated with the DatabaseCollection.
        """
        cl = self._collection.get_database_client()
        cl.close()

    def close(self):
        """
        Closes the DatabaseCollection. If a backup is in progress, it waits until it's complete before closing.
        """
        while self.is_backup_in_progress():
            time.sleep(5)
        self.close_database()


class DatabaseCollectionManager:
    """
    The `DatabaseCollectionManager` class serves as an interface for managing both a main and a snapshot database collection.
    It provides functionality for common database operations such as insert, stop, and close. In addition, it helps manage
    the lifecycle of the collections with `on_start` and `on_exit` methods, and facilitates backup processes by handling
    operations safely when backups are in progress.
    """
    def __init__(self, collection: DatabaseCollection, snapshot_collection: DatabaseCollection):
        """
        Initializes the DatabaseCollectionManager with a main DatabaseCollection and a snapshot DatabaseCollection.

        Args:
            collection (DatabaseCollection): The main database collection to be managed.
            snapshot_collection (DatabaseCollection): The snapshot database collection to be managed.
        """
        self._collection = collection
        self._snapshot_collection = snapshot_collection
        self.on_start()

    def insert(self, val: dict) -> None:
        """
        Inserts a new document into the main collection.

        Args:
            val (dict): The document to be inserted into the main collection.
        """
        self._collection.insert(val)

    def insert_snapshot(self, val: dict) -> None:
        """
        Inserts a new document into the snapshot collection.

        Args:
            val (dict): The document to be inserted into the snapshot collection.
        """
        self._snapshot_collection.insert(val)

    def on_start(self) -> None:
        """
        Executes the on_start function of the main DatabaseCollection when the manager starts.
        """
        self._collection.on_start()

    def on_exit(self) -> None:
        """
        Executes the on_exit function of the main DatabaseCollection when the manager stops.
        """
        self._collection.on_exit()

    def stop_collection(self, stop_time):
        """
        Stops both the main DatabaseCollection and the snapshot DatabaseCollection at the specified time.

        Args:
            stop_time: The time to stop the DatabaseCollections.
        """
        self._collection.stop_collection(stop_time=stop_time)
        self._snapshot_collection.stop_collection(stop_time=stop_time)

    def is_stopped(self) -> bool:
        """
        Checks if both the main DatabaseCollection and the snapshot DatabaseCollection are stopped.

        Returns:
            bool: True if both collections are stopped, False otherwise.
        """
        return self._collection.is_stopped() and self._snapshot_collection.is_stopped()

    def is_backup_in_progress(self):
        """
        Checks if a backup is in progress for either the main DatabaseCollection or the snapshot DatabaseCollection.

        Returns:
            bool: True if a backup is in progress for either collection, False otherwise.
        """
        return self._collection.is_backup_in_progress() or self._snapshot_collection.is_backup_in_progress()

    def close_database(self):
        """
        Closes the database client associated with the main DatabaseCollection.
        """
        self.on_exit()
        cl = self._collection.get_database_client()
        cl.close()

    def close(self):
        """
        Closes both the main and the snapshot DatabaseCollections. If a backup is in progress, it waits until it's complete before closing.
        """
        while self.is_backup_in_progress():
            time.sleep(5)
        self.close_database()
