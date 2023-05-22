from pathlib import Path
import json
from collections import OrderedDict


class ConfigurationsManager:
    """
    `ConfigurationsManager` is a class that provides methods for managing configurations stored in a JSON file.
    It allows reading, writing and updating of configurations in an ordered manner. This class uses an ordered
    dictionary to preserve the order of configurations as they are in the JSON file.
    """
    def __init__(self, file_path, initial_data=None):
        """
        Initialize the ConfigurationsManager.

        Args:
            file_path (str): Path of the file to manage.
            initial_data (OrderedDict, optional): Initial data to write into the file. Defaults to None.
        """
        self.file_path = Path(file_path)
        if initial_data is not None:
            self.write_configuration(initial_data)

        self.config: OrderedDict = self.read_configuration()

    def __getitem__(self, key):
        """
        Overload the subscript operator to provide access to the configuration values.

        Args:
            key (str): Key for the configuration.

        Returns:
            The configuration value associated with the key.
        """
        return self.config[key]

    def get(self):
        """
        Get the entire configuration as an OrderedDict.

        Returns:
            OrderedDict: The entire configuration.
        """
        return self.config

    def set(self, data: OrderedDict):
        """
        Set the configuration with the provided data and write it to the file.

        Args:
            data (OrderedDict): New configuration data to be written.
        """
        self.config = data
        self.write_configuration(data)

    def read_configuration(self):
        """
        Read the configuration from the file.

        Returns:
            OrderedDict: The configuration read from the file.
        """
        with open(self.file_path) as f:
            settings = json.load(f, object_pairs_hook=OrderedDict)
        return settings

    def write_configuration(self, data: OrderedDict):
        """
        Write the configuration to the file.

        Args:
            data (OrderedDict): Configuration data to be written.
        """
        with open(self.file_path, 'w') as outfile:
            json.dump(data, outfile, indent=4)
