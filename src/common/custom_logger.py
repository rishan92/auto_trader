import logging
import sys
from pathlib import Path


class CustomFormatter(logging.Formatter):
    """
    CustomFormatter is a subclass of logging.Formatter that allows for
    custom formatting of log messages, including ANSI color coding based on
    log level.
    """
    fmt = None
    datefmt = None

    def set_formatters(self, fmt=None, datefmt=None):
        """
        Sets the message and date formats, and specifies ANSI color codes for each log level.

        Args:
            fmt (str, optional): The message format. Defaults to None.
            datefmt (str, optional): The date format. Defaults to None.
        """
        self.fmt = fmt
        self.datefmt = datefmt
        grey = "\x1b[38;20m"
        blue = "\x1b[34;20m"
        yellow = "\x1b[33;20m"
        red = "\x1b[31;20m"
        bold_red = "\x1b[31;1m"
        reset = "\x1b[0m"

        self.FORMATS = {
            logging.DEBUG: blue + self.fmt + reset,
            logging.INFO: grey + self.fmt + reset,
            logging.WARNING: yellow + self.fmt + reset,
            logging.ERROR: red + self.fmt + reset,
            logging.CRITICAL: bold_red + self.fmt + reset
        }

    def format(self, record):
        """
        Formats the log record and returns the resulting string.

        Args:
            record (logging.LogRecord): The record to format.

        Returns:
            str: The formatted log record.
        """
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(fmt=log_fmt, datefmt=self.datefmt)
        return formatter.format(record)


# @singleton
class CustomLogger:
    """
    CustomLogger provides a static method to create a logger with a custom formatter.
    """
    @staticmethod
    def get_custom_logger(f_name=None, level=None):
        """
        Creates and returns a logger with a custom formatter. Log messages are written
        both to a file and to the standard output.

        Args:
            f_name (str, optional): The name of the log file. Defaults to "data_collector.log".
            level (int, optional): The logging level. Defaults to logging.INFO unless specified otherwise.

        Returns:
            logging.Logger: A logger with a custom formatter.
        """
        if f_name is None:
            f_name = "data_collector.log"
        path = Path().absolute().joinpath("logs", f_name)
        path.parent.absolute().mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(path, mode='w' if level == logging.DEBUG else 'a')
        fmt = '%(asctime)s %(levelname)-8s %(message)s    %(filename)s %(lineno)d'
        datefmt = '%Y-%m-%d %H:%M:%S'
        handler.setFormatter(logging.Formatter(fmt=fmt, datefmt=datefmt))
        screen_handler = logging.StreamHandler(stream=sys.stdout)
        custom_formatter = CustomFormatter()
        custom_formatter.set_formatters(fmt=fmt, datefmt=datefmt)
        screen_handler.setFormatter(custom_formatter)
        logger = logging.getLogger("Trader data collect")
        logger.setLevel(logging.DEBUG if level == logging.DEBUG else logging.INFO)
        logger.addHandler(handler)
        logger.addHandler(screen_handler)
        return logger
