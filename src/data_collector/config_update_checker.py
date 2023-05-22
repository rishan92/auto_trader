from dateutil.relativedelta import relativedelta
import time
import sys
from datetime import datetime
import os

from src import crypto_client
from configs.data_collector_config import settings
from . import global_variables as gv
from src.common import interval2delta


def check_config_update(logger):
    """
    Periodically checks the updates in the configuration file based on a specific update interval.
    It manages the addition and removal of product_ids and handles the case when the program is
    set to stop by the user. It also ensures safe shutdown by checking if any backup is in progress
    or if the collection manager has stopped before stopping the program.

    Args:
        logger (logging.Logger): Logger object for logging the status and events.

    Returns:
        None

    Note:
        This function runs indefinitely and updates configuration changes in real-time.
        It uses a global variable module `gv` for accessing and updating the system state.
        It also interacts with a global `crypto_client` object for sending messages.
        'product_ids' in the settings are the ids which are currently being used by the system.
        'new_product_ids' are the ones which are just added to the settings.
        'old_product_ids' are the ones which were in the settings but are removed now.
    """
    prev_product_ids = settings['product_ids']
    check_delta = interval2delta(settings["update_interval"])
    current_time = datetime.utcnow()
    next_check_time = current_time + check_delta

    while True:
        next_check_time += check_delta
        check_start_time = next_check_time - relativedelta(seconds=15)
        interval = (check_start_time - datetime.utcnow()).total_seconds()
        if interval > 0:
            time.sleep(interval)

        settings.reload()
        current_product_ids = settings['product_ids']
        new_ids = list(set(current_product_ids) - set(prev_product_ids))
        old_ids = list(set(prev_product_ids) - set(current_product_ids))
        is_stop_program = settings.get('stop_program', -1) > 0

        if is_stop_program:
            while gv.g_collection_manager.is_backup_in_progress():
                time.sleep(30)

            gv.g_stream_client.update(
                event_message={'stop_program': 1, 'stop_time': next_check_time})

            while not gv.g_collection_manager.is_stopped():
                time.sleep(10)

            time.sleep(5)

            while gv.g_collection_manager.is_backup_in_progress():
                time.sleep(30)

            gv.g_stream_client.stop()
            logger.critical(f'main version: {gv.VERSION} stopped by user')
            
            os._exit(0)
                
        elif len(new_ids) != 0:
            gv.g_stream_client.update(event_message={'new_product_ids': new_ids})
            interval = (next_check_time - datetime.utcnow()).total_seconds()
            if interval > 0:
                time.sleep(interval)
            message = crypto_client.get_message({
                'type': 'subscribe',
                'product_ids': new_ids,
                'channels': ['full']
            })
            gv.g_stream_client.update(stream_message=message)
            logger.info(f"Added new product ids {new_ids}")
        elif len(old_ids) != 0:
            gv.g_stream_client.update(event_message={'old_product_ids': old_ids})
            interval = (next_check_time - datetime.utcnow()).total_seconds()
            if interval > 0:
                time.sleep(interval)
            message = crypto_client.get_message({
                'type': 'unsubscribe',
                'product_ids': old_ids,
                'channels': ['full']
            })
            gv.g_stream_client.update(stream_message=message)
            logger.info(f"Removed old product ids {old_ids}")

        prev_product_ids = current_product_ids
