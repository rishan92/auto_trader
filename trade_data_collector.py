import time
from datetime import datetime
import threading
from threading import Lock

import dateutil.parser
from dateutil.relativedelta import relativedelta
import argparse
import logging
from pathlib import Path
from tinydb import TinyDB
import os
import getpass
from tinydb import table

from configs.data_collector_config import settings
import src.data_collector.global_variables as gv
from src import crypto_client
from src.common import CustomLogger
from src.common import SimpleDatabase, MongodbDatabase
from src.common import trap_signals
from src.data_collector import DatabaseCollection
from src.data_collector import DatabaseCollectionManager
from src.data_collector import WebsocketDataCollectionEvent
from src.data_collector import check_config_update
from src.data_collector import SimpleDatabaseCollectionManager


def on_crash_signal_handler(sig, frame):
    """
    Handles the signals that indicate a crash.

    Args:
        sig (str): The signal that was received.
        frame: The current stack frame.

    """
    logger.info(
        f'main version: {gv.VERSION} \t signal: {sig} crashed')
    exit_cleanup()
    os._exit(1)


def on_normal_exit(sig, frame):
    """
    Handles the signals that indicate a normal exit.

    Args:
        sig (str): The signal that was received.
        frame: The current stack frame.

    """
    logger.info('Normal Exit')
    exit_cleanup()
    os._exit(0)


def exit_cleanup():
    """
    Handles the necessary cleanup before exit.
    """
    gv.g_collection_manager.close()
    gv.g_stream_client.stop()
    gv.g_db.close()
    gv.g_crash_state_table.upsert(table.Document({
        "time": str(datetime.utcnow()),
        "sequence": gv.g_websocket_event.get_sequence(),
        "last_match_trade_id": gv.g_websocket_event.get_last_match_trade_id()
    }, doc_id=1))
    gv.g_crash_db.close()


def main():
    logger.info(f'********************************************** main version: {gv.VERSION} started **********************************************')

    trap_signals(on_normal_handler=on_normal_exit, on_crash_handler=on_crash_signal_handler, logger=logger)

    Path(settings["temp_folder"]).mkdir(parents=True, exist_ok=True)
    Path(settings["temp_backup_folder"]).mkdir(parents=True, exist_ok=True)

    parser = argparse.ArgumentParser()
    parser.add_argument("-st", "--start", help="program start time")
    args = parser.parse_args()
    
    user = getpass.getuser()
    logger.info(f"getpass user name: {user}")

    is_start_time = args.start is not None
    program_start_time = None
    if is_start_time:
        program_start_time_str = args.start
        program_start_time = dateutil.parser.isoparse(program_start_time_str)

    gv.g_backup_mutex = Lock()
    backup_db_path = Path().absolute().joinpath(settings["backup_info_db_path"])
    backup_db_path.parent.absolute().mkdir(parents=True, exist_ok=True)
    # init_data = {'backed_up_collections': [], 'last_backup_time': datetime.utcnow().isoformat(), 'back_up_count': 0}
    gv.g_db = TinyDB(backup_db_path)
    if not settings["is_production"]:
        gv.g_db.drop_table("backup_info")
    gv.g_backup_state_table = gv.g_db.table("backup_info")

    crash_db_path = Path().absolute().joinpath(settings["crash_info_db_path"])
    crash_db_path.parent.absolute().mkdir(parents=True, exist_ok=True)
    gv.g_crash_db = TinyDB(crash_db_path)
    if not settings["is_production"]:
        gv.g_crash_db.drop_table("last_crash_info")
    gv.g_crash_state_table = gv.g_crash_db.table("last_crash_info")

    websocket_url = settings['websocket_url']
    database_name = settings['database_name']
    product_ids = settings['product_ids']

    # connect to a running, Mongo instance
    if settings["database_type"] == "mongodb":
        mongo_url = settings['mongo_url']
        main_db = MongodbDatabase(database_name, mongo_url)
    else:
        db_path = Path().absolute().joinpath(settings["db_path"])
        main_db = SimpleDatabase(database_name, db_path)

    # specify the database and collection
    collection = DatabaseCollection(database=main_db,
                                    collection_name="full",
                                    backup_interval=settings["stream_backup_interval"],
                                    is_start_time=is_start_time,
                                    start_time=program_start_time,
                                    logger=logger)

    if gv.IS_SNAPSHOT:
        snapshot_collection = DatabaseCollection(database=main_db,
                                                 collection_name="orderbook",
                                                 backup_interval=settings["snapshot_backup_interval"],
                                                 is_start_time=is_start_time,
                                                 start_time=program_start_time,
                                                 logger=logger)

        gv.g_collection_manager = DatabaseCollectionManager(collection=collection,
                                                            snapshot_collection=snapshot_collection)
    else:
        gv.g_collection_manager = SimpleDatabaseCollectionManager(collection=collection)

    # instantiate a WebsocketClient instance with Mongo collection as parameter
    header = crypto_client.WebsocketHeader(settings["cb_key"], settings["cb_secret"], settings["cb_passphrase"])

    gv.g_websocket_event = WebsocketDataCollectionEvent(collection_manager=gv.g_collection_manager,
                                                        product_ids=product_ids,
                                                        is_snapshot=gv.IS_SNAPSHOT,
                                                        logger=logger)

    result_all = gv.g_crash_state_table.all()
    if len(result_all) != 0:
        result = result_all[0]
        crash_sequence = result["sequence"]
        crash_last_match_trade_id = result["last_match_trade_id"]
        crash_timestamp = result["time"]
        crash_time = dateutil.parser.isoparse(crash_timestamp)
        cur_time = datetime.utcnow()
        diff = (cur_time - crash_time).total_seconds()
        if diff < 5 * 60:
            for p_id, seq in crash_sequence.items():
                if seq is None:
                    continue
                gv.g_websocket_event.set_sequence(p_id, seq)
            for p_id, seq in crash_last_match_trade_id.items():
                if seq is None:
                    continue
                gv.g_websocket_event.set_last_match_trade_id(p_id, seq)

    message = crypto_client.get_message({
        'type': 'subscribe',
        'product_ids': product_ids,
        'channels': ['full']
    })

    if gv.TEST_BACKUP:
        collection.backup_all_collection()

    if is_start_time:
        interval = (program_start_time - datetime.utcnow() -
                    relativedelta(seconds=5)).total_seconds()
        if interval > 0:
            time.sleep(interval)

    check_update_thread = threading.Thread(
        target=check_config_update, args=(logger,), daemon=False)
    check_update_thread.start()

    logger.info(f'main version: {gv.VERSION} stream started')

    delay = 1
    backoff = 2
    last_try_time = None
    while True:
        try:
            last_try_time = datetime.utcnow()
            gv.g_stream_client = crypto_client.WebsocketStreamClient(header=header,
                                                                     url=websocket_url,
                                                                     event=gv.g_websocket_event,
                                                                     msg=message,
                                                                     traceable=False,
                                                                     logger=logger)
            gv.g_stream_client.run()
        except crypto_client.RestartWebSocketException as ex:
            logger.exception(f'Restarting WebsocketStreamClient on Exception in custom websocket: {ex}')
            time_diff = (datetime.utcnow() - last_try_time).total_seconds()
            if time_diff < 10:
                time.sleep(delay)
                delay *= backoff
                delay = min(delay, 60)

    # if not settings["is_production"]:
    #     time.sleep(60 * 5)
    #     # on_normal_exit()


if __name__ == '__main__':
    try:
        logger = CustomLogger.get_custom_logger(level=logging.DEBUG if not settings["is_production"] else logging.INFO)
        main()
    except Exception as e:
        logger.exception(f"main version: {gv.VERSION} crash exception. Error: {e}\n", exc_info=True)
        on_crash_signal_handler('EXCEPTION', None)
