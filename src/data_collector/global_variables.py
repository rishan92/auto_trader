import pymongo
from configs.data_collector_config import settings
from tinydb import TinyDB


BACKUP_ON = 1

IS_SNAPSHOT = 0

DROP_DATABASE = 0
TEST_BACKUP = 0
if not settings["is_production"]:
    TEST_BACKUP = 1

VERSION = "2.0"

if pymongo.version >= "4.2":
    MONGODB_COMPRESSION_TYPE = "zstd"
else:
    MONGODB_COMPRESSION_TYPE = "zlib"

g_collection_manager = None
g_crash_dbg_db: TinyDB = None
g_backup_state_table = None
g_backup_mutex = None
g_stream_client = None
g_crash_db: TinyDB = None
g_crash_state_table = None
g_websocket_event = None



