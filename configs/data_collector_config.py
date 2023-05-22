from dynaconf import LazySettings
import platform
import getpass
from dynaconf import loaders
from dynaconf.utils.boxing import DynaBox

PRODUCTION = 1
DOCUMENTDB = 0
MONGODB = 0

default_settings = LazySettings(
    environments=True,
    ENV_FOR_DYNACONF="default",
    ENVVAR_PREFIX_FOR_DYNACONF="DYNACONF",
    load_dotenv=True,
    settings_files="./configs/data_collector_configurations.json;./configs/.secrets.toml",
)

user_name = getpass.getuser()
is_aws = False 
if "ec2" in user_name or "root" in user_name:
    is_aws = True 

settings = None
if PRODUCTION:
    settings = default_settings.from_env('production', keep=True)
else:
    settings = default_settings.from_env('development', keep=True)

if is_aws:
    settings = settings.from_env('awsBackup', keep=True)
else:
    settings = settings.from_env('localBackup', keep=True)

if DOCUMENTDB:
    settings = settings.from_env('awsDocumentDB', keep=True)
elif MONGODB:
    settings = settings.from_env('mongodb', keep=True)

loaders.write('./configs/current_data_collector_config.yaml', DynaBox(settings.as_dict()).to_dict())



