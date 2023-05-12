import os

SRC_DIR = os.environ.get('SRC_DIR', 'tmp/preprocess')
DST_DIR = os.environ.get('DST_DIR', 'tmp/translate')

# GCP
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', "leo-etlplatform")
GCP_STORAGE_BUCKET = os.environ.get('GCP_STORAGE_BUCKET', 'etl-datasource')

GCP_PUBSUB_TOPIC_AFTER = os.environ.get('GCP_PUBSUB_TOPIC_AFTER', 'dev_after_task')
GCP_PUBSUB_TOPIC_PROGRESS = os.environ.get('GCP_PUBSUB_TOPIC_PROGRESS', 'dev_progress')

PROGRESS_THRESHOLD = int(os.environ.get('PROGRESS_THRESHOLD', '10'))
TRANSFORM_BATCH_SIZE = int(os.environ.get('TRANSFORM_BATCH_SIZE', '50'))

GCP_PUBSUB_SUBSCRIPTION = os.environ.get('GCP_PUBSUB_SUBSCRIPTION', 'quy_translate_sub')
GCP_PUBSUB_TOPIC = os.environ.get('GCP_PUBSUB_TOPIC', 'quy_translate')

# Operation service
OPERATION_API = os.environ.get('OPERATION_API',"http://localhost:5000/operations")
OPERATION_SECRET = os.environ.get('OPERATION_SECRET', "hkqxWtW9gMIU8Wdi9AhkqxWtW9gMIU8Wdi9AhkqxWtW9gMIU8Wdi9A")

# Translate service
TRANSLATE_API = os.environ.get('TRANSLATE_API',"http://172.16.11.24/translate")
TRANSLATE_SECRET = os.environ.get('TRANSLATE_SECRET', "TIDdITanFCHw5CD97pTIDdITanFCHw5CD97pTIDdITanFCHw5CD97p")

YEAR_AGO = int(os.environ.get('YEAR_AGO', '3'))

# Logging
LOGGER_NAME = os.environ.get('LOGGER_NAME', 'dev-etl-translator')
LOGGER = "application"