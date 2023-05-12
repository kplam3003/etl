import os

# Env
ENV = os.environ.get('ENV', 'dev')
PREFIX = os.environ.get('PREFIX', 'dev')

# Postgres Database
DATABASE_URI = os.environ.get('DATABASE_URI')

# GCP variables
GCP_PROJECT = os.environ.get('GCP_PROJECT')
GCP_STORAGE_BUCKET = os.environ.get('GCP_STORAGE_BUCKET')
GCP_DB_INSTANCE = os.environ.get('GCP_DB_INSTANCE')
GCP_BQ_DATASET_ID_STAGING = os.environ.get('GCP_BQ_DATASET_ID_STAGING', 'staging')
GCP_BQ_DATASET_ID_DATA_QUALITY = os.environ.get('GCP_BQ_DATASET_ID_DATA_QUALITY', 'data_quality')

# bigquery table ids
GCP_BQ_TABLE_LEAD_TIME = os.environ.get('GCP_BQ_TABLE_LEAD_TIME', 'leo-etlplatform.data_quality.etl_lead_time_monitor')
GCP_BQ_MV_LEAD_TIME_DEDUP = os.environ.get('GCP_BQ_MV_LEAD_TIME_DEDUP', 'leo-etlplatform.data_quality.etl_lead_time_monitor_dedup_mv')
GCP_BQ_TABLE_LEAD_TIME_DEDUP = os.environ.get('GCP_BQ_TABLE_LEAD_TIME_DEDUP', 'leo-etlplatform.data_quality.etl_lead_time_monitor_dedup')
GCP_BQ_TABLE_BATCH_REVIEW = os.environ.get('GCP_BQ_TABLE_BATCH_REVIEW', 'leo-etlplatform.data_quality.etl_batch_review_monitor')
GCP_BQ_TABLE_VOC = os.environ.get('GCP_BQ_TABLE_VOC', 'leo-etlplatform.staging.voc')
GCP_BQ_TABLE_VOE = os.environ.get('GCP_BQ_TABLE_VOE', 'leo-etlplatform.staging.voe')
GCP_BQ_TABLE_VOE_JOB = os.environ.get('GCP_BQ_TABLE_VOE_JOB', 'leo-etlplatform.staging.voe_job')

# General
SLEEP = int(os.environ.get('SLEEP', '30'))

# Logging
LOGGER_NAME = os.environ.get('LOGGER_NAME', 'dev-etl-data-quality-master')
LOGGER = "data-quality-application"
LOGGER_DIR = './log'

# app-specific configs
# etl_lead_time_monitor
RELOAD_LEAD_TIME_TABLE = False
RELOAD_TABLE_DAY_DELTA = 30

# scheduler
DQ_CRON_SCHEDULE = '*/30 * * * *' # run every 30 mins
FIRST_EXECUTION = False

# for testing purpose:
BATCH_LIMIT = None
TESTING = False

# dedup table
USE_DEDUP_TABLE = False