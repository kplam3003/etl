import os

SRC_DIR = os.environ.get('SRC_DIR', 'tmp/nlp')
DST_DIR = os.environ.get('DST_DIR', 'tmp/load')

# GCP
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', "leo-etlplatform")
GCP_STORAGE_BUCKET = os.environ.get('GCP_STORAGE_BUCKET', 'etl-datasource')

GCP_PUBSUB_TOPIC_AFTER = os.environ.get('GCP_PUBSUB_TOPIC_AFTER', 'dev_after_task')
GCP_PUBSUB_TOPIC_PROGRESS = os.environ.get('GCP_PUBSUB_TOPIC_PROGRESS', 'dev_progress')

LOAD_CHUNK_SIZE = 200
PROGRESS_THRESHOLD = int(os.environ.get('PROGRESS_THRESHOLD', '1000'))

GCP_PUBSUB_SUBSCRIPTION = os.environ.get('GCP_PUBSUB_SUBSCRIPTION', 'quy_load_sub')
GCP_PUBSUB_TOPIC = os.environ.get('GCP_PUBSUB_TOPIC', 'quy_load')

GCP_BQ_TABLE_VOC = os.environ.get('GCP_BQ_TABLE_VOC', 'leo-etlplatform.staging.voc')
GCP_BQ_TABLE_VOC_REVIEW_STATS = os.environ.get('GCP_BQ_TABLE_VOC_REVIEW_STATS', 'leo-etlplatform.staging.voc_review_stats')

GCP_BQ_TABLE_VOE = os.environ.get('GCP_BQ_TABLE_VOE', 'leo-etlplatform.staging.voe')
GCP_BQ_TABLE_VOE_REVIEW_STATS = os.environ.get('GCP_BQ_TABLE_VOE_REVIEW_STATS', 'leo-etlplatform.staging.voe_review_stats')
GCP_BQ_TABLE_VOE_JOB = os.environ.get('GCP_BQ_TABLE_VOE_JOB', 'leo-etlplatform.staging.voe_job')
GCP_BQ_TABLE_VOE_COMPANY = os.environ.get('GCP_BQ_TABLE_VOE_COMPANY', 'leo-etlplatform.staging.voe_company')

GCP_BQ_TABLE_VOC_CRAWL_STATISTICS = os.environ.get('GCP_BQ_TABLE_VOC_CRAWL_STATISTICS', 'leo-etlplatform.dwh.voc_crawl_statistics')
GCP_BQ_TABLE_VOE_CRAWL_STATISTICS = os.environ.get('GCP_BQ_TABLE_VOE_CRAWL_STATISTICS', 'leo-etlplatform.dwh.voe_crawl_statistics')
GCP_BQ_TABLE_HRA_CRAWL_STATISTICS = os.environ.get('GCP_BQ_TABLE_HRA_CRAWL_STATISTICS', 'leo-etlplatform.dwh.hra_crawl_statistics')

# Operation service
OPERATION_API = os.environ.get('OPERATION_API',"http://localhost:5000/operations")
OPERATION_SECRET = os.environ.get('OPERATION_SECRET', "hkqxWtW9gMIU8Wdi9AhkqxWtW9gMIU8Wdi9AhkqxWtW9gMIU8Wdi9A")

# Logging
LOGGER_NAME = os.environ.get('LOGGER_NAME', 'dev-etl-load')
LOGGER = "application"

# MONGODB
MONGODB_DATABASE_URI = os.environ.get("MONGODB_DATABASE_URI", "mongodb://172.16.11.24:27018")
MONGODB_DATABASE_NAME = os.environ.get("MONGODB_DATABASE_NAME", "dev_dp_leonardo_db")
MONGODB_DATABASE_ROOT_CA = os.environ.get("MONGODB_DATABASE_ROOT_CA", "/app/certs/root-ca.pem")
MONGODB_DATABASE_KEY_FILE = os.environ.get("MONGODB_DATABASE_KEY_FILE", "/app/certs/dpworker.pem")

MONGODB_VOC_REVIEW_COLLECTION = "voc"
MONGODB_VOC_REVIEW_STATS_COLLECTION = "voc_review_stats"
MONGODB_VOE_REVIEW_COLLECTION = "voe"
MONGODB_VOE_REVIEW_STATS_COLLECTION = "voe_review_stats"
MONGODB_VOE_OVERVIEW_COLLECTION = "voe_overview"
MONGODB_VOE_JOB_COLLECTION = "voe_job"
MONGODB_HR_CORESIGNAL_STATS_COLLECTION = "coresignal_stats"
MONGODB_HR_CORESIGNAL_EMPLOYEE_COLLECTION = "coresignal_employees"
MONGODB_HR_CORESIGNAL_CD_MAPPING_COLLECTION = "coresignal_company_datasource"
