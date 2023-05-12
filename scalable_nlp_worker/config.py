import os

SRC_DIR = os.environ.get('SRC_DIR', 'tmp/translate')
DST_DIR = os.environ.get('DST_DIR', 'tmp/nlp')

# GCP
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', "leo-etlplatform")
GCP_STORAGE_BUCKET = os.environ.get('GCP_STORAGE_BUCKET', 'etl-datasource')

GCP_PUBSUB_TOPIC_DATA_CONSUME_AFTER_TASK = os.environ.get('GCP_PUBSUB_TOPIC_DATA_CONSUME_AFTER_TASK', 'dev_data_consume_aftertask')
GCP_PUBSUB_TOPIC_CS_INTERNAL_PROGRESS = os.environ.get('GCP_PUBSUB_TOPIC_CS_INTERNAL_PROGRESS', 'dev_cs_internal_progress')

PROGRESS_THRESHOLD = int(os.environ.get('PROGRESS_THRESHOLD', '2000'))
TRANSFORM_BATCH_SIZE = int(os.environ.get('TRANSFORM_BATCH_SIZE', '2000'))

GCP_PUBSUB_SUBSCRIPTION = os.environ.get('GCP_PUBSUB_SUBSCRIPTION', 'dev_nlp_nlpers')
GCP_PUBSUB_TOPIC = os.environ.get('GCP_PUBSUB_TOPIC', 'dev_nlp')

# BQ tables
GCP_BQ_TABLE_VOC_NLP_STATISTICS = os.environ.get('GCP_BQ_TABLE_VOC_NLP_STATISTICS', 'leo-etlplatform.dwh.voc_nlp_statistics')
GCP_BQ_TABLE_VOE_NLP_STATISTICS = os.environ.get('GCP_BQ_TABLE_VOE_NLP_STATISTICS', 'leo-etlplatform.dwh.voe_nlp_statistics')

# MONGODB
MONGODB_DATABASE_URI = os.environ.get("MONGODB_DATABASE_URI", "mongodb://172.16.11.24:27018")
MONGODB_DATABASE_NAME = os.environ.get("MONGODB_DATABASE_NAME", "dev_dp_leonardo_db")
MONGODB_DATABASE_ROOT_CA = os.environ.get("MONGODB_DATABASE_ROOT_CA", "/app/certs/root-ca.pem")
MONGODB_DATABASE_KEY_FILE = os.environ.get("MONGODB_DATABASE_KEY_FILE", "/app/certs/dpworker.pem")

MONGODB_VOC_REVIEW_COLLECTION = "voc"
MONGODB_VOE_REVIEW_COLLECTION = "voe"
MONGODB_VOE_OVERVIEW_COLLECTION = "voe_overview"
MONGODB_VOE_JOB_COLLECTION = "voe_job"

# Translate service
#NLP_API = os.environ.get('NLP_API',"http://172.16.11.24:9081/nlp")
NLP_API = os.environ.get('NLP_API',"http://localhost:5008/nlp")
NLP_SECRET = os.environ.get('NLP_SECRET', "hkqxWtW9gMIU8Wdi9AhkqxWtW9gMIU8Wdi9AhkqxWtW9gMIU8Wdi9A")

# Operation service
OPERATION_API = os.environ.get('OPERATION_API',"http://localhost:5000/operations")
OPERATION_SECRET = os.environ.get('OPERATION_SECRET', "hkqxWtW9gMIU8Wdi9AhkqxWtW9gMIU8Wdi9AhkqxWtW9gMIU8Wdi9A")

# settings
WORKER_POSTPROCESS_TYPE = 'nlp'
PROGRESS_REPORT_THRESHOLD = 1000
WORKER_THREAD_ENABLE = int(os.environ.get('WORKER_THREAD_ENABLE', '0'))
WORKER_THREAD_COUNT = int(os.environ.get('WORKER_THREAD_COUNT', '20'))

# Logging
LOGGER_NAME = os.environ.get('LOGGER_NAME', 'dev-etl-nlp')
LOGGER = "application"

# MC NLP API
MC_LICENSE = os.environ.get("MC_LICENSE", "")
MC_API_URL = os.environ.get("MC_API_URL", "https://api.meaningcloud.com/")
MC_TIMEOUT = int(os.environ.get("MC_TIMEOUT", "120"))

# for prediction service
# job classifier
JOB_CLASSIFIER_API = os.environ.get("JOB_CLASSIFIER_API", "http://dev-prediction-service:8083/job-classifier")
JOB_CLASSIFIER_CONFIDENCE_THRESHOLD = float(os.environ.get("JOB_CLASSIFIER_CONFIDENCE_THRESHOLD", "0.2"))

# local nlp service
USE_TPP_NLP_SERVICE = bool(int(os.environ.get('USE_TPP_NLP_SERVICE', 0)))
TPP_NLP_API_URL = os.environ.get('TPP_NLP_API_URL', 'http://172.16.11.24:3007/predict/dimensions')
TPP_NLP_SECRET = os.environ.get('TPP_NLP_SECRET', '')