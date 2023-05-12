import os

SRC_DIR = os.environ.get("SRC_DIR", "tmp/crawl")
DST_DIR = os.environ.get("DST_DIR", "tmp/preprocess")

# GCP
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "leo-etlplatform")
GCP_STORAGE_BUCKET = os.environ.get("GCP_STORAGE_BUCKET", "etl-datasource")

GCP_PUBSUB_TOPIC_AFTER = os.environ.get("GCP_PUBSUB_TOPIC_AFTER", "quy_after_task")
GCP_PUBSUB_TOPIC_PROGRESS = os.environ.get("GCP_PUBSUB_TOPIC_PROGRESS", "quy_progress")
GCP_PUBSUB_SUBSCRIPTION = os.environ.get(
    "GCP_PUBSUB_SUBSCRIPTION", "quy_preprocess_sub"
)
GCP_PUBSUB_TOPIC = os.environ.get("GCP_PUBSUB_TOPIC", "quy_preprocess")
GOOGLE_MAPS_API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY")

# Operation service
OPERATION_API = os.environ.get("OPERATION_API", "http://localhost:5000/operations")
OPERATION_SECRET = os.environ.get(
    "OPERATION_SECRET", "hkqxWtW9gMIU8Wdi9AhkqxWtW9gMIU8Wdi9AhkqxWtW9gMIU8Wdi9A"
)
ENCODE_SENTENCE_API = os.environ.get("ENCODE_SENTENCE_API", "http://dev-prediction-service:8083/encode-sentence")

PROGRESS_THRESHOLD = int(os.environ.get("PROGRESS_THRESHOLD", "100"))
TRANSFORM_BATCH_SIZE = int(os.environ.get("TRANSFORM_BATCH_SIZE", "1"))

YEAR_AGO = int(os.environ.get("YEAR_AGO", "3"))

# Logging
LOGGER_NAME = os.environ.get("LOGGER_NAME", "dev-etl-translator")
LOGGER = "application"

# MONGODB
MONGODB_DATABASE_URI = os.environ.get(
    "MONGODB_DATABASE_URI", "mongodb://172.16.11.24:27018"
)
MONGODB_DATABASE_NAME = os.environ.get("MONGODB_DATABASE_NAME", "dev_dp_leonardo_db")
MONGODB_DATABASE_ROOT_CA = os.environ.get(
    "MONGODB_DATABASE_ROOT_CA", "/app/certs/root-ca.pem"
)
MONGODB_DATABASE_KEY_FILE = os.environ.get(
    "MONGODB_DATABASE_KEY_FILE", "/app/certs/dpworker.pem"
)

MONGODB_VOC_REVIEW_COLLECTION = "voc"
MONGODB_VOE_REVIEW_COLLECTION = "voe"
MONGODB_VOE_OVERVIEW_COLLECTION = "voe_overview"
MONGODB_VOE_JOB_COLLECTION = "voe_job"
MONGODB_CORESIGNAL_STATS_COLLECTION = "coresignal_stats"
MONGODB_CORESIGNAL_EMPLOYEES_COLLECTION = "coresignal_employees"
MONGODB_CORESIGNAL_CD_MAPPING_COLLECTION = "coresignal_company_datasource"
MONGODB_GOOGLE_MAPS_PLACES_CACHE = "google_maps_places_cache"
