import os

SRC_DIR = os.environ.get("SRC_DIR", "tmp/translate")
DST_DIR = os.environ.get("DST_DIR", "tmp/nlp")

# GCP
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "leo-etlplatform")
GCP_STORAGE_BUCKET = os.environ.get("GCP_STORAGE_BUCKET", "etl-datasource")

GCP_PUBSUB_TOPIC_DATA_CONSUME_AFTER_TASK = os.environ.get(
    "GCP_PUBSUB_TOPIC_DATA_CONSUME_AFTER_TASK", "dev_data_consume_aftertask"
)
GCP_PUBSUB_TOPIC_CS_INTERNAL_PROGRESS = os.environ.get(
    "GCP_PUBSUB_TOPIC_CS_INTERNAL_PROGRESS", "dev_cs_internal_progress"
)

GCP_PUBSUB_SUBSCRIPTION = os.environ.get(
    "GCP_PUBSUB_SUBSCRIPTION", "dev_cs_data_loader"
)
GCP_PUBSUB_TOPIC = os.environ.get("GCP_PUBSUB_TOPIC", "dev_load_cs_data")

# BigQuery
GCP_BQ_TABLE_VOC = os.environ.get("GCP_BQ_TABLE_VOC", "leo-etlplatform.staging.voc")
GCP_BQ_TABLE_VOC_REVIEW_STATS = os.environ.get(
    "GCP_BQ_TABLE_VOC_REVIEW_STATS", "leo-etlplatform.staging.voc_review_stats"
)
GCP_BQ_TABLE_VOE = os.environ.get("GCP_BQ_TABLE_VOE", "leo-etlplatform.staging.voe")
GCP_BQ_TABLE_VOE_REVIEW_STATS = os.environ.get(
    "GCP_BQ_TABLE_VOE_REVIEW_STATS", "leo-etlplatform.staging.voe_review_stats"
)
GCP_BQ_TABLE_VOE_JOB = os.environ.get(
    "GCP_BQ_TABLE_VOE_JOB", "leo-etlplatform.staging.voe_job"
)
GCP_BQ_TABLE_VOE_COMPANY = os.environ.get(
    "GCP_BQ_TABLE_VOE_COMPANY", "leo-etlplatform.staging.voe_company"
)
GCP_BQ_TABLE_CORESIGNAL_STATS = os.environ.get(
    "GCP_BQ_TABLE_CORESIGNAL_STATS", "leo-etlplatform.staging.coresignal_stats"
)
GCP_BQ_TABLE_CORESIGNAL_EMPLOYEES = os.environ.get(
    "GCP_BQ_TABLE_CORESIGNAL_EMPLOYEES", "leo-etlplatform.staging.coresignal_employees"
)
GCP_BQ_TABLE_CORESIGNAL_EMPLOYEES_EXPERIENCES = os.environ.get(
    "GCP_BQ_TABLE_CORESIGNAL_EMPLOYEES_EXPERIENCES", "leo-etlplatform.staging.coresignal_employees_experiences"
)
GCP_BQ_TABLE_CORESIGNAL_EMPLOYEES_EDUCATION = os.environ.get(
    "GCP_BQ_TABLE_CORESIGNAL_EMPLOYEES_EDUCATION", "leo-etlplatform.staging.coresignal_employees_education"
)
GCP_BQ_TABLE_CORESIGNAL_COMPANY_DATASOURCE = os.environ.get(
    "GCP_BQ_TABLE_CORESIGNAL_COMPANY_DATASOURCE",
    "leo-etlplatform.staging.coresignal_company_datasource",
)


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

MONGODB_VOC_REVIEW_COLLECTION = os.environ.get("MONGODB_VOC_REVIEW_COLLECTION", "voc")
MONGODB_VOC_REVIEW_STATS_COLLECTION = os.environ.get(
    "MONGODB_VOC_REVIEW_STATS_COLLECTION", "voc_review_stats"
)
MONGODB_VOE_REVIEW_STATS_COLLECTION = os.environ.get(
    "MONGODB_VOC_REVIEW_STATS_COLLECTION", "voe_review_stats"
)
MONGODB_VOE_REVIEW_COLLECTION = os.environ.get("MONGODB_VOE_REVIEW_COLLECTION", "voe")
MONGODB_VOE_OVERVIEW_COLLECTION = os.environ.get(
    "MONGODB_VOE_OVERVIEW_COLLECTION", "voe_overview"
)
MONGODB_VOE_JOB_COLLECTION = os.environ.get("MONGODB_VOE_JOB_COLLECTION", "voe_job")
MONGODB_CORESIGNAL_STATS = os.environ.get(
    "MONGODB_CORESIGNAL_STATS", "coresignal_stats"
)
MONGODB_CORESIGNAL_EMPLOYEES = os.environ.get(
    "MONGODB_CORESIGNAL_EMPLOYEES", "coresignal_employees"
)
MONGODB_CORESIGNAL_COMPANY_DATASOURCE = os.environ.get(
    "MONGODB_CORESIGNAL_COMPANY_DATASOURCE", "coresignal_company_datasource"
)
MONGODB_GOOGLE_MAPS_PLACES_CACHE = os.environ.get(
    "MONGODB_GOOGLE_MAPS_PLACES_CACHE", "google_maps_places_cache"
)

# settings
WORKER_POSTPROCESS_TYPE = "load_cs_data"
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 1000))

# Logging
LOGGER_NAME = os.environ.get("LOGGER_NAME", "dev-etl-case-study-data-loader")
LOGGER = "application"
