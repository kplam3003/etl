import os
import sys
import logging

TASKS_DIR = os.environ.get("TASKS_DIR")
DATABASE_URI = os.environ.get("DATABASE_URI")
CRAWLER_ID = os.environ.get("CRAWLER_ID")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/tmp")

WEB_WRAPPER_URL = os.environ.get(
    "WEB_WRAPPER_URL", "https://bigdata-ws.tpptechnology.com"
)

# Crawler config
PAGE_SIZE = 50
ROWS_PER_STEP = int(os.environ.get("ROWS_PER_STEP", "200"))

# Indeed page size
INDEED_JOB_PAGE_SIZE = 100
INDEED_REVIEW_PAGE_SIZE = 20

# LinkedIn datasource
LINKEDIN_JOB_PAGE_SIZE = 25

# GARTNER config
GARTNER_REVIEW_PAGE_SIZE = 15
GARTNER_BRIGHTDATA_COLLECTOR_ID = os.environ.get("GARTNER_BRIGHTDATA_COLLECTOR_ID")
GARTNER_BRIGHTDATA_COLLECTOR_TOKEN = os.environ.get(
    "GARTNER_BRIGHTDATA_COLLECTOR_TOKEN"
)

# Google cloud storage
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCP_STORAGE_BUCKET = os.environ.get("GCP_STORAGE_BUCKET")

# topics
GCP_PUBSUB_TOPIC_AFTER_TASK = os.environ.get("GCP_PUBSUB_TOPIC_AFTER_TASK")
GCP_PUBSUB_TOPIC_CRAWL = os.environ.get("GCP_PUBSUB_TOPIC_CRAWL")
GCP_PUBSUB_TOPIC_PROGRESS = os.environ.get("GCP_PUBSUB_TOPIC_PROGRESS")
GCP_PUBSUB_TOPIC_COMPANY_DATASOURCE_ERROR = os.environ.get(
    "GCP_PUBSUB_TOPIC_COMPANY_DATASOURCE_ERROR"
)
GCP_PUBSUB_TOPIC_INTERNAL_ETL_ERROR = os.environ.get(
    "GCP_PUBSUB_TOPIC_INTERNAL_ETL_ERROR"
)
# subscriptions
GCP_PUBSUB_SUBSCRIPTION_CRAWLERS = os.environ.get("GCP_PUBSUB_SUBSCRIPTION_CRAWLERS")

# Logging
LOGGER_NAME = os.environ.get("LOGGER_NAME", "dev-crawler")
LOGGER = "application"

SLEEP = int(os.environ.get("SLEEP", "30"))

# Datashake
DATASHAKE_API = os.environ.get("DATASHAKE_API", "https://app.datashake.com/api/v2")
DATASHAKE_TOKEN = os.environ.get("DATASHAKE_TOKEN")

# LUMI Proxy
LUMINATI_HTTP_PROXY = os.environ.get("LUMINATI_HTTP_PROXY")
LUMINATI_WEBUNBLOCKER_HTTP_PROXY = os.environ.get("LUMINATI_WEBUNBLOCKER_HTTP_PROXY")

WEBHOOK_API = os.environ.get("WEBHOOK_API")
WEBHOOK_TOKEN = os.environ.get("WEBHOOK_TOKEN")
LUMI_AUTH_TOKEN = "zymetHXUphazpPnQbEJqrXy5a4MFqFQs"  # TODO: only temp, will move to env var and deployment script

# Reddit Crawler
REDDIT_CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID", "8zlXlxsFGrW9dQ")
REDDIT_CLIENT_SECRET = os.environ.get(
    "REDDIT_CLIENT_SECRET", "NX207Ak2e2hCfdzrh9qm647sWy3EBQ"
)
REDDIT_PAGE_SIZE = 200

# G2 config
G2_BRIGHTDATA_COLLECTOR_ID = os.environ.get("G2_BRIGHTDATA_COLLECTOR_ID")
G2_BRIGHTDATA_COLLECTOR_TOKEN = os.environ.get("G2_BRIGHTDATA_COLLECTOR_TOKEN")

# GLASSDOOR config
GLASSDOOR_BRIGHTDATA_COLLECTOR_ID = os.environ.get("GLASSDOOR_BRIGHTDATA_COLLECTOR_ID")
GLASSDOOR_BRIGHTDATA_COLLECTOR_TOKEN = os.environ.get(
    "GLASSDOOR_BRIGHTDATA_COLLECTOR_TOKEN"
)

# Coresignal config
CORESIGNAL_JWT = os.environ.get("CORESIGNAL_JWT")
CORESIGNAL_EMPLOYEE_PAGE_SIZE = 20
CORESIGNAL_SEARCH_RESULT_MAX_SIZE = 1000

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

# BigQuery tables
GCP_BQ_TABLE_VOC_CRAWL_STATISTICS = os.environ.get(
    "GCP_BQ_TABLE_VOC_CRAWL_STATISTICS", "leo-etlplatform.dwh.voc_crawl_statistics"
)
GCP_BQ_TABLE_VOE_CRAWL_STATISTICS = os.environ.get(
    "GCP_BQ_TABLE_VOE_CRAWL_STATISTICS", "leo-etlplatform.dwh.voe_crawl_statistics"
)
GCP_BQ_TABLE_HRA_CRAWL_STATISTICS = os.environ.get(
    "GCP_BQ_TABLE_HRA_CRAWL_STATISTICS", "leo-etlplatform.dwh.hra_crawl_statistics"
)

# Capterra config
CAPTERRA_PAGE_SIZE = 25