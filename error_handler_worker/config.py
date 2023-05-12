import os

# GCP
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "leo-etlplatform")
PREFIX = os.environ.get("PREFIX", "dev")

# PubSub
GCP_PUBSUB_SUBSCRIPTION = os.environ.get(
    "GCP_PUBSUB_SUBSCRIPTION",
    "dev_internal_etl_error_subscription",
)
GCP_PUBSUB_TOPIC = os.environ.get("GCP_PUBSUB_TOPIC", "dev_internal_etl_error")


# BigQuery
GCP_BQ_TABLE_ETL_ERRORS = os.environ.get("GCP_BQ_TABLE_ETL_ERRORS")

# Logging
LOGGER_NAME = os.environ.get("LOGGER_NAME", "dev-etl-internal-error-handler")
LOGGER = "application"
