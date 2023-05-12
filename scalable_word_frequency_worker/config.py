import os

# GCP
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', "leo-etlplatform")
PREFIX = os.environ.get('PREFIX', "dev")

GCP_STORAGE_BUCKET = os.environ.get('GCP_STORAGE_BUCKET', 'etl-datasource')

GCP_PUBSUB_TOPIC_DATA_CONSUME_AFTER_TASK = os.environ.get('GCP_PUBSUB_TOPIC_DATA_CONSUME_AFTER_TASK', 'dev_data_consume_aftertask')
GCP_PUBSUB_TOPIC_CS_INTERNAL_PROGRESS = os.environ.get('GCP_PUBSUB_TOPIC_CS_INTERNAL_PROGRESS', 'dev_cs_internal_progress')

PROGRESS_THRESHOLD = int(os.environ.get('PROGRESS_THRESHOLD', '1'))

# PubSub
GCP_PUBSUB_SUBSCRIPTION = os.environ.get('GCP_PUBSUB_SUBSCRIPTION', 'dev_word_frequency_worker')
GCP_PUBSUB_TOPIC = os.environ.get('GCP_PUBSUB_TOPIC', 'dev_word_frequency')

GCP_BQ_DATASET_ID_EXPORT = os.environ.get('GCP_BQ_DATASET_ID_EXPORT', 'export')
GCP_BQ_DATASET_ID_DATAMART = os.environ.get('GCP_BQ_DATASET_ID_DATAMART', 'datamart')

# BigQuery
GCP_BQ_TABLE_KEYWORDS_OUTPUT = os.environ.get('GCP_BQ_TABLE_KEYWORD_EXTRACTION_OUTPUT', 'leo-etlplatform.datamart.keywords_output')
GCP_BQ_TABLE_WORD_FREQUENCY = os.environ.get('GCP_BQ_TABLE_WORD_FREQUENCY', 'leo-etlplatform.datamart.word_frequency')

GCP_BQ_TABLE_VOC_KEYWORDS_OUTPUT = os.environ.get('GCP_BQ_TABLE_VOC_KEYWORDS_OUTPUT', 'leo-etlplatform.staging.voc_keywords_output')
GCP_BQ_TABLE_VOE_KEYWORDS_OUTPUT = os.environ.get('GCP_BQ_TABLE_VOE_KEYWORDS_OUTPUT', 'leo-etlplatform.staging.voe_keywords_output')

GCP_BQ_TABLE_VOC_SUMMARY_TABLE_PREFIX = os.environ.get('GCP_BQ_TABLE_VOC_SUMMARY_TABLE_PREFIX', 'leo-etlplatform.datamart_cs.summary_table_*')
GCP_BQ_TABLE_VOE_SUMMARY_TABLE_PREFIX = os.environ.get('GCP_BQ_TABLE_VOE_SUMMARY_TABLE_PREFIX', 'leo-etlplatform.datamart_cs.voe_summary_table_*')

# Logging
LOGGER_NAME = os.environ.get('LOGGER_NAME', 'dev-etl-postprocess-word-frequency')
if os.path.exists('.config'):
    LOGGER = 'local-test-application'
else:
    LOGGER = "application"

# app-specific
SLEEP = int(os.environ.get('SLEEP', 20))
NORMALIZE = True
CORPUS_BUILD_BATCH_SIZE = int(os.environ.get('CORPUS_BUILD_BATCH_SIZE', 30000))
GCP_BQ_LOAD_BATCH_SIZE = int(os.environ.get('GCP_BQ_LOAD_BATCH_SIZE', 100))

# # test topics and subscriptions
# GCP_PUBSUB_TOPIC_AFTER = 'hieu_test_message_sink'
# GCP_PUBSUB_SUBSCRIPTION = 'hieu_test_sub'
# GCP_PUBSUB_TOPIC = 'hieu_test_message_sink'
