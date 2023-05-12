import os

# GCP
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', "leo-etlplatform")
PREFIX = os.environ.get('PREFIX', "dev")

GCP_STORAGE_BUCKET = os.environ.get('GCP_STORAGE_BUCKET', 'etl-datasource')

GCP_PUBSUB_TOPIC_DATA_CONSUME_AFTER_TASK = os.environ.get('GCP_PUBSUB_TOPIC_DATA_CONSUME_AFTER_TASK', 'dev_data_consume_aftertask')
GCP_PUBSUB_TOPIC_CS_INTERNAL_PROGRESS = os.environ.get('GCP_PUBSUB_TOPIC_CS_INTERNAL_PROGRESS', 'dev_cs_internal_progress')

PROGRESS_THRESHOLD = int(os.environ.get('PROGRESS_THRESHOLD', '1'))

# PubSub
GCP_PUBSUB_SUBSCRIPTION = os.environ.get('GCP_PUBSUB_SUBSCRIPTION', 'dev_keyword_extract_worker')
GCP_PUBSUB_TOPIC = os.environ.get('GCP_PUBSUB_TOPIC', 'dev_keyword_extract')

GCP_BQ_DATASET_ID_EXPORT = os.environ.get('GCP_BQ_DATASET_ID_EXPORT', 'export')
GCP_BQ_DATASET_ID_DATAMART = os.environ.get('GCP_BQ_DATASET_ID_DATAMART', 'datamart')

# BigQuery
# input
GCP_BQ_TABLE_SUMMARY_TABLE = os.environ.get('GCP_BQ_TABLE_SUMMARY_TABLE', 'leo-etlplatform.datamart.summary_table')
GCP_BQ_TABLE_SUMMARY_TABLE_PREFIX = os.environ.get('GCP_BQ_TABLE_SUMMARY_TABLE_PREFIX', 'leo-etlplatform.datamart_cs.*')
GCP_BQ_TABLE_SUMMARY_TABLE_ORIGIN = os.environ.get('GCP_BQ_TABLE_SUMMARY_TABLE_ORIGIN', 'leo-etlplatform.dwh.summary_table_origin')
GCP_BQ_TABLE_CASESTUDY_DIMENSION_CONFIG = os.environ.get('GCP_BQ_TABLE_CASESTUDY_DIMENSION_CONFIG', 'leo-etlplatform.dwh.casestudy_company_source')
GCP_BQ_TABLE_VOC = os.environ.get('GCP_BQ_TABLE_VOC', 'leo-etlplatform.staging.voc')
GCP_BQ_TABLE_VOE = os.environ.get('GCP_BQ_TABLE_VOE', 'leo-etlplatform.staging.voe')
GCP_BQ_TABLE_VOC_KEYWORDS_OUTPUT = os.environ.get('GCP_BQ_TABLE_VOC_KEYWORDS_OUTPUT', 'leo-etlplatform.staging.voc_keywords_output')
GCP_BQ_TABLE_VOE_KEYWORDS_OUTPUT = os.environ.get('GCP_BQ_TABLE_VOE_KEYWORDS_OUTPUT', 'leo-etlplatform.staging.voe_keywords_output')
GCP_BQ_TABLE_VOC_CUSTOM_DIMENSION = os.environ.get('GCP_BQ_TABLE_VOC_CUSTOM_DIMENSION', 'leo-etlplatform.staging.voc_custom_dimension')
GCP_BQ_TABLE_VOE_CUSTOM_DIMENSION = os.environ.get('GCP_BQ_TABLE_VOE_CUSTOM_DIMENSION', 'leo-etlplatform.staging.voe_custom_dimension')
GCP_BQ_TABLE_CORESIGNAL_EMPLOYEES = os.environ.get('GCP_BQ_TABLE_CORESIGNAL_EMPLOYEES', 'leo-etlplatform.staging.coresignal_employees')
GCP_BQ_TABLE_CORESIGNAL_EMPLOYEES_EXPERIENCES = os.environ.get('GCP_BQ_TABLE_CORESIGNAL_EMPLOYEES_EXPERIENCES', 'leo-etlplatform.staging.coresignal_employees_experiences')
GCP_BQ_TABLE_CORESIGNAL_EMPLOYEES_EDUCATION = os.environ.get('GCP_BQ_TABLE_CORESIGNAL_EMPLOYEES_EDUCATION', 'leo-etlplatform.staging.coresignal_employees_education')
GCP_BQ_TABLE_HRA_CASESTUDY_DIMENSION_CONFIG = os.environ.get('GCP_BQ_TABLE_HRA_CASESTUDY_DIMENSION_CONFIG', 'leo-etlplatform.dwh.hra_casestudy_dimension_config')
GCP_BQ_TABLE_HRA_EXPERIENCE_FUNCTION = os.environ.get('GCP_BQ_TABLE_HRA_EXPERIENCE_FUNCTION', 'leo-etlplatform.dwh.hra_experience_function')
GCP_BQ_TABLE_HRA_EDUCATION_DEGREE = os.environ.get('GCP_BQ_TABLE_HRA_EDUCATION_DEGREE', 'leo-etlplatform.dwh.hra_education_degree')

# output
GCP_BQ_TABLE_KEYWORDS_OUTPUT = os.environ.get('GCP_BQ_TABLE_KEYWORD_EXTRACTION_OUTPUT', 'leo-etlplatform.datamart.keywords_output')
GCP_BQ_TABLE_WORD_FREQUENCY = os.environ.get('GCP_BQ_TABLE_WORD_FREQUENCY', 'leo-etlplatform.datamart.word_frequency')

# Logging
LOGGER_NAME = os.environ.get('LOGGER_NAME', 'dev-etl-postprocess-keyword-extraction')
if os.path.exists('.config'):
    LOGGER = 'local-test-application'
else:
    LOGGER = "application"

# app-specific
SLEEP = int(os.environ.get('SLEEP', 20))
SPACY_MODEL = os.environ.get('SPACY_MODEL', 'en_core_web_md')
N_PROCESS = int(os.environ.get('SPACY_N_PROCESS', 2))
WORKER_BATCH_SIZE = int(os.environ.get('WORKER_BATCH_SIZE', 10000))
SPACY_BATCH_SIZE = int(os.environ.get('SPACY_BATCH_SIZE', 64))

INCLUDE_STOPWORDS = ["lot", "lots", "alot"]
EXCLUDE_STOPWORDS = ["no", 'not', "n't", "n\'t", "nt"]
SPACY_DISABLE_COMPONENTS = ['ner']
EXTRACTION_MODE = 'base'

ENCODE_SENTENCE_API = os.environ.get("ENCODE_SENTENCE_API", "http://dev-prediction-service:8083/encode-sentence")