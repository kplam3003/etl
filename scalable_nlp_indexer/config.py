import os

# GCP
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', "leo-etlplatform")
PREFIX = os.environ.get('PREFIX', "dev")

GCP_STORAGE_BUCKET = os.environ.get('GCP_STORAGE_BUCKET', 'etl-datasource')

GCP_PUBSUB_TOPIC_DATA_CONSUME_AFTER_TASK = os.environ.get('GCP_PUBSUB_TOPIC_DATA_CONSUME_AFTER_TASK', 'dev_data_consume_aftertask')
GCP_PUBSUB_TOPIC_CS_INTERNAL_PROGRESS = os.environ.get('GCP_PUBSUB_TOPIC_CS_INTERNAL_PROGRESS', 'dev_cs_internal_progress')

PROGRESS_THRESHOLD = int(os.environ.get('PROGRESS_THRESHOLD', '1'))

# PubSub
GCP_PUBSUB_TOPIC = os.environ.get('GCP_PUBSUB_TOPIC', 'nlp_index')
GCP_PUBSUB_SUBSCRIPTION = os.environ.get('GCP_PUBSUB_SUBSCRIPTION', 'nlp_indexer')

GCP_BQ_DATASET_ID_EXPORT = os.environ.get('GCP_BQ_DATASET_ID_EXPORT', 'export')
GCP_BQ_DATASET_ID_DATAMART = os.environ.get('GCP_BQ_DATASET_ID_DATAMART', 'datamart')

GCP_BQ_TABLE_VOC = os.environ.get('GCP_BQ_TABLE_VOC', 'leo-etlplatform.staging.voc')
GCP_BQ_TABLE_VOE = os.environ.get('GCP_BQ_TABLE_VOE', 'leo-etlplatform.staging.voe')

GCP_BQ_TABLE_BATCH_STATUS = os.environ.get('GCP_BQ_TABLE_BATCH_STATUS', 'leo-etlplatform.staging.batch_status')
GCP_BQ_TABLE_VOE_BATCH_STATUS = os.environ.get('GCP_BQ_TABLE_VOE_BATCH_STATUS', 'leo-etlplatform.staging.voe_batch_status')

GCP_BQ_TABLE_CASESTUDY_COMPANY_SOURCE = os.environ.get('GCP_BQ_TABLE_CASESTUDY_COMPANY_SOURCE', 'leo-etlplatform.dwh.casestudy_company_source')
GCP_BQ_TABLE_VOE_CASESTUDY_COMPANY_SOURCE = os.environ.get('GCP_BQ_TABLE_VOE_CASESTUDY_COMPANY_SOURCE', 'leo-etlplatform.dwh.voe_casestudy_company_source')

GCP_BQ_TABLE_NLP_OUTPUT_CASE_STUDY = os.environ.get('GCP_BQ_TABLE_NLP_OUTPUT_CASE_STUDY', 'leo-etlplatform.dwh.nlp_output_case_study')
GCP_BQ_TABLE_VOE_NLP_OUTPUT_CASE_STUDY = os.environ.get('GCP_BQ_TABLE_VOE_NLP_OUTPUT_CASE_STUDY', 'leo-etlplatform.dwh.voe_nlp_output_case_study')

GCP_BQ_TABLE_SUMMARY_TABLE_PREFIX = os.environ.get('GCP_BQ_TABLE_SUMMARY_TABLE_PREFIX', 'leo-etlplatform.datamart_cs.summary_table')
GCP_BQ_TABLE_SUMMARY_TABLE_VOE_PREFIX = os.environ.get('GCP_BQ_TABLE_SUMMARY_TABLE_VOE_PREFIX', 'leo-etlplatform.datamart_cs.voe_summary_table')
GCP_BQ_TABLE_SUMMARY_TABLE_VOE_JOB_PREFIX = os.environ.get('GCP_BQ_TABLE_SUMMARY_TABLE_VOE_JOB_PREFIX', 'leo-etlplatform.datamart_cs.voe_job_summary_table')

GCP_BQ_TABLE_CASESTUDY_DIMENSION_CONFIG = os.environ.get('GCP_BQ_TABLE_CASESTUDY_DIMENSION_CONFIG', 'leo-etlplatform.dwh.casestudy_dimension_config')
GCP_BQ_TABLE_VOE_CASESTUDY_DIMENSION_CONFIG = os.environ.get('GCP_BQ_TABLE_VOE_CASESTUDY_DIMENSION_CONFIG', 'leo-etlplatform.dwh.voe_casestudy_dimension_config')

GCP_BQ_TABLE_LANGUAGE_TRANS = os.environ.get('GCP_BQ_TABLE_LANGUAGE_TRANS', 'leo-etlplatform.dwh.language_trans')
GCP_BQ_TABLE_VOE_LANGUAGE_TRANS = os.environ.get('GCP_BQ_TABLE_VOE_LANGUAGE_TRANS', 'leo-etlplatform.dwh.voe_language_trans')

GCP_BQ_TABLE_POLARITY_TRANS = os.environ.get('GCP_BQ_TABLE_POLARITY_TRANS', 'leo-etlplatform.dwh.polarity_trans')
GCP_BQ_TABLE_VOE_POLARITY_TRANS = os.environ.get('GCP_BQ_TABLE_VOE_POLARITY_TRANS', 'leo-etlplatform.dwh.voe_polarity_trans')

GCP_BQ_TABLE_CASESTUDY_BATCHID = os.environ.get('GCP_BQ_TABLE_CASESTUDY_BATCHID', 'leo-etlplatform.staging.casestudy_batchid')
GCP_BQ_TABLE_VOE_CASESTUDY_BATCHID = os.environ.get('GCP_BQ_TABLE_VOE_CASESTUDY_BATCHID', 'leo-etlplatform.staging.voe_casestudy_batchid')

GCP_BQ_TABLE_CASESTUDY_RUN_ID = os.environ.get('GCP_BQ_TABLE_CASESTUDY_RUN_ID', 'leo-etlplatform.staging.case_study_run_id')
GCP_BQ_TABLE_VOE_CASESTUDY_RUN_ID = os.environ.get('GCP_BQ_TABLE_VOE_CASESTUDY_RUN_ID', 'leo-etlplatform.staging.voe_case_study_run_id')

GCP_BQ_TABLE_VOE_JOB = os.environ.get('GCP_BQ_TABLE_VOE_JOB', 'leo-etlplatform.staging.voe_job')
GCP_BQ_TABLE_VOE_COMPANY = os.environ.get('GCP_BQ_TABLE_VOE_COMPANY', 'leo-etlplatform.staging.voe_company')

GCP_BQ_TABLE_DIMENSION_DEFAULT = os.environ.get('GCP_BQ_TABLE_DIMENSION_DEFAULT', "leo-etlplatform.dwh.dimension_default")

GCP_BQ_TABLE_DIMENSION_KEYWORD_LIST = os.environ.get('GCP_BQ_TABLE_DIMENSION_KEYWORD_LIST', "leo-etlplatform.dwh.dimension_keyword_list")
GCP_BQ_TABLE_VOE_DIMENSION_KEYWORD_LIST = os.environ.get('GCP_BQ_TABLE_VOE_DIMENSION_KEYWORD_LIST', "leo-etlplatform.dwh.voe_dimension_keyword_list")

GCP_BQ_TABLE_COMPANY_ALIASES_LIST = os.environ.get('GCP_BQ_TABLE_COMPANY_ALIASES_LIST', "leo-etlplatform.dwh.company_aliases_list")
GCP_BQ_TABLE_VOE_COMPANY_ALIASES_LIST = os.environ.get('GCP_BQ_TABLE_VOE_COMPANY_ALIASES_LIST', "leo-etlplatform.dwh.voe_company_aliases_list")

GCP_BQ_TABLE_PARENT_REVIEW_MAPPING = os.environ.get('GCP_BQ_TABLE_PARENT_REVIEW_MAPPING', "leo-etlplatform.staging.parent_review_mapping")
GCP_BQ_TABLE_VOE_PARENT_REVIEW_MAPPING = os.environ.get('GCP_BQ_TABLE_VOE_PARENT_REVIEW_MAPPING', "leo-etlplatform.staging.voe_parent_review_mapping")

GCP_BQ_TABLE_REVIEW_COUNTRY_MAPPING = os.environ.get('GCP_BQ_TABLE_REVIEW_COUNTRY_MAPPING', "leo-etlplatform.staging.review_country_mapping")
GCP_BQ_TABLE_VOE_REVIEW_COUNTRY_MAPPING = os.environ.get('GCP_BQ_TABLE_VOE_REVIEW_COUNTRY_MAPPING', "leo-etlplatform.staging.voe_review_country_mapping")

# Logging
LOGGER_NAME = os.environ.get('LOGGER_NAME', 'dev-etl-postprocess-nlp-indexer')
if os.path.exists('.config'):
    LOGGER = 'local-test-application'
else:
    LOGGER = "application"

# app-specific
SLEEP = int(os.environ.get('SLEEP', 20))
NORMALIZE = True
CORPUS_BUILD_BATCH_SIZE = int(os.environ.get('CORPUS_BUILD_BATCH_SIZE', 30000))
GCP_BQ_LOAD_BATCH_SIZE = int(os.environ.get('GCP_BQ_LOAD_BATCH_SIZE', 100))


# Elastic Search
CASE_STUDY_NLP_INDEX_PREFIX = os.environ.get('CASE_STUDY_NLP_INDEX_PREFIX', 'dev_nlp_index_csid_')
ELASTICSEARCH_HOST = os.environ.get('ELASTICSEARCH_HOST', 'http://104.196.250.110:9200')
NLP_INDEX_BATCH_SIZE = int(os.environ.get("NLP_INDEX_BATCH_SIZE", 1000))
