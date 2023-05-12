import os

# BEGIN https://jira.tpptechnology.com/browse/TPPLEO-548
DATABASE_URI = os.environ.get('DATABASE_URI')

GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', "leo-etlplatform")
GCP_PUBSUB_TOPIC_CS_PROGRESS = os.environ.get('GCP_PUBSUB_TOPIC_CS_PROGRESS', 'dev_cs_progress')
GCP_PUBSUB_TOPIC_COMPANY_DATASOURCE_PROGRESS = os.environ.get('GCP_PUBSUB_TOPIC_COMPANY_DATASOURCE_PROGRESS', 'dev_company_datasource_progress')
GCP_PUBSUB_TOPIC_DATA_FLOW = os.environ.get('GCP_PUBSUB_TOPIC_DATA_FLOW', 'dev_data_flow') # for data_tranform
GCP_PUBSUB_TOPIC_DATA_CONSUME_DATA_FLOW = os.environ.get('GCP_PUBSUB_TOPIC_DATA_CONSUME_DATA_FLOW', 'dev_data_consume_data_flow') # for data_consume

# subscriptions
GCP_PUBSUB_SUBSCRIPTION_REQUESTS_WEBPLATFORM = os.environ.get('GCP_PUBSUB_SUBSCRIPTION_REQUESTS_WEBPLATFORM')

# others
SOURCE_ID_GOOGLE_PLAY = int(os.environ.get('SOURCE_ID_GOOGLE_PLAY', '1'))
SOURCE_ID_APPLE_STORE = int(os.environ.get('SOURCE_ID_APPLE_STORE', '2'))
SOURCE_ID_CAPTERRA = int(os.environ.get('SOURCE_ID_CAPTERRA', '3'))
SOURCE_ID_G2 = int(os.environ.get('SOURCE_ID_G2', '4'))
SOURCE_ID_TRUSTPILOT = int(os.environ.get('SOURCE_ID_TRUSTPILOT', '5'))
SOURCE_ID_GLASSDOOR = int(os.environ.get('SOURCE_ID_GLASSDOOR', '45'))
SOURCE_ID_INDEED = int(os.environ.get('SOURCE_ID_INDEED', '46'))
SOURCE_ID_LINKEDIN = int(os.environ.get('SOURCE_ID_LINKEDIN', '47'))
# END https://jira.tpptechnology.com/browse/TPPLEO-548
STATUS_WAITING = 'waiting' # to be updated to running
COMPANY_DATASOURCE_REQUEST_TYPE = 'company_datasource'
CASE_STUDY_REQUEST_TYPE = 'case_study'
SYNC_ACTION = 'sync'
DIMENSION_CHANGE_ACTION = 'dimension_change'
UPDATE_ACTION = 'update'
FINISHED_STATUS = 'finished'
HRA_DATA_TYPE = 'HRA'
VOE_DATA_TYPE = 'VoE'
VOC_DATA_TYPE = 'VoC'
GOOGLE_TRANSLATION_SERVICE = 'Google Translation'
CORESIGNAL_STATS_DATA_TYPE  = 'coresignal_stats'
CORESIGNAL_EMPLOYEES_DATA_TYPE = 'coresignal_employees'
REVIEW_DATA_TYPE = 'review'
JOB_DATA_TYPE = 'job'
REVIEW_STATS_DATA_TYPE = 'review_stats'
PROGRESS_EVENT = 'progress'
DATA_CONSUME_EVENT= 'data_consume'
DATA_TRANSFORM_EVENT = 'data_transform'
PROGRESS = -1.0

# Logging
LOGGER_NAME = os.environ.get('LOGGER_NAME', 'dev-web-synchronizer-v3')
LOGGER = "application"

# configs
COMPANY_DATASOURCE_PAYLOAD_MANDATORY_FIELDS = (
    "type",
    "data_type",
    "company_datasource_id",
    "company_id",
    "company_name",
    "source_id",
    "source_name",
    "source_code",
    "source_type",
    "urls",
    "nlp_type",
    "translation_service"
)

CASE_STUDY_PAYLOAD_MANDATORY_FIELDS = (
    "type",
    "action",
    "case_study_id",
    "case_study_name",
    "company_datasources",
    "dimension_config",
    "exports",
    "polarities",
    "relative_relevance",
    "absolute_relevance",
    "schedule_time",
    "translation_service"
)

DATA_CONSUME_STEPS = {
    'nlp': 'load_cs_data',
    'load_cs_data': 'keyword_extract',
    'keyword_extract': 'export',
    'export': 'word_frequency',
    'word_frequency': 'nlp_index',
    'nlp_index': 'END'
}

