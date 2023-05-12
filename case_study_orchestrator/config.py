import os
import sys


# Env
ENV = os.environ.get('ENV', 'dev')

# project configs
DATABASE_URI = os.environ.get('DATABASE_URI')

GCP_PROJECT = os.environ.get('GCP_PROJECT')
GCP_ZONE = os.environ.get('GCP_ZONE')
GCP_MACHINE_IMAGE = os.environ.get('GCP_MACHINE_IMAGE')
GCP_STORAGE_BUCKET = os.environ.get('GCP_STORAGE_BUCKET')
GCP_DB_INSTANCE = os.environ.get('GCP_DB_INSTANCE')
GCP_PUBSUB_CRAWL_TOPIC_ID = os.environ.get('GCP_PUBSUB_CRAWL_TOPIC_ID')

# Logging
LOGGER_NAME = os.environ.get('LOGGER_NAME', 'dev-data-consume-orchestrator')
LOGGER = "application"

# PubSub Topic
# orchestrator topics
GCP_PUBSUB_TOPIC_DATA_CONSUME_AFTER_TASK = os.environ.get('GCP_PUBSUB_TOPIC_DATA_CONSUME_AFTER_TASK')
GCP_PUBSUB_TOPIC_DATA_CONSUME_DATA_FLOW = os.environ.get('GCP_PUBSUB_TOPIC_DATA_FLOW')
GCP_PUBSUB_TOPIC_CS_INTERNAL_PROGRESS = os.environ.get('GCP_PUBSUB_TOPIC_CS_INTERNAL_PROGRESS')
# worker topics
GCP_PUBSUB_TOPIC_NLP_WORKER = os.environ.get('GCP_PUBSUB_TOPIC_NLP_WORKER')
GCP_PUBSUB_TOPIC_CS_DATA_LOADER = os.environ.get('GCP_PUBSUB_TOPIC_CS_DATA_LOADER')
GCP_PUBSUB_TOPIC_KEYWORD_EXTRACT = os.environ.get('GCP_PUBSUB_TOPIC_KEYWORD_EXTRACT')
GCP_PUBSUB_TOPIC_EXPORT = os.environ.get('GCP_PUBSUB_TOPIC_EXPORT')
GCP_PUBSUB_TOPIC_WORD_FREQUENCY = os.environ.get('GCP_PUBSUB_TOPIC_WORD_FREQUENCY')
GCP_PUBSUB_TOPIC_NLP_INDEX = os.environ.get('GCP_PUBSUB_TOPIC_NLP_INDEX')
# web-app topics
GCP_PUBSUB_TOPIC_CS_PROGRESS = os.environ.get('GCP_PUBSUB_TOPIC_CS_PROGRESS')
GCP_PUBSUB_TOPIC_CS_ERROR = os.environ.get('GCP_PUBSUB_TOPIC_CS_ERROR')

# PubSub Subscription
# orchestrator subscriptions
GCP_PUBSUB_SUBSCRIPTION_DATA_CONSUME_AFTER_TASK = os.environ.get('GCP_PUBSUB_SUBSCRIPTION_DATA_CONSUME_AFTER_TASK')
GCP_PUBSUB_SUBSCRIPTION_DATA_CONSUME_DATA_FLOW = os.environ.get('GCP_PUBSUB_SUBSCRIPTION_DATA_CONSUME_DATA_FLOW')
GCP_PUBSUB_SUBSCRIPTION_CS_INTERNAL_PROGRESS = os.environ.get('GCP_PUBSUB_SUBSCRIPTION_CS_INTERNAL_PROGRESS')

# general settings
SLEEP = int(os.environ.get('SLEEP', '30'))

# general constants
STEP_TYPE_TO_TOPICS = {
    'nlp': GCP_PUBSUB_TOPIC_NLP_WORKER,
    'load_cs_data': GCP_PUBSUB_TOPIC_CS_DATA_LOADER,
    'keyword_extract': GCP_PUBSUB_TOPIC_KEYWORD_EXTRACT,
    'export': GCP_PUBSUB_TOPIC_EXPORT,
    'word_frequency': GCP_PUBSUB_TOPIC_WORD_FREQUENCY,
    'nlp_index': GCP_PUBSUB_TOPIC_NLP_INDEX
}

CASE_STUDY_FINISHED_TYPE = "__FINISH__"
CASE_STUDY_PREPARATION_STEPS = ('update_cs_data', 'nlp', 'load_cs_data')
CASE_STUDY_CALCULATION_STEPS = ('keyword_extract', 'export', 'word_frequency', 'nlp_index')
NEXT_STEP_TYPES = {
    'update_cs_data': 'nlp',
    'nlp': 'load_cs_data',
    'load_cs_data': 'keyword_extract',
    'keyword_extract': 'export',
    'export': 'word_frequency',
    'word_frequency': 'nlp_index',
    'nlp_index': CASE_STUDY_FINISHED_TYPE
}

EVENT_TO_STATUS = {
    'finish': 'finished',
    'fail': 'completed with error'
}

COMPLETE_STATUSES = ('finished', 'completed with error')
RUNNING_STATUSES = ('initing', 'waiting', 'running', '', None)