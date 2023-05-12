import os

# GCP
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', "leo-etlplatform")
GCP_STORAGE_BUCKET = os.environ.get('GCP_STORAGE_BUCKET', 'etl-datasource')

# App
SECRET = 'IlLtqamrFVs3TVDIK3DctBZeyerP18o0c4kWuxroWBGthChpJbPKSw47qPannKWsOBrmxP5/1uXdxq3J26QjiA=='
CACHE_DIR = os.environ.get('CACHE_DIR', '/tmp')
MEMCACHED_SERVER = os.environ.get('MEMCACHED_SERVER', 'localhost')

# Logging
LOGGER_NAME = os.environ.get('LOGGER_NAME', 'dev-prediction-service')
LOGGER = "application"

# Machine learning models
# Job classifier
JOB_CLASSIFIER_MODEL_DIRECTORY = os.environ.get("JOB_CLASSIFIER_MODEL_DIRECTORY", "model_binaries/job_classifier")
JOB_CLASSIFIER_MODEL_FILE = os.environ.get("JOB_CLASSIFIER_MODEL_FILE", "default.onnx")
JOB_CLASSIFIER_LABEL_MAPPING = os.environ.get("JOB_CLASSIFIER_LABEL_MAPPING", "default.label")
