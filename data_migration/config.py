import os


ETL_DATABASE_URI = os.environ.get('ETL_DATABASE_URI',  'postgres+psycopg2://postgres:password@localhost:5433/test_data')

GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
DATAMART = os.environ.get('GCP_DATAMART')
DATAMART_CS = os.environ.get('GCP_DATAMART_CS')
DWH = os.environ.get('GCP_DWH')
STAGING = os.environ.get('GCP_STAGING')
EXPORT = os.environ.get('GCP_EXPORT')
DATA_QUALITY=os.environ.get('GCP_DATA_QUALITY')

GCP_STORAGE_CORS_WEBURL=os.environ.get('GCP_STORAGE_CORS_WEBURL')
BUCKET_NAME = os.environ.get('BUCKET_NAME')

PREFIX = os.environ.get('PREFIX', 'test')
GOOGLE_APPLICATION_CREDENTIALS = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')

# MONGODB
MONGODB_DATABASE_URI = os.environ.get("MONGODB_DATABASE_URI", "mongodb://172.16.11.24:27018")
MONGODB_DATABASE_NAME = os.environ.get("MONGODB_DATABASE_NAME", "dev_dp_leonardo_db")
MONGODB_DATABASE_ROOT_CA = os.environ.get("MONGODB_DATABASE_ROOT_CA", "/app/certs/root-ca.pem")
MONGODB_DATABASE_KEY_FILE = os.environ.get("MONGODB_DATABASE_KEY_FILE", "/app/certs/dpworker.pem")

MONGODB_VOC_REVIEW_COLLECTION = "voc"
MONGODB_VOE_REVIEW_COLLECTION = "voe"
MONGODB_VOE_OVERVIEW_COLLECTION = "voe_overview"
MONGODB_VOE_JOB_COLLECTION = "voe_job"
MONGODB_VOC_REVIEW_STATS = "voc_review_stats"
MONGODB_VOE_REVIEW_STATS = "voe_review_stats"
MONGODB_CORESIGNAL_STATS = "coresignal_stats"
MONGODB_CORESIGNAL_EMPLOYEES = "coresignal_employees"
MONGODB_CORESIGNAL_CD_MAPPING = "coresignal_company_datasource"
MONGODB_GOOGLE_MAPS_PLACES_CACHE = "google_maps_places_cache"
