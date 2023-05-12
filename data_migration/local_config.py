import os


ETL_DATABASE_URI = os.environ.get(
    "ETL_DATABASE_URI",
    "postgres+psycopg2://postgres:password@localhost:5432/hieu_local_mdm_test",
)


GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
DATAMART = os.environ.get("GCP_DATAMART")
DATAMART_CS = os.environ.get("GCP_DATAMART_CS")
DWH = os.environ.get("GCP_DWH")
STAGING = os.environ.get("GCP_STAGING")
EXPORT = os.environ.get("GCP_EXPORT")
DATA_QUALITY = os.environ.get("GCP_DATA_QUALITY")

GCP_STORAGE_CORS_WEBURL = os.environ.get("GCP_STORAGE_CORS_WEBURL")
BUCKET_NAME = os.environ.get("BUCKET_NAME")

PREFIX = os.environ.get("PREFIX", "test")
GOOGLE_APPLICATION_CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

# MONGODB
MONGODB_DATABASE_URI = os.environ.get("MONGODB_DATABASE_URI")
MONGODB_DATABASE_NAME = os.environ.get("MONGODB_DATABASE_NAME")
