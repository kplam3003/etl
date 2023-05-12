"""update table language_trans

Revision ID: c794828e7a01
Revises: 7732d577560c
Create Date: 2021-03-04 09:20:42.160761

"""
from alembic import op
import sqlalchemy as sa

from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage
import gcsfs
import pandas as pd
import ast
import time
import config
# revision identifiers, used by Alembic.
revision = 'c794828e7a01'
down_revision = '7732d577560c'
branch_labels = None
depends_on = None

bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
fs = gcsfs.GCSFileSystem()


# Get GCS bucket
bucket_name = config.BUCKET_NAME

# Get Bigquery Database name:
project = config.GCP_PROJECT_ID
database_list = [config.STAGING, config.DWH, config.DATAMART, config.EXPORT]
staging = config.STAGING
dwh = config.DWH
datamart = config.DATAMART

def upgrade():
    """update table language_trans"""
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n Upgrade c794828e7a01 process is successfull!")
    
def downgrade():
    pass
    print("\n Downgrade c794828e7a01 process is successfull!")

query_string1 = f"""

UPDATE `{project}.{dwh}.language_trans` 
SET language_code = 
    CASE
        WHEN lower(language_code) like '%hmn%' THEN 'hmn'
        ElSE language_code
    END,
    language = 
    CASE
        WHEN language_code = 'und' THEN language_code
        ElSE language
    END
WHERE 1=1;

"""

