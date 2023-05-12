"""update table language_trans

Revision ID: 0534bb3684cb
Revises: b5ffcc35fea7
Create Date: 2021-02-18 10:52:23.600137

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
revision = '0534bb3684cb'
down_revision = '76de71bdfd61'
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
    print("\n Upgrade 0534bb3684cb process is successfull!")
    
def downgrade():
    pass
    print("\n Downgrade 0534bb3684cb process is successfull!")

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

