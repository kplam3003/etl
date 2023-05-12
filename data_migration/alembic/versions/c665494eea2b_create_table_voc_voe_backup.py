"""create_table_voc_voe_backup

Revision ID: c665494eea2b
Revises: eb0a113a0b89
Create Date: 2021-07-13 03:09:16.763703

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
revision = 'c665494eea2b'
down_revision = 'eb0a113a0b89'
branch_labels = None
depends_on = None

bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
fs = gcsfs.GCSFileSystem()

# Get GCS bucket
bucket_name = config.BUCKET_NAME

# Get Bigquery Database name:
project = config.GCP_PROJECT_ID
database_list = [config.STAGING, config.DWH, config.DATAMART, config.EXPORT, config.DATAMART_CS]
staging = config.STAGING
dwh = config.DWH
datamart = config.DATAMART
datamart_cs = config.DATAMART_CS


def upgrade():
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n Create table voc_bk and voe_bk in Staging is successfull!")
    time.sleep(15)
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade eb0a113a0b89 process is successfull!")  
     
query_string1 = f"""
CREATE OR REPLACE TABLE `{project}.{staging}.voc_bk`
PARTITION BY RANGE_BUCKET( case_study_id , GENERATE_ARRAY(1, 4000,5))
AS
SELECT * FROM `{project}.{staging}.voc`
;
CREATE OR REPLACE TABLE `{project}.{staging}.voe_bk`
PARTITION BY RANGE_BUCKET( case_study_id , GENERATE_ARRAY(1, 4000,5))
AS
SELECT * FROM `{project}.{staging}.voe`;

"""

query_string2 = f"""

DROP TABLE `{project}.{staging}.voc_bk`;

DROP TABLE `{project}.{staging}.voe_bk`;
"""

