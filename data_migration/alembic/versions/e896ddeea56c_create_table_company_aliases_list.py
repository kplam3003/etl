"""create_table_company_aliases_list

Revision ID: e896ddeea56c
Revises: 5bbc76e9bbe1
Create Date: 2021-05-18 08:01:41.242346

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
revision = 'e896ddeea56c'
down_revision = '5bbc76e9bbe1'
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
    print("\n Create table company_aliases_list on DWH successfull!")
	
  
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 5bbc76e9bbe1 process is successfull!")

query_string1 = f"""
CREATE OR REPLACE TABLE `{project}.{dwh}.company_aliases_list` 
(
    `created_at` timestamp,
    `case_study_id` int64,
    `company_id`  int64,
    `company_name`  string,
    `aliases`  string,
    `run_id`  string,
	)
PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5));
"""

query_string2 = f"""
DROP TABLE `{project}.{dwh}.company_aliases_list` ;
"""