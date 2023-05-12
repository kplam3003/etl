"""create_table_voe_company_aliases_list

Revision ID: 35002bc44d80
Revises: e896ddeea56c
Create Date: 2021-05-18 09:25:38.561076

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
revision = '35002bc44d80'
down_revision = 'e896ddeea56c'
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
    print("\n Create table voe_company_aliases_list on DWH successfull!")
	
  
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade e896ddeea56c process is successfull!")

query_string1 = f"""
CREATE OR REPLACE TABLE `{project}.{dwh}.voe_company_aliases_list` 
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
DROP TABLE `{project}.{dwh}.voe_company_aliases_list` ;
"""
