"""create_table_parent_review_mapping

Revision ID: 0377b5317080
Revises: 465711e15c62
Create Date: 2021-07-09 06:26:06.048467

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
revision = '0377b5317080'
down_revision = '465711e15c62'
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
    print("\n Create table parent_review_mapping on STAGING successfull!")
    time.sleep(15)


def downgrade():

    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 465711e15c62 process is successfull!")
	
query_string1 = f"""
CREATE OR REPLACE TABLE `{project}.{staging}.parent_review_mapping` 
(   `created_at` timestamp,
    `case_study_id` int64,
    `company_id`  int64, 
    `source_id`  int64,
    `request_id` int64,
    `batch_id`  int64,
	`file_name` string,
    `parent_review_id` string NOT NULL,
    `review_id` string NOT NULL,
    `technique_type` string NOT NULL
   )
PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5));
"""

query_string2 = f"""
DROP TABLE `{project}.{staging}.parent_review_mapping` ;
"""