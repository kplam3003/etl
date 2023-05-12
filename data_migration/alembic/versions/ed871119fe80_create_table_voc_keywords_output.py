"""create_table_voc_keywords_output

Revision ID: ed871119fe80
Revises: 872980087278
Create Date: 2021-06-01 07:23:13.201824

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
revision = 'ed871119fe80'
down_revision = '872980087278'
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

query_string1 = f"""
CREATE OR REPLACE TABLE `{project}.{staging}.voc_keywords_output`
    ( 
    `case_study_id` int64 NOT NULL,
    `review_id` string NOT NULL, 
    `trans_review`  string,
    `keywords` string
    )

PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5)) ;
"""
def upgrade():
    ##########  CREATE VOC_KEYWORDS_OUTPUT TABLE IN STAGING #################

    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\nCreating voc_keywords_output tables in Staging successfull!")

query_string2 = f"""
DROP  TABLE `{project}.{staging}.voc_keywords_output` ;
""" 
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 872980087278 process is successfull!")
