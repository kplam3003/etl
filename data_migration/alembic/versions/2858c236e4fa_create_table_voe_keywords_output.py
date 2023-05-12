"""create_table_voe_keywords_output

Revision ID: 2858c236e4fa
Revises: ed871119fe80
Create Date: 2021-06-01 07:23:28.329420

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
revision = '2858c236e4fa'
down_revision = 'ed871119fe80'
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
CREATE OR REPLACE TABLE `{project}.{staging}.voe_keywords_output`
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
    print("\nCreating voe_keywords_output tables in Staging successfull!")

query_string2 = f"""
DROP  TABLE `{project}.{staging}.voe_keywords_output` ;
""" 
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade ed871119fe80 process is successfull!")

