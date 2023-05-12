"""create_table_keywords_output

Revision ID: 549a47b793a1
Revises: bfcc11502842
Create Date: 2021-04-05 03:45:48.864610

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
revision = '549a47b793a1'
down_revision = 'bfcc11502842'
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

query_string1 = f"""
        CREATE OR REPLACE TABLE `{project}.{datamart}.keywords_output`
        ( 
        `case_study_id` int64 NOT NULL,
        `run_id` string NOT NULL,
		`dimension_config_id` int64 NOT NULL,
        `review_id` string NOT NULL, 
        `trans_review`  string,
        `keywords` string
        )

        PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5)) ;
        """
def upgrade():
    ##########  CREATE KEYWORDS_OUTPUT TABLE IN Datamart #################

    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\nCreating keywords_output tables in Datamart successfull!")

query_string2 = f"""
        DROP  TABLE `{project}.{datamart}.keywords_output` ;
        """ 
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade bfcc11502842 process is successfull!")
