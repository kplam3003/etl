"""create_table_word_frequency

Revision ID: 3456fcf0cb42
Revises: 549a47b793a1
Create Date: 2021-04-05 03:46:14.441468

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
revision = '3456fcf0cb42'
down_revision = '549a47b793a1'
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
        CREATE OR REPLACE TABLE `{project}.{datamart}.word_frequency`
        ( 
        `case_study_id` int64 NOT NULL,
        `run_id` string NOT NULL,
		`dimension_config_id` int64 NOT NULL,
        `keywords` string,
		`frequency` float64
        )

        PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5)) ;
        """

def upgrade():
    ##########  CREATE WORD_FREQUENCY TABLE IN DATAMART #################

    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\nCreating word_frequency` tables in Datamart successfull!")

query_string2 = f"""
        DROP  TABLE `{project}.{datamart}.word_frequency` ;
        """ 

def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 549a47b793a1 process is successfull!")
