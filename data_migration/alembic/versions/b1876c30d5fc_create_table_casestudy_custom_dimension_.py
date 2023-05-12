"""create_table_casestudy_custom_dimension_statistics

Revision ID: b1876c30d5fc
Revises: f529a27834df
Create Date: 2021-10-12 11:37:33.259015

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
revision = 'b1876c30d5fc'
down_revision = 'f529a27834df'
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
        CREATE OR REPLACE TABLE `{project}.{dwh}.casestudy_custom_dimension_statistics`
        ( 
        `created_at` TIMESTAMP,
        `case_study_id` INTEGER,
        `case_study_name` STRING,
        `dimension_config_id` INTEGER,
        `dimension_config_name` STRING,
        `dimension` STRING,
        `label` STRING,
        `word` STRING,
        `word_count` INTEGER,
        `review_count` INTEGER,
        `run_id` STRING
        )

        PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5)) ;
        """
query_string2 = f"""
        DROP  TABLE `{project}.{dwh}.casestudy_custom_dimension_statistics` ;
        """ 

def upgrade():
    ##########  CREATE CASE_STUDY_CUSTOM_DIMENSION_STATISTICS TABLE IN DWH #################

    query_job = bqclient.query(query_string1)
    query_job.result()
    print("\nCreating casestudy_custom_dimension_statistics` tables in dwh successfull!")


def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade f529a27834df process is successfull!")
