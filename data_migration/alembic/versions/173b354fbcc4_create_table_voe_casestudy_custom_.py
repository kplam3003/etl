"""create_table_voe_casestudy_custom_dimension_statistics

Revision ID: 173b354fbcc4
Revises: b1876c30d5fc
Create Date: 2021-10-14 17:37:05.843868

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
revision = '173b354fbcc4'
down_revision = 'b1876c30d5fc'
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
        CREATE OR REPLACE TABLE `{project}.{dwh}.voe_casestudy_custom_dimension_statistics`
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
        CREATE
        OR REPLACE TABLE `{project}.{dwh}.voe_casestudy_custom_dimension_statistics_bk` as
        select
            *
        from
            `{project}.{dwh}.voe_casestudy_custom_dimension_statistics`;

        DROP TABLE `{project}.{dwh}.voe_casestudy_custom_dimension_statistics`; 
        """

def upgrade():
    ##########  CREATE VOE_CASE_STUDY_CUSTOM_DIMENSION_STATISTICS TABLE IN DWH #################

    query_job = bqclient.query(query_string1)
    query_job.result()
    print("\nCreating voe_casestudy_custom_dimension_statistics` tables in dwh successfull!")


def downgrade():
    query_job = bqclient.query(query_string2)
    query_job.result()
    print("\n Downgrade b1876c30d5fc process is successfull!")
