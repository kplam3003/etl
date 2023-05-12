"""create_table_voe_job

Revision ID: 39face510f29
Revises: b101e9ed4818
Create Date: 2021-03-31 10:47:07.386889

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
revision = '39face510f29'
down_revision = 'b101e9ed4818'
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
        CREATE OR REPLACE TABLE `{project}.{staging}.voe_job`
        ( 
        `created_at` timestamp,
        `case_study_id` int64,
        `source_name` string,
        `source_id` int64,
        `company_name`  string,
        `company_id` int64,
        `job_id` string,
        `job_name`  string,
        `job_function` string,
        `job_type`  string,
        `posted_date` timestamp,
        `job_country` string,    
        `role_seniority` string,    
        `batch_id` int64,
        `batch_name` string,
        `file_name` string,
        `request_id` int64,
        `step_id` int64
        )

        PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5)) ;
        """

def upgrade():
    ##########  CREATE VOE_JOB TABLE IN STAGING #################

    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\nCreating voe_job tables in Staging successfull!")


query_string2 = f"""
        DROP  TABLE `{project}.{staging}.voe_job` ;
        """ 
def downgrade():

    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade b101e9ed4818 process is successfull!")
