"""create_data_quality_elt_lead_time_monitor_table

Revision ID: 34187b6db7a3
Revises: 069d86599e52
Create Date: 2021-03-10 08:01:48.053252

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
revision = '34187b6db7a3'
down_revision = '069d86599e52'
branch_labels = None
depends_on = None

bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
fs = gcsfs.GCSFileSystem()


# Get GCS bucket
bucket_name = config.BUCKET_NAME

# Get Bigquery Database name:
project = config.GCP_PROJECT_ID
database_list = [config.DATA_QUALITY]
data_quality=config.DATA_QUALITY



def upgrade():
    
    query_string1 = f"""
        CREATE OR REPLACE TABLE `{project}.{data_quality}.etl_lead_time_monitor`
            (
            `request_id` int64 NOT NULL,
            `case_study_id` int64 NOT NULL,
            `case_study_name` string NOT NULL,
            `request_status` string NOT NULL,
            `request_created_at` timestamp NOT NULL,
            `request_updated_at` timestamp NOT NULL,
            `request_lead_time` int64,
            `batch_id` int64 NOT NULL,
            `batch_name` string NOT NULL,
            `batch_status` string NOT NULL,
            `batch_created_at` timestamp NOT NULL,
            `batch_updated_at` timestamp NOT NULL,
            `batch_lead_time` int64,
            `step_id` int64 NOT NULL,
            `step_name` string NOT NULL,
            `step_type` string NOT NULL,
            `step_status` string NOT NULL,
            `step_created_at` timestamp NOT NULL,
            `step_updated_at` timestamp NOT NULL,
            `step_lead_time` int64,
            `step_detail_id` int64,
            `step_detail_name` string,
            `step_detail_status` string,
            `step_detail_created_at` timestamp,
            `step_detail_updated_at` timestamp,
            `step_detail_lead_time` int64,
            `item_count` int64,
            `company_id` int64 NOT NULL,
            `company_name` string,
            `source_id` int64 NOT NULL,
            `source_name` string,
            `inserted_at` timestamp NOT NULL
            )

        PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5)) 
        ;

    """
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\nCreating table elt_lead_time_monitor in Data Quality successfull!")
    

def downgrade():
    
    query_string2 = f"""
        DROP  TABLE `{project}.{data_quality}.etl_lead_time_monitor` ;
        """
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 34187b6db7a3 process is successfull!")