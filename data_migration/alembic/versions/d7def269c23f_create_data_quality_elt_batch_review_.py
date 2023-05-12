"""create_data_quality_elt_batch_review_monitor

Revision ID: d7def269c23f
Revises: 34187b6db7a3
Create Date: 2021-03-10 08:12:49.570113

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
revision = 'd7def269c23f'
down_revision = '34187b6db7a3'
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
        CREATE OR REPLACE table `{project}.{data_quality}.etl_batch_review_monitor`
            (
            `request_id` int64 NOT NULL,
            `case_study_id` int64 NOT NULL,
            `batch_id` int64 NOT NULL,
            `batch_name` string NOT NULL,
            `company_id` int64 NOT NULL,
            `company_name` string NOT NULL,
            `source_id` int64 NOT NULL,
            `source_name` string NOT NULL,
            `step_type` string NOT NULL,
            `step_status` string NOT NULL,
            `step_lead_time` int64,
            `number_files` int64,
            `total_rows` int64,
            `num_rows_in_files` int64,
            `num_unique_reviews` int64,
            `rows_per_second` string,
            `num_input` int64,
            `num_output` int64,
            `success_percent` float64,
            `inserted_at` timestamp NOT NULL    
            )

        PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5)) 

        ;
    """
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\nCreating table elt_batch_review_monitor in Data Quality successfull!")


def downgrade():
    
    query_string2 = f"""
        DROP  TABLE `{project}.{data_quality}.etl_batch_review_monitor` ;
        """
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade d7def269c23f process is successfull!")
