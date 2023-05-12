"""update_etl_batch_review_schema

Revision ID: f3dbd03f74a6
Revises: dcdbc2022ba4
Create Date: 2021-03-19 03:44:14.492428

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
revision = 'f3dbd03f74a6'
down_revision = 'dcdbc2022ba4'
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

query_string1 =f"""
        CREATE OR REPLACE TABLE `{project}.{data_quality}.etl_batch_review_monitor_backup` AS 
        SELECT 
        *  except(rows_per_second),
        CAST(rows_per_second AS float64) AS rows_per_second
        FROM `{project}.{data_quality}.etl_batch_review_monitor`;
        DROP TABLE `{project}.{data_quality}.etl_batch_review_monitor`;
            
    """
query_string2 = f"""
        CREATE OR REPLACE table `{project}.{data_quality}.etl_batch_review_monitor`
            (
            `request_id` int64 NOT NULL,
            `case_study_id` int64 NOT NULL,
            `batch_id` int64 ,
            `batch_name` string ,
            `company_id` int64 ,
            `company_name` string,
            `source_id` int64 ,
            `source_name` string,
            `step_type` string ,
            `step_status` string ,
            `step_lead_time` int64,
            `num_files` int64,
            `total_rows` int64,
            `num_rows_in_files` int64,
            `num_unique_reviews` int64,
            `num_input` int64,
            `num_output` int64,
            `success_percent` float64,
            `inserted_at` timestamp,
            `rows_per_second` float64,

            )

        PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5)) 

        ;

        INSERT INTO `{project}.{data_quality}.etl_batch_review_monitor`( request_id, case_study_id, batch_id, batch_name, company_id, company_name, source_id, source_name, step_type, step_status, step_lead_time, num_files, total_rows, num_rows_in_files, num_unique_reviews, num_input, num_output, success_percent, inserted_at, rows_per_second )
        SELECT * 
        FROM `{project}.{data_quality}.etl_batch_review_monitor_backup`;

        DROP TABLE `{project}.{data_quality}.etl_batch_review_monitor_backup`;
    """	
def upgrade():
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\nCreate and copy data to table etl_batch_review_monitor_backup in Data Quality successful!")
    time.sleep(15)

    
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\nCreate and insert data into table elt_batch_review_monitor in Data Quality successful!")
    time.sleep(15)


def downgrade():
    pass