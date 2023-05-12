"""update_etl_lead_time_schema

Revision ID: dcdbc2022ba4
Revises: 1f445620ada3
Create Date: 2021-03-19 03:43:27.134038

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
revision = 'dcdbc2022ba4'
down_revision = '1f445620ada3'
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
        CREATE OR REPLACE TABLE `{project}.{data_quality}.etl_lead_time_monitor_backup` AS 
        SELECT 
        * 
        FROM `{project}.{data_quality}.etl_lead_time_monitor`;
        DROP TABLE `{project}.{data_quality}.etl_lead_time_monitor`;
    """

query_string2 = f"""
        CREATE OR REPLACE TABLE `{project}.{data_quality}.etl_lead_time_monitor`
            (
            `request_id` int64 NOT NULL,
            `case_study_id` int64 NOT NULL,
            `case_study_name` string ,
            `request_status` string ,
            `request_created_at` timestamp,
            `request_updated_at` timestamp ,
            `request_lead_time` int64,
            `batch_id` int64 ,
            `batch_name` string ,
            `batch_status` string ,
            `batch_created_at` timestamp ,
            `batch_updated_at` timestamp ,
            `batch_lead_time` int64,
            `step_id` int64 ,
            `step_name` string ,
            `step_type` string ,
            `step_status` string ,
            `step_created_at` timestamp ,
            `step_updated_at` timestamp ,
            `step_lead_time` int64,
            `step_detail_id` int64,
            `step_detail_name` string,
            `step_detail_status` string,
            `step_detail_created_at` timestamp,
            `step_detail_updated_at` timestamp,
            `step_detail_lead_time` int64,
            `item_count` int64,
            `company_id` int64 ,
            `company_name` string,
            `source_id` int64 ,
            `source_name` string,
            `inserted_at` timestamp 
            )

        PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5)) 
        ;

        INSERT INTO `{project}.{data_quality}.etl_lead_time_monitor`( request_id,case_study_id, case_study_name, request_status, request_created_at, request_updated_at, request_lead_time, batch_id, batch_name, batch_status, batch_created_at, batch_updated_at, batch_lead_time, step_id, step_name, step_type, step_status, step_created_at, step_updated_at, step_lead_time, step_detail_id, step_detail_name, step_detail_status, step_detail_created_at, step_detail_updated_at, step_detail_lead_time, item_count, company_id, company_name, source_id, source_name, inserted_at)
        SELECT * 
        FROM `{project}.{data_quality}.etl_lead_time_monitor_backup`;

        DROP TABLE `{project}.{data_quality}.etl_lead_time_monitor_backup`;

    """


def upgrade():
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\nCopy data to table etl_lead_time_monitor_backup and drop etl_lead_time_monitor table successfull!")
    time.sleep(15)

    
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\nCreate and insert data to table etl_lead_time_monitor successfull!")
    time.sleep(15)

query_string3 = f"""
        CREATE OR REPLACE TABLE `{project}.{data_quality}.etl_lead_time_monitor_backup` AS 
        SELECT 
        * 
        FROM `{project}.{data_quality}.etl_lead_time_monitor`;
        DROP TABLE `{project}.{data_quality}.etl_lead_time_monitor`;
        """
query_string4 = f"""
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

        INSERT INTO `{project}.{data_quality}.etl_lead_time_monitor`(request_id, case_study_id, case_study_name, request_status, request_created_at, request_updated_at, request_lead_time, batch_id, batch_name, batch_status, batch_created_at, batch_updated_at, batch_lead_time, step_id, step_name, step_type, step_status, step_created_at, step_updated_at, step_lead_time, step_detail_id, step_detail_name, step_detail_status, step_detail_created_at, step_detail_updated_at, step_detail_lead_time, item_count, company_id, company_name, source_id, source_name, inserted_at)
        SELECT * 
        FROM `{project}.{data_quality}.etl_lead_time_monitor_backup`;

        DROP TABLE `{project}.{data_quality}.etl_lead_time_monitor_backup`;
        """


def downgrade():
    query_job =bqclient.query(query_string3)
    query_job .result()
    print("\nCopy data to table etl_lead_time_monitor_backup and drop etl_lead_time_monitor table successfull!")

    query_job =bqclient.query(query_string4)
    query_job .result()
    print("\nDowngrade 1f445620ada3 process is successfull!")
