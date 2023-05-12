"""create staging etl_error table

Revision ID: fedec5576850
Revises: c327919184b2
Create Date: 2022-04-05 09:23:08.242171

"""
from alembic import op
import sqlalchemy as sa
import time

from google.cloud import bigquery

import config

# revision identifiers, used by Alembic.
revision = 'fedec5576850'
down_revision = 'c327919184b2'
branch_labels = None
depends_on = None

# Get Bigquery Database name:
project = config.GCP_PROJECT_ID
data_quality = config.DATA_QUALITY
staging = config.STAGING

# queries
create_etl_error_query = f"""
CREATE OR REPLACE table `{project}.{staging}.etl_errors` ( 
    `company_datasource_id` int64,
    `batch_id` int64,
    `step_detail_id` int64,
    `company_id` int64,
    `company_name` string,
    `source_id` int64,
    `source_name` string,
    `data_type` string,
    `error_source` string,
    `error_code` string,
    `severity` string,
    `error_message` string,
    `total_step_details` int64,
    `error_time` timestamp,
    `created_time` timestamp,
    `additional_info` string,
    `data_version` int64
)

PARTITION BY RANGE_BUCKET(company_datasource_id, GENERATE_ARRAY(1, 4000,5));
"""

delete_etl_error_query = f"""
DROP TABLE `{project}.{staging}.etl_errors`;
""" 


def upgrade():
    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
    query_job = bqclient.query(create_etl_error_query)
    query_job.result()
    print(f"Creating etl_errors table in {staging} successfully!")
    time.sleep(5)

def downgrade():
    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
    query_job = bqclient.query(delete_etl_error_query)
    query_job.result()
    print(f"Dropped etl_errors table in {staging} successfully!")
