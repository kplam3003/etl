"""Rename hr_crawl_statistics to hra_crawl_statistics to be more consistent

Revision ID: e957fe9a4d01
Revises: 29b131608403
Create Date: 2022-10-14 14:12:17.058539

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e957fe9a4d01'
down_revision = '29b131608403'
branch_labels = None
depends_on = None


from google.cloud import bigquery

import config

project = config.GCP_PROJECT_ID
dwh = config.DWH
old_table_name = "hr_crawl_statistics"
old_table_id = f"{project}.{dwh}.{old_table_name}"
new_table_name = "hra_crawl_statistics"
new_table_id = f"{project}.{dwh}.{new_table_name}"

def upgrade():
    rename_query = f"""       
        CREATE OR REPLACE table `{new_table_id}` ( 
            `request_id` int64,
            `company_datasource_id` int64,
            `step_detail_id` int64,
            `created_at` timestamp,
            `company_id` int64,
            `company_name` string,
            `source_id` int64,
            `source_name` string,
            `batch_id` int64,
            `num_reviews` int64,
            `data_version` int64,
            `data_type` string
        )
        PARTITION BY RANGE_BUCKET(company_datasource_id, GENERATE_ARRAY(1, 4000,5));
        
        INSERT INTO `{new_table_id}`
        SELECT *
        FROM {old_table_id};
    """
    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
    query_job = bqclient.query(rename_query)
    query_job.result()
    print(f"Renamed `hr_crawl_statistics` table to `hra_crawl_statistics` successfully!")


def downgrade():
    rename_query = f"""
        CREATE OR REPLACE table `{old_table_id}` ( 
            `request_id` int64,
            `company_datasource_id` int64,
            `step_detail_id` int64,
            `created_at` timestamp,
            `company_id` int64,
            `company_name` string,
            `source_id` int64,
            `source_name` string,
            `batch_id` int64,
            `num_reviews` int64,
            `data_version` int64,
            `data_type` string
        )
        PARTITION BY RANGE_BUCKET(company_datasource_id, GENERATE_ARRAY(1, 4000,5));
        
        INSERT INTO `{old_table_id}`
        SELECT *
        FROM {new_table_id};
        
        DROP TABLE `{new_table_id}`;
    """
    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
    query_job = bqclient.query(rename_query)
    query_job.result()
    print(f"Renamed `hra_crawl_statistics` table to `hr_crawl_statistics` successfully!")
