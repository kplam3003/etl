"""create table crawl statistics

Revision ID: 600a11854e2d
Revises: 240a133a11b1
Create Date: 2021-08-06 09:49:06.783320

"""
from alembic import op
import sqlalchemy as sa

from google.cloud import bigquery

import config

# revision identifiers, used by Alembic.
revision = '600a11854e2d'
down_revision = '240a133a11b1'
branch_labels = None
depends_on = None


# Get Bigquery Database name:
project = config.GCP_PROJECT_ID
dwh = config.DWH

voc_crawl_statistics_setup_query = f"""
CREATE OR REPLACE table `{project}.{dwh}.voc_crawl_statistics` ( 
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
    `data_version` int64
)

PARTITION BY RANGE_BUCKET(company_datasource_id, GENERATE_ARRAY(1, 4000,5)) ;
"""

voe_crawl_statistics_setup_query = f"""
CREATE OR REPLACE table `{project}.{dwh}.voe_crawl_statistics` ( 
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
    `data_version` int64
)

PARTITION BY RANGE_BUCKET(company_datasource_id, GENERATE_ARRAY(1, 4000,5)) ;
"""

voe_job_crawl_statistics_setup_query = f"""
CREATE OR REPLACE table `{project}.{dwh}.voe_job_crawl_statistics` ( 
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
    `data_version` int64
)

PARTITION BY RANGE_BUCKET(company_datasource_id, GENERATE_ARRAY(1, 4000,5)) ;
"""

voe_company_crawl_statistics_setup_query = f"""
CREATE OR REPLACE table `{project}.{dwh}.voe_company_crawl_statistics` ( 
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
    `data_version` int64
)

PARTITION BY RANGE_BUCKET(company_datasource_id, GENERATE_ARRAY(1, 4000,5)) ;
"""

# tear down
voc_crawl_statistics_teardown = f"""
DROP TABLE `{project}.{dwh}.voc_crawl_statistics`;
""" 

voe_crawl_statistics_teardown = f"""
DROP TABLE `{project}.{dwh}.voe_crawl_statistics`;
"""

voe_job_crawl_statistics_teardown = f"""
DROP TABLE `{project}.{dwh}.voe_job_crawl_statistics`;
"""

voe_company_crawl_statistics_teardown = f"""
DROP TABLE `{project}.{dwh}.voe_company_crawl_statistics`;
"""

def upgrade():
    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
    # voc
    query_job = bqclient.query(voc_crawl_statistics_setup_query)
    query_job .result()
    print(f"Created voc_crawl_statistics table in {dwh} successfully!")
    # voe
    query_job = bqclient.query(voe_crawl_statistics_setup_query)
    query_job .result()
    print(f"Created voe_crawl_statistics table in {dwh} successfully!")
    # voe_job
    query_job = bqclient.query(voe_job_crawl_statistics_setup_query)
    query_job .result()
    print(f"Created voe_job_crawl_statistics table in {dwh} successfully!")
    # voe_company
    query_job = bqclient.query(voe_company_crawl_statistics_setup_query)
    query_job .result()
    print(f"Created voe_company_crawl_statistics table in {dwh} successfully!")


def downgrade():
    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
    # voc
    query_job = bqclient.query(voc_crawl_statistics_teardown)
    query_job .result()
    print(f"Deleted voc_crawl_statistics table in {dwh} successfully!")
    # voe
    query_job = bqclient.query(voe_crawl_statistics_teardown)
    query_job .result()
    print(f"Deleted voe_crawl_statistics table in {dwh} successfully!")
    # voe_job
    query_job = bqclient.query(voe_job_crawl_statistics_teardown)
    query_job .result()
    print(f"Deleted voe_job_crawl_statistics table in {dwh} successfully!")
    # voe_company
    query_job = bqclient.query(voe_company_crawl_statistics_teardown)
    query_job .result()
    print(f"Deleted voe_company_crawl_statistics table in {dwh} successfully!")

