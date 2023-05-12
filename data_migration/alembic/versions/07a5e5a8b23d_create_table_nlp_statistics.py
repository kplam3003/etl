"""create table nlp statistics

Revision ID: 07a5e5a8b23d
Revises: 600a11854e2d
Create Date: 2021-08-06 11:24:09.577658

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '07a5e5a8b23d'
down_revision = '600a11854e2d'
branch_labels = None
depends_on = None

from google.cloud import bigquery

import config

# Get Bigquery Database name:
project = config.GCP_PROJECT_ID
dwh = config.DWH

voc_nlp_statistics_setup_query = f"""
CREATE OR REPLACE table `{project}.{dwh}.voc_nlp_statistics` ( 
    `request_id` int64,
    `company_datasource_id` int64,
    `processed_at` timestamp,
    `company_id` int64,
    `company_name` string,
    `source_id` int64,
    `source_name` string,
    `num_processed_reviews` int64,
    `nlp_type` string,
    `nlp_pack` string,
    `data_version` int64
)

PARTITION BY RANGE_BUCKET(company_datasource_id, GENERATE_ARRAY(1, 4000,5)) ;
"""

voe_nlp_statistics_setup_query = f"""
CREATE OR REPLACE table `{project}.{dwh}.voe_nlp_statistics` ( 
    `request_id` int64,
    `company_datasource_id` int64,
    `processed_at` timestamp,
    `company_id` int64,
    `company_name` string,
    `source_id` int64,
    `source_name` string,
    `num_processed_reviews` int64,
    `nlp_type` string,
    `nlp_pack` string,
    `data_version` int64
)

PARTITION BY RANGE_BUCKET(company_datasource_id, GENERATE_ARRAY(1, 4000,5)) ;
"""


# tear down
voc_crawl_statistics_teardown = f"""
DROP TABLE `{project}.{dwh}.voc_nlp_statistics`;
""" 

voe_nlp_statistics_teardown = f"""
DROP TABLE `{project}.{dwh}.voe_nlp_statistics`;
"""


def upgrade():
    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
    # voc
    query_job = bqclient.query(voc_nlp_statistics_setup_query)
    query_job .result()
    print(f"Created voc_nlp_statistics table in {dwh} successfully!")
    # voe
    query_job = bqclient.query(voe_nlp_statistics_setup_query)
    query_job .result()
    print(f"Created voe_nlp_statistics table in {dwh} successfully!")


def downgrade():
    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
    # voc
    query_job = bqclient.query(voc_crawl_statistics_teardown)
    query_job .result()
    print(f"Deleted voc_crawl_statistics table in {dwh} successfully!")
    # voe
    query_job = bqclient.query(voe_nlp_statistics_teardown)
    query_job .result()
    print(f"Deleted voe_crawl_statistics table in {dwh} successfully!")
