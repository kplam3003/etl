"""hr_crawl_statistics

Revision ID: f20edf3ba1ad
Revises: e7dab98aa8e1
Create Date: 2022-08-30 02:26:33.089681

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "f20edf3ba1ad"
down_revision = "e7dab98aa8e1"
branch_labels = None
depends_on = None


from google.cloud import bigquery

import config

project = config.GCP_PROJECT_ID
dwh = config.DWH


def upgrade():
    hr_crawl_statistics_setup_query = f"""
    CREATE OR REPLACE table `{project}.{dwh}.hr_crawl_statistics` ( 
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

    PARTITION BY RANGE_BUCKET(company_datasource_id, GENERATE_ARRAY(1, 4000,5)) ;
    """
    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
    # voc
    query_job = bqclient.query(hr_crawl_statistics_setup_query)
    query_job.result()
    print(f"Created hr_crawl_statistics table in {dwh} successfully!")


def downgrade():
    hr_crawl_statistics_teardown_query = f"""
    DROP TABLE `{project}.{dwh}.hr_crawl_statistics`;
    """
    bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
    # voc
    query_job = bqclient.query(hr_crawl_statistics_teardown_query)
    query_job.result()
    print(f"Deleted hr_crawl_statistics table in {dwh} successfully!")
