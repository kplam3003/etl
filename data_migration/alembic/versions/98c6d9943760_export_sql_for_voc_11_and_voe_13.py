"""Export SQL for VOC 11 and VOE 13

Revision ID: 98c6d9943760
Revises: 63e722afbe6b
Create Date: 2022-06-27 17:52:07.203246

"""
from alembic import op
import sqlalchemy as sa

from google.cloud import bigquery
import config


# revision identifiers, used by Alembic.
revision = '98c6d9943760'
down_revision = '63e722afbe6b'
branch_labels = None
depends_on = None

project = config.GCP_PROJECT_ID
datamart = config.DATAMART
datamart_cs = config.DATAMART_CS

bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)


def upgrade():
    voc_11_query = f"""
    CREATE OR REPLACE VIEW `{project}.{datamart_cs}.VOC_11_profile_stats` 
    AS 
    SELECT 
        company_name,
        source_name,
        url,
        parent_review_id,
        MAX(rating) AS rating,
        MAX(review_date) AS review_date,
        MAX(total_reviews) AS total_reviews,
        MAX(total_ratings) AS total_ratings,
        MAX(average_rating) AS average_rating
    FROM `{project}.{datamart}.summary_table`
    GROUP BY company_name,
        source_name,
        url,
        parent_review_id
    """
    voc_11_query_job = bqclient.query(voc_11_query)
    voc_11_query_job.result()
    
    voe_13_query = f"""
    CREATE OR REPLACE VIEW `{project}.{datamart_cs}.VOE_13_profile_stats` 
    AS 
    SELECT 
        company_name,
        source_name,
        url,
        parent_review_id,
        MAX(rating) AS rating,
        MAX(review_date) AS review_date,
        MAX(total_reviews) AS total_reviews,
        MAX(total_ratings) AS total_ratings,
        MAX(average_rating) AS average_rating
    FROM `{project}.{datamart}.summary_table`
    GROUP BY company_name,
        source_name,
        url,
        parent_review_id
    """
    voe_13_query_job = bqclient.query(voe_13_query)
    voe_13_query_job.result()

    print("Create VOC_11 and VOE_13 views in Datamart successful!")


def downgrade():
    voc_11_query = f"""
    DROP VIEW `{project}.{datamart_cs}.VOC_11_profile_stats`;
    """
    voc_11_query_job = bqclient.query(voc_11_query)
    voc_11_query_job.result()
    
    voe_13_query = f"""
    DROP VIEW `{project}.{datamart_cs}.VOE_13_profile_stats`;
    """
    voe_13_query_job = bqclient.query(voe_13_query)
    voe_13_query_job.result()
    print("Drop VOC_11 and VOE_13 views in Datamart successful!")
