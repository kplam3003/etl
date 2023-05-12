"""fix VOC 11 and VOE 13 export view creation

Revision ID: 49a5600dde64
Revises: e529afe937df
Create Date: 2022-07-04 13:53:33.031448

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '49a5600dde64'
down_revision = 'e529afe937df'
branch_labels = None
depends_on = None

from google.cloud import bigquery
import config

project = config.GCP_PROJECT_ID
datamart = config.DATAMART
datamart_cs = config.DATAMART_CS

bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)

def upgrade():
    voc_11_query = f"""
    CREATE OR REPLACE VIEW `{project}.{datamart}.VOC_11_profilestats` 
    AS 
    SELECT 
        case_study_id,
        case_study_name,
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
    GROUP BY
        case_study_id,
        case_study_name, 
        company_name,
        source_name,
        url,
        parent_review_id
    """
    voc_11_query_job = bqclient.query(voc_11_query)
    voc_11_query_job.result()
    
    voe_13_query = f"""
    CREATE OR REPLACE VIEW `{project}.{datamart}.VOE_13_profilestats` 
    AS 
    SELECT 
        case_study_id,
        case_study_name,
        company_name,
        source_name,
        url,
        parent_review_id,
        MAX(rating) AS rating,
        MAX(review_date) AS review_date,
        MAX(total_reviews) AS total_reviews,
        MAX(total_ratings) AS total_ratings,
        MAX(average_rating) AS average_rating
    FROM `{project}.{datamart}.voe_summary_table`
    GROUP BY 
        case_study_id,
        case_study_name,
        company_name,
        source_name,
        url,
        parent_review_id
    """
    voe_13_query_job = bqclient.query(voe_13_query)
    voe_13_query_job.result()

    print("Create VOC_11 and VOE_13 views in Datamart successful!")

    voc_11_drop_query = f"""
    DROP VIEW `{project}.{datamart_cs}.VOC_11_profile_stats`;
    """
    voc_11_drop_query_job = bqclient.query(voc_11_drop_query)
    voc_11_drop_query_job.result()
    
    voe_13_drop_query = f"""
    DROP VIEW `{project}.{datamart_cs}.VOE_13_profile_stats`;
    """
    voe_13_drop_query_job = bqclient.query(voe_13_drop_query)
    voe_13_drop_query_job.result()
    print("Drop incorrect VOC_11 and VOE_13 views in datamart_cs successful!")


def downgrade():
    voc_11_drop_query = f"""
    DROP VIEW `{project}.{datamart}.VOC_11_profilestats`;
    """
    voc_11_drop_query_job = bqclient.query(voc_11_drop_query)
    voc_11_drop_query_job.result()
    
    voe_13_drop_query = f"""
    DROP VIEW `{project}.{datamart}.VOE_13_profilestats`;
    """
    voe_13_drop_query_job = bqclient.query(voe_13_drop_query)
    voe_13_drop_query_job.result()
    print("Drop VOC_11 and VOE_13 views in Datamart successful!")

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

    print("Recreate VOC_11 and VOE_13 views in datamart_cs successful!")

