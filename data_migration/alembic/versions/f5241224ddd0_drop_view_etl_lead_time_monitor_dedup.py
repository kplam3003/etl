"""drop_view_etl_lead_time_monitor_dedup

Revision ID: f5241224ddd0
Revises: 54f8b85c5e13
Create Date: 2021-03-25 07:58:19.017482

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
revision = 'f5241224ddd0'
down_revision = '54f8b85c5e13'
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

query_string1 = f"""
        DROP  VIEW  `{project}.{data_quality}.etl_lead_time_monitor_dedup` ;
    
        """


def upgrade():
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\nDrop view etl_lead_time_monitor_dedup in Data Quality successfull!")



query_string2 = f"""
        CREATE OR REPLACE VIEW `{project}.{data_quality}.etl_lead_time_monitor_dedup` 
        AS
        WITH new_table AS (
            SELECT
                *,
                ROW_NUMBER() OVER(
                PARTITION BY
                    request_id,
                    case_study_id,
                    batch_id,
                    step_id,
                    step_detail_id
                ORDER BY inserted_at DESC
                    ) rn
                FROM `{project}.{data_quality}.etl_lead_time_monitor`
            ),
        min_step_detail_id AS (
            SELECT
                request_id,
                case_study_id,
                MIN(step_detail_id) as step_detail_id,
                1 as request_starts
            FROM `{project}.{data_quality}.etl_lead_time_monitor`
            GROUP BY
                request_id,
                case_study_id
            ),
        deduped_table AS (
            SELECT
                *
                EXCEPT(rn)
            FROM new_table
            WHERE rn = 1
            )
            SELECT
            L.*,
            CASE WHEN R.request_starts IS NULL THEN 0
                ELSE 1
            END as request_starts
            FROM deduped_table L
            LEFT JOIN min_step_detail_id R
                ON L.request_id = R.request_id
                AND L.case_study_id = R.case_study_id
                AND L.step_detail_id = R.step_detail_id
                    
    """
def downgrade():
    
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 54f8b85c5e13 process is successfull!")
