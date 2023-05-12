"""create_etl_leadtime_monitor_dedup_mv

Revision ID: 1d0ef651ad6f
Revises: 02fe7c40c70e
Create Date: 2021-04-13 11:06:45.530915

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
revision = '1d0ef651ad6f'
down_revision = '02fe7c40c70e'
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
        CREATE MATERIALIZED VIEW `{project}.{data_quality}.etl_lead_time_monitor_dedup_mv` AS
		SELECT
			case_study_id,
			MAX(case_study_name) as case_study_name,
			request_id,
			MIN(request_status) as request_status,
			MAX(request_created_at) as request_created_at,
			MAX(request_updated_at) as request_updated_at,
			batch_id,
			MAX(batch_name) as batch_name,
			MIN(batch_status) as batch_status,
			MAX(batch_created_at) as batch_created_at,
			MAX(batch_updated_at) as batch_updated_at,
			MAX(source_id) as source_id,
			MAX(source_name) as source_name,
			MAX(company_id) as company_id,
			MAX(company_name) as company_name,
			step_id,
			MAX(step_name) as step_name,
			step_type,
			MIN(step_status) as step_status,
			MAX(step_created_at) as step_created_at,
			MAX(step_updated_at) as step_updated_at,
			step_detail_id,
			MAX(step_detail_name) as step_detail_name,
			MIN(step_detail_status) as step_detail_status,
			MAX(step_detail_created_at) as step_detail_created_at,
			MAX(step_detail_updated_at) as step_detail_updated_at,
			MAX(item_count) as item_count,
			MAX(request_lead_time) as request_lead_time,
			MAX(batch_lead_time) as batch_lead_time,
			MAX(step_lead_time) as step_lead_time,
			MAX(step_detail_lead_time) as step_detail_lead_time,
			MAX(inserted_at) as inserted_at
		FROM `{project}.{data_quality}.etl_lead_time_monitor`
		WHERE step_detail_id IS NOT NULL
		GROUP BY
			case_study_id,
			request_id,
			batch_id,
			step_id,
			step_type,
			step_detail_id;
    
        """


def upgrade():
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n Create view etl_lead_time_monitor_dedup_mv in Data Quality successfull!")



query_string2 = f"""
        DROP MATERIALIZED VIEW  `{project}.{data_quality}.etl_lead_time_monitor_dedup_mv`;
                    
    """
def downgrade():
    
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 02fe7c40c70e  process is successfull!")