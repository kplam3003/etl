"""change_sources_of_voc_1_7

Revision ID: de8292124cb7
Revises: cc0ff2ba6b51
Create Date: 2021-02-01 05:00:45.426376

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
revision = 'de8292124cb7'
down_revision = 'cc0ff2ba6b51'
branch_labels = None
depends_on = None


bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
fs = gcsfs.GCSFileSystem()


# Get GCS bucket
bucket_name = config.BUCKET_NAME

# Get Bigquery Database name:
project = config.GCP_PROJECT_ID
database_list = [config.STAGING, config.DWH, config.DATAMART, config.EXPORT]
staging = config.STAGING
dwh = config.DWH
datamart = config.DATAMART

def upgrade():
    """change_sources_of_voc_1_7"""
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n Upgrade process is successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade process is successfull!")
    
query_string1 = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOC_1_7_cusrevprocessed_v` 
AS
SELECT 

    case_study_id,
    case_study_name,
    max(dimension_config_name) dimension_config_name,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    -- max(source_name)source_name,
    max(company_name) company_name,
    company_id, 
    -- source_id,
    review_date as daily_date,
    count(distinct review_id) as records,
    count(distinct review_id) as collected_review_count,
    count(distinct case when dimension is not null  then review_id else null end) as processed_review_count

FROM `{project}.{datamart}.summary_table`
WHERE dimension_config_name is not null
GROUP BY
    case_study_id,
    case_study_name,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id, 
    -- source_id,
    daily_date;
  """


query_string2 = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOC_1_7_cusrevprocessed_v` 
AS
SELECT 

    case_study_id,
    case_study_name,
    max(dimension_config_name) dimension_config_name,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    -- max(source_name)source_name,
    max(company_name) company_name,
    company_id, 
    -- source_id,
    review_date as daily_date,
    count(distinct review_id) as records,
    count(distinct review_id) as collected_review_count,
    count(distinct case when dimension is not null  then review_id else null end) as processed_review_count

FROM `{project}.{dwh}.summary_table`
WHERE dimension_config_name is not null
GROUP BY
    case_study_id,
    case_study_name,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id, 
    -- source_id,
    daily_date;

"""