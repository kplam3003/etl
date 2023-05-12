"""change_sources_of_voc_2

Revision ID: b038922d0ee5
Revises: de8292124cb7
Create Date: 2021-02-01 05:03:06.200093

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
revision = 'b038922d0ee5'
down_revision = 'de8292124cb7'
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
    """change_sources_of_voc_2"""
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n Upgrade process is successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade process is successfull!")
    
query_string1 = f"""

CREATE OR REPLACE VIEW `{project}.{datamart}.VOC_2_polaritydistr_v` 
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
    polarity,
    count(distinct review_id) as records,
    count(distinct review_id) as collected_review_count

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
    daily_date,
    polarity
;

"""


query_string2 = f"""

CREATE OR REPLACE VIEW `{project}.{datamart}.VOC_2_polaritydistr_v` 
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
    polarity,
    count(distinct review_id) as records,
    count(distinct review_id) as collected_review_count

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
    daily_date,
    polarity
;

"""
