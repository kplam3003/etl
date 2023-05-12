"""change_sources_of_voc_6_2

Revision ID: 9dc4c6b674d4
Revises: 5890e1444551
Create Date: 2021-02-01 06:43:21.250620

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
revision = '9dc4c6b674d4'
down_revision = '5890e1444551'
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
    """change_sources_of_voc_6_2"""
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n Upgrade 9d0842a01776 process is successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 9d0842a01776 process is successfull!")
    
query_string1 = f"""

CREATE OR REPLACE VIEW `{project}.{datamart}.VOC_6_2_dimentioned` 
AS

SELECT
    case_study_id,
    max(case_study_name) case_study_name,
    max(dimension_config_name) dimension_config_name,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    max(source_name) source_name,
    max(company_name) company_name,
    company_id, 
    source_id,
    'KPC' as dimension_type,
    modified_dimension as dimension,
    review_date as daily_date,
    count(distinct review_id) as records,
    count(distinct review_id) as collected_review_count

FROM `{project}.{datamart}.summary_table`
WHERE
dimension_config_name is not null
AND dimension is not null

GROUP BY 
    case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id, 
    source_id,
    modified_dimension,
    daily_date;

"""

query_string2 = f"""

CREATE OR REPLACE VIEW `{project}.{datamart}.VOC_6_2_dimentioned` 
AS

SELECT
    case_study_id,
    max(case_study_name) case_study_name,
    max(dimension_config_name) dimension_config_name,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    max(source_name) source_name,
    max(company_name) company_name,
    company_id, 
    source_id,
    'KPC' as dimension_type,
    modified_dimension as dimension,
    review_date as daily_date,
    count(distinct review_id) as records,
    count(distinct review_id) as collected_review_count

FROM `{project}.{dwh}.summary_table`
WHERE
dimension_config_name is not null
AND dimension is not null

GROUP BY 
    case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id, 
    source_id,
    modified_dimension,
    daily_date;


"""
