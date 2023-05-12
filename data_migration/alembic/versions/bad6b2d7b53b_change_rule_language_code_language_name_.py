"""change rule language_code = language_name if language_name is null in voc_1_5

Revision ID: bad6b2d7b53b
Revises: 9095df2df9b4
Create Date: 2021-02-18 14:09:00.421694

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
revision = 'bad6b2d7b53b'
down_revision = '9095df2df9b4'
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
    """change rule language_code = language_name if language_name is null in voc_1_5"""
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n Upgrade bad6b2d7b53b process is successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade bad6b2d7b53b process is successfull!")

query_string1 = f"""

CREATE OR REPLACE VIEW `{project}.{datamart}.VOC_1_5_cusrevlangue_p_v` AS
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
    CASE 
            WHEN language is null AND (language_code is null or language_code = '') THEN 'blank'
            WHEN language is null AND language_code is not null THEN language_code
            ELSE language
        END as language_name,
    CASE WHEN language_code is null or language_code = '' THEN 'blank'
        ELSE language_code
    END as language_code,
    
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
    language_name,
    language_code,
    daily_date;

"""

query_string2 = f"""

CREATE OR REPLACE VIEW `{project}.{datamart}.VOC_1_5_cusrevlangue_p_v` AS
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
    CASE WHEN language is null or language_code is null or language_code = '' THEN 'others' ELSE language END as language_name,
    CASE WHEN language = 'others' or language_code is null or language_code = '' THEN 'others' ELSE language_code END as language_code,
    
    
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
    language_name,
    language_code,
    daily_date;

"""
