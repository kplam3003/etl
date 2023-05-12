"""convert null language to others value in VOC_1_5

Revision ID: 6976a3e2f083
Revises: 54ed970b044d
Create Date: 2021-01-27 06:27:45.676316

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
revision = '6976a3e2f083'
down_revision = '54ed970b044d'
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
    # convert null language to 'others' value in VOC 1_5:
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n Upgrade process is successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade process is successfull!")

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
CASE WHEN language is null or language_code is null or language_code = '' THEN 'others' ELSE language END as language_name,
CASE WHEN language = 'others' or language_code is null or language_code = '' THEN 'others' ELSE language_code END as language_code,
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
language as language_name,
language_code,
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
language_name,
language_code,
daily_date;

"""