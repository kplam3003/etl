"""update_view_voc_1_7

Revision ID: 6579a970306d
Revises: a081d8d7c4ad
Create Date: 2021-07-15 12:24:14.654485

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
revision = '6579a970306d'
down_revision = 'a081d8d7c4ad'
branch_labels = None
depends_on = None

bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
fs = gcsfs.GCSFileSystem()

# Get GCS bucket
bucket_name = config.BUCKET_NAME

# Get Bigquery Database name:
project = config.GCP_PROJECT_ID
database_list = [config.STAGING, config.DWH, config.DATAMART, config.EXPORT, config.DATAMART_CS]
staging = config.STAGING
dwh = config.DWH
datamart = config.DATAMART
datamart_cs = config.DATAMART_CS

def upgrade():
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n Update view VOC_1_7_cusrevprocessed_v successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade a081d8d7c4ad process is successfull!")


query_string1 = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOC_1_7_cusrevprocessed_v` AS 
WITH BATCH_LIST as (
    SELECT
        batch_id
    FROM
        `{project}.{staging}.batch_status`
    WHERE
        status = 'Active'
        
),
parent_review as (
SELECT 
DISTINCT 
case_study_id,
review_id,
parent_review_id,
technical_type
FROM  `{project}.{staging}.parent_review_mapping`
WHERE
        batch_id IN (
            SELECT
                batch_id FROM BATCH_LIST
        )
        
)
SELECT 
a.case_study_id,
case_study_name,
max(dimension_config_name) dimension_config_name,
dimension_config_id,
nlp_type,
nlp_pack,
max(company_name) company_name,
company_id, 
review_date as daily_date,
count(distinct parent_review_id) as records,
count(distinct parent_review_id) as collected_review_count,
count(distinct case when dimension is not null  then parent_review_id else null end) as processed_review_count

FROM `{project}.{datamart}.summary_table` a
LEFT JOIN parent_review p
    ON a.case_study_id = p.case_study_id
    AND a.review_id = p.review_id
WHERE dimension_config_name is not null
GROUP BY
case_study_id,
case_study_name,
dimension_config_id,
nlp_type,
nlp_pack,
company_id, 
daily_date;
"""
	
query_string2 = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOC_1_7_cusrevprocessed_v` AS 
SELECT 
case_study_id,
case_study_name,
max(dimension_config_name) dimension_config_name,
dimension_config_id,
nlp_type,
nlp_pack,
max(company_name) company_name,
company_id, 
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
daily_date;
"""
