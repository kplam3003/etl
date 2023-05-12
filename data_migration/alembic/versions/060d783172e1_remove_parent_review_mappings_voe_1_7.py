"""remove parent review mappings VOE 1_7

Revision ID: 060d783172e1
Revises: e774fd321b8c
Create Date: 2022-06-28 16:32:36.221742

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '060d783172e1'
down_revision = 'e774fd321b8c'
branch_labels = None
depends_on = None

from google.cloud import bigquery
import config

bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)

# Get GCS bucket
bucket_name = config.BUCKET_NAME

# Get Bigquery Database name:
project = config.GCP_PROJECT_ID
staging = config.STAGING
dwh = config.DWH
datamart = config.DATAMART
datamart_cs = config.DATAMART_CS

VOE_1_7_old = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOE_1_7_emprevprocessed_v` AS 
WITH BATCH_LIST as (
    SELECT
        batch_id
    FROM
        `{project}.{staging}.voe_batch_status`
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
FROM  `{project}.{staging}.voe_parent_review_mapping`
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

FROM `{project}.{datamart}.voe_summary_table` a
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

VOE_1_7_new = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOE_1_7_emprevprocessed_v` AS 
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
FROM `{project}.{datamart}.voe_summary_table` a
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

def upgrade():
    query_job = bqclient.query(VOE_1_7_new)
    query_job .result()
    print(f"\nUpdate view VOE_1_7_emprevprocessed_v successful!")
    
def downgrade():
    query_job = bqclient.query(VOE_1_7_old)
    query_job .result()
    print(f"\nDowngrade {revision} process is successful!")
