"""remove parent review mappings VOE 9_1

Revision ID: e3496f027920
Revises: 060d783172e1
Create Date: 2022-06-28 16:35:33.336870

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e3496f027920'
down_revision = '060d783172e1'
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

VOE_9_1_old = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOE_9_1_reviewsbycompany_p` AS
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
    max(case_study_name) case_study_name,
    max(dimension_config_name) dimension_config_name,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    max(source_name) source_name,
    max(company_name) company_name,
    company_id, 
    source_id,
    review_date as daily_date,
    count(distinct parent_review_id) as records,
    count(distinct parent_review_id) as collected_review_count

FROM `{project}.{datamart}.voe_summary_table` a
LEFT JOIN parent_review p
    ON a.case_study_id = p.case_study_id
    AND a.review_id = p.review_id
WHERE dimension_config_name is not null
GROUP BY
    case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id, 
    source_id,
    daily_date;
	
"""

VOE_9_1_new = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOE_9_1_reviewsbycompany_p` AS
SELECT
    a.case_study_id,
    max(case_study_name) case_study_name,
    max(dimension_config_name) dimension_config_name,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    max(source_name) source_name,
    max(company_name) company_name,
    company_id, 
    source_id,
    review_date as daily_date,
    count(distinct parent_review_id) as records,
    count(distinct parent_review_id) as collected_review_count
FROM `{project}.{datamart}.voe_summary_table` a
WHERE dimension_config_name is not null
GROUP BY
    case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id, 
    source_id,
    daily_date;
	
"""

def upgrade():
    query_job = bqclient.query(VOE_9_1_new)
    query_job .result()
    print(f"\nUpdate view VOE_9_1_reviewsbycompany_p successful!")
    
def downgrade():
    query_job = bqclient.query(VOE_9_1_old)
    query_job .result()
    print(f"\nDowngrade {revision} process is successful!")
