"""remove parent review mappings VOE 1_5

Revision ID: 7c48ec6186b3
Revises: c784e8f3616b
Create Date: 2022-06-28 16:28:17.873003

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '7c48ec6186b3'
down_revision = 'c784e8f3616b'
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

VOE_1_5_old = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOE_1_5_emprevlangue_p_v` AS 
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
CASE 
		WHEN language is null AND (language_code is null or language_code = '') THEN 'blank'
		WHEN language is null AND language_code is not null THEN language_code
		ELSE language
	END as language_name,
CASE WHEN language_code is null or language_code = '' THEN 'blank'
	ELSE language_code
END as language_code,

count(distinct parent_review_id) as records,
count(distinct parent_review_id) as collected_review_count

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
language_name,
language_code,
daily_date;
"""

VOE_1_5_new = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOE_1_5_emprevlangue_p_v` AS 
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
    CASE 
            WHEN language is null AND (language_code is null or language_code = '') THEN 'blank'
            WHEN language is null AND language_code is not null THEN language_code
            ELSE language
        END as language_name,
    CASE WHEN language_code is null or language_code = '' THEN 'blank'
        ELSE language_code
    END as language_code,
    count(distinct parent_review_id) as records,
    count(distinct parent_review_id) as collected_review_count
FROM `{project}.{datamart}.voe_summary_table` a
WHERE dimension_config_name is not null
GROUP BY 
    case_study_id,
    case_study_name,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id, 
    language_name,
    language_code,
    daily_date;
"""


def upgrade():
    query_job = bqclient.query(VOE_1_5_new)
    query_job .result()
    print(f"\nUpdate view VOE_1_5_emprevlangue_p_v successful!")
    
def downgrade():
    query_job = bqclient.query(VOE_1_5_old)
    query_job .result()
    print(f"\nDowngrade {revision} process is successful!")
