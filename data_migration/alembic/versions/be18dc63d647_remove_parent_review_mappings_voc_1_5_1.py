"""remove parent review mappings VOC 1.5.1

Revision ID: be18dc63d647
Revises: 4183699d73da
Create Date: 2022-06-28 14:49:29.217122

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'be18dc63d647'
down_revision = '4183699d73da'
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

VOC_1_5_1_old = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOC_1_5_1_cusrevcountry_p_v` AS
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
        DISTINCT case_study_id,
        review_id,
        parent_review_id,
        technical_type
    FROM
        `{project}.{staging}.parent_review_mapping`
    WHERE
        batch_id IN (
            SELECT
                batch_id
            FROM
                BATCH_LIST
        )
),
country_code as (
    select
        distinct case_study_id,
        review_id,
        case
            WHEN review_country is null
            or review_country = 'Unknown'
            THEN 'blank'
            else review_country
        end as country_name,
        case
            when country_code is null
            or country_code = 'Unknown' then 'blank'
            else country_code
        end as country_code
    from
        `{project}.{staging}.review_country_mapping`
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
    ct.country_name,
    ct.country_code,
    count(distinct parent_review_id) as records,
    count(distinct parent_review_id) as collected_review_count
FROM
    `{project}.{datamart}.summary_table` a
    LEFT JOIN parent_review p ON a.case_study_id = p.case_study_id
    AND a.review_id = p.review_id
    LEFT JOIN country_code ct ON a.case_study_id = ct.case_study_id
    AND a.review_id = ct.review_id
WHERE
    dimension_config_name is not null
GROUP BY
    case_study_id,
    case_study_name,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id,
    daily_date,
    country_name,
    country_code;
"""

VOC_1_5_1_new = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOC_1_5_1_cusrevcountry_p_v` AS
WITH BATCH_LIST as (
    SELECT
        batch_id
    FROM
        `{project}.{staging}.batch_status`
    WHERE
        status = 'Active'
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
    case
        WHEN review_country is null
        or review_country = 'Unknown'
        THEN 'blank'
        else review_country
    end as country_name,
    case
        when country_code is null
        or country_code = 'Unknown' then 'blank'
        else country_code
    end as country_code,
    count(distinct parent_review_id) as records,
    count(distinct parent_review_id) as collected_review_count
FROM
    `{project}.{datamart}.summary_table` a
WHERE
    dimension_config_name is not null
GROUP BY
    case_study_id,
    case_study_name,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id,
    daily_date,
    country_name,
    country_code;
"""

def upgrade():
    query_job = bqclient.query(VOC_1_5_1_new)
    query_job.result()
    print(f"\nUpdate view VOC_1_5_1_cusrevcountry_p_v successful!")
    
def downgrade():
    query_job = bqclient.query(VOC_1_5_1_old)
    query_job.result()
    print(f"\nDowngrade {revision} process is successful!")
