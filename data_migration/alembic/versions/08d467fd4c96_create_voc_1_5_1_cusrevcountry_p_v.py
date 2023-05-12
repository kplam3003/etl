"""create_VOC_1_5_1_cusrevcountry_p_v

Revision ID: 08d467fd4c96
Revises: 7acd80d92ff9
Create Date: 2021-10-25 07:52:00.908297

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
revision = '08d467fd4c96'
down_revision = '7acd80d92ff9'
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
    
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n Create view VOC_1_5_1_cusrevcountry_p_v successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade VOC_1_5_1_cusrevcountry_p_v process is successfull!")
	
query_string1 = f"""
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
            WHEN review_country is null or review_country = 'Unknown'
            THEN 'blank'
            else review_country
        end as country_name,
        case
            when country_code is null or country_code = 'Unknown'
            then 'blank'
            else country_code
        end as country_code
    FROM
        `{project}.{staging}.review_country_mapping`
    WHERE
        batch_id in (
            SELECT
                batch_id
            FROM
                BATCH_LIST
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
    max(ct.country_name) country_name,
    max(ct.country_code) country_code,
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
    daily_date
    """

query_string2 = f"""
DROP VIEW `{project}.{datamart}.VOC_1_5_1_cusrevcountry_p_v`;
"""
