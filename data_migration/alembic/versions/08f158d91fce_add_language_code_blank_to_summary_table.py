"""add language_code = 'BLANK' to summary_table

Revision ID: 08f158d91fce
Revises: ab7e714a0357
Create Date: 2021-03-03 09:01:31.308925

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
revision = '08f158d91fce'
down_revision = 'ab7e714a0357'
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
    """add language_code = 'BLANK' to summary_table"""
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n Upgrade 08f158d91fce process is successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 08f158d91fce process is successfull!")
    
    
query_string1 = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.summary_table` AS
WITH run_id_t as (
    SELECT
        *,
        row_number() over (
            partition by case_study_id
            order by
                created_at desc,
                case_study_id desc
        ) as rank
    FROM
        `{project}.{staging}.case_study_run_id`
)

SELECT
    created_at,
    a.case_study_id,
    case_study_name,
    source_id,
    source_name,
    company_id,
    company_name,
    nlp_pack,
    nlp_type,
    user_name,
    dimension_config_id,
    dimension_config_name,
    review_id,
    review,
    trans_review,
    trans_status,
    review_date,
    rating,
    CASE 
        WHEN language_code is null or language_code = '' THEN 'blank'
        ELSE language_code
    END as language_code,
    CASE 
        WHEN language is null AND (language_code is null or language_code = '') THEN 'blank'
        WHEN language is null AND language_code is not null THEN language_code
        ELSE language
    END as language,
    code,
    dimension_type,
    dimension,
    modified_dimension,
    label,
    modified_label,
    is_used,
    terms,
    a.relevance,
    a.rel_relevance,
    polarity,
    modified_polarity,
    batch_id,
    batch_name,
    a.run_id,
    abs_relevance_inf,
    rel_relevance_inf
 

FROM
    `{project}.{dwh}.summary_table_origin` a
LEFT JOIN (

    select distinct case_study_id,run_id, abs_relevance_inf, rel_relevance_inf from `{project}.{dwh}.casestudy_company_source`
    
) b
ON a.case_study_id = b.case_study_id
AND a.run_id = b.run_id
WHERE
    a.run_id in (
        SELECT
            run_id
        FROM
            run_id_t
        WHERE
            rank = 1
    )


AND (CASE WHEN abs_relevance_inf > 0 THEN a.relevance >= abs_relevance_inf ELSE 1=1 END)

AND (CASE WHEN rel_relevance_inf > 0 THEN a.rel_relevance >= rel_relevance_inf ELSE 1=1 END)

;


"""
query_string2 = f"""

CREATE OR REPLACE VIEW `{project}.{datamart}.summary_table` AS
WITH run_id_t as (
    SELECT
        *,
        row_number() over (
            partition by case_study_id
            order by
                created_at desc,
                case_study_id desc
        ) as rank
    FROM
        `{project}.{staging}.case_study_run_id`
)

SELECT
    created_at,
    a.case_study_id,
    case_study_name,
    source_id,
    source_name,
    company_id,
    company_name,
    nlp_pack,
    nlp_type,
    user_name,
    dimension_config_id,
    dimension_config_name,
    review_id,
    review,
    trans_review,
    trans_status,
    review_date,
    rating,
    language_code,
    language,
    code,
    dimension_type,
    dimension,
    modified_dimension,
    label,
    modified_label,
    is_used,
    terms,
    a.relevance,
    a.rel_relevance,
    polarity,
    modified_polarity,
    batch_id,
    batch_name,
    a.run_id,
    abs_relevance_inf,
    rel_relevance_inf
 

FROM
    `{project}.{dwh}.summary_table_origin` a
LEFT JOIN (

    select distinct case_study_id,run_id, abs_relevance_inf, rel_relevance_inf from `{project}.{dwh}.casestudy_company_source`
    
) b
ON a.case_study_id = b.case_study_id
AND a.run_id = b.run_id
WHERE
    a.run_id in (
        SELECT
            run_id
        FROM
            run_id_t
        WHERE
            rank = 1
    )


AND (CASE WHEN abs_relevance_inf > 0 THEN a.relevance >= abs_relevance_inf ELSE 1=1 END)

AND (CASE WHEN rel_relevance_inf > 0 THEN a.rel_relevance >= rel_relevance_inf ELSE 1=1 END)

;

"""
