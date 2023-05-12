"""add filter relevance to table summary_table in datamart

Revision ID: b5ffcc35fea7
Revises: 5612356a5eeb
Create Date: 2021-02-18 07:10:39.132180

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
revision = 'b5ffcc35fea7'
down_revision = '5612356a5eeb'
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
    """add filter relevance to table summary_table in datamart"""
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n Upgrade b5ffcc35fea7 process is successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade b5ffcc35fea7 process is successfull!")

query_string1 = f"""

CREATE OR REPLACE VIEW `{project}.{datamart}.summary_table`
AS
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

AND a.relevance >= abs_relevance_inf

AND a.rel_relevance >= rel_relevance_inf
;

"""
query_string2 = f"""

CREATE OR REPLACE VIEW `{project}.{datamart}.summary_table`
AS

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
    case_study_id,
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
    relevance,
    rel_relevance,
    polarity,
    modified_polarity,
    batch_id,
    batch_name,
    run_id
FROM
    `{project}.{dwh}.summary_table_origin`
WHERE
    run_id in (
        SELECT
            run_id
        FROM
            run_id_t
        WHERE
            rank = 1
    );

"""