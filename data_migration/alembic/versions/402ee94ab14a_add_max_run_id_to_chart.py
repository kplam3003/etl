"""add max(run_id) to chart

Revision ID: 402ee94ab14a
Revises: bdcd29b62ccf
Create Date: 2021-01-29 04:25:57.443390

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
revision = '402ee94ab14a'
down_revision = '9f97c805b038'
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
    """create summary_table_origin and add view summary_table"""
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n Upgrade process is successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade process is successfull!")
    
query_string1 = f"""

# Create summary_table_origin:

CREATE OR REPLACE TABLE `{project}.{dwh}.summary_table_origin`
PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5))
AS
SELECT * FROM `{project}.{dwh}.summary_table`;

# Create view summary_table:

CREATE
OR REPLACE VIEW `{project}.{datamart}.summary_table` AS WITH run_id_t as (
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
    )
    
    AND is_used = TRUE    
    ;


"""

query_string2 = f"""

DROP VIEW `{project}.{datamart}.summary_table`;

DROP TABLE `{project}.{dwh}.summary_table_origin`;

"""
