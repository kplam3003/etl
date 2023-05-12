"""create table VOC_6_7

Revision ID: 6d19badc1a28
Revises: c9fddf1e32fe
Create Date: 2021-01-27 07:02:04.353188

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
revision = '6d19badc1a28'
down_revision = 'c9fddf1e32fe'
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
    # create table VOC 6_7:
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n Upgrade process is successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade process is successfull!")

query_string1 = f"""
CREATE
OR REPLACE VIEW `{project}.{datamart}.VOC_6_7_commonterms` as WITH tmp AS (
    SELECT
        case_study_id,
        case_study_name,
        dimension_config_name,
        dimension_config_id,
        nlp_type,
        nlp_pack,
        source_name,
        company_name,
        company_id,
        source_id,
        review_id,
        review_date,
        modified_dimension,
        modified_label,
        split(terms, '	') AS single_terms,
        polarity,
        modified_polarity
    FROM
        `{project}.{dwh}.summary_table`
    WHERE
        dimension is not null
        and dimension_config_name is not null
),
single AS (
    SELECT
        *
    EXCEPT
(single_terms),
        single_terms
    FROM
        tmp,
        UNNEST(single_terms) AS single_terms
)
select
    case_study_id,
    max(case_study_name) case_study_name,
    max(dimension_config_name) dimension_config_name,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    max(source_name) source_name,
    max(company_name) company_name,
    company_id,
    source_id,
    modified_dimension as dimension,
    polarity,
    single_terms,
    review_date as daily_date,
    count(distinct review_id) as records,
    count(distinct review_id) as collected_review_count
FROM
    single
GROUP BY
    case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id,
    source_id,
    modified_dimension,
    polarity,
    single_terms,
    daily_date;
"""

query_string2 = f"""

DROP VIEW `{project}.{datamart}.VOC_6_7_commonterms`;

"""

