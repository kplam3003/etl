"""create table VOC_6_3

Revision ID: c18c942de585
Revises: 40a1de07232a
Create Date: 2021-01-27 06:53:05.771867

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
revision = 'c18c942de585'
down_revision = '40a1de07232a'
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
    # create table VOC 6_3:
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n Upgrade process is successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade process is successfull!")

query_string1 = f"""

CREATE
OR REPLACE VIEW `{project}.{datamart}.VOC_6_3_dimpolarity` AS
SELECT
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
    'KPC' as dimension_type,
    modified_dimension as dimension,
    CASE
        WHEN polarity in ('N', 'N+') then 'Negative'
        WHEN polarity in ('P', 'P+') then 'Positive'
        ELSE null
    END polarity_type,
    review_date as daily_date,
    count(distinct review_id) as records,
    count(
        distinct CASE
            WHEN polarity in ('N', 'N+', 'P', 'P+') THEN review_id
            ELSE null
        END
    ) as collected_review_count
FROM
    `{project}.{dwh}.summary_table`
WHERE
    dimension_config_name is not null
GROUP BY
    case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id,
    source_id,
    modified_dimension,
    polarity_type,
    daily_date;

"""

query_string2 = f"""

DROP  VIEW `{project}.{datamart}.VOC_6_3_dimpolarity` ;
"""
