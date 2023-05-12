"""create table VOC_6_2

Revision ID: 40a1de07232a
Revises: 6976a3e2f083
Create Date: 2021-01-27 06:42:42.367829

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
revision = '40a1de07232a'
down_revision = '6976a3e2f083'
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
    # create table VOC 6_2:
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n Upgrade process is successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade process is successfull!")

query_string1 = f"""

CREATE
OR REPLACE VIEW `{project}.{datamart}.VOC_6_2_dimentioned` AS
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
    review_date as daily_date,
    count(distinct review_id) as records,
    count(distinct review_id) as collected_review_count
from
    `{project}.{dwh}.summary_table`
where
    dimension_config_name is not null
    and dimension is not null
group by
    case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id,
    source_id,
    modified_dimension,
    daily_date;

"""


query_string2 = f"""

DROP VIEW `{project}.{datamart}.VOC_6_2_dimentioned`;

"""
