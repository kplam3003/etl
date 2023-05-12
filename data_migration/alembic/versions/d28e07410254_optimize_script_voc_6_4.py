"""optimize script voc_6_4

Revision ID: d28e07410254
Revises: d15a0a99ed2e
Create Date: 2021-02-03 07:48:20.247636

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
revision = 'd28e07410254'
down_revision = 'd15a0a99ed2e'
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
    """optimize script voc_6_4"""
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n Upgrade d28e07410254 process is successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade d28e07410254 process is successfull!")
    
query_string1 = f"""

CREATE OR REPLACE VIEW `{project}.{datamart}.VOC_6_4_dimss` AS

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
    dimension_type,
    modified_dimension as dimension,
    review_date as daily_date,

    sum(modified_polarity) as sum_ss,
    count(review_id) as sum_review_count

FROM `{project}.{datamart}.summary_table` a
WHERE
dimension_config_name is not null
AND dimension is not null
AND is_used = true
GROUP BY
    case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id, 
    source_id,
    dimension_type,
    modified_dimension,
    daily_date;

"""
query_string2 = f"""

CREATE OR REPLACE VIEW `{project}.{datamart}.VOC_6_4_dimss` AS

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
    dimension_type,
    modified_dimension as dimension,
    review_date as daily_date,

    sum(modified_polarity) as sum_ss,
    count(review_id) as sum_review_count,

     (select count(distinct review_id)  from `{project}.{datamart}.summary_table` 
    where case_study_id = a.case_study_id
    and modified_dimension = a.modified_dimension
    ) as collected_review_count

FROM `{project}.{datamart}.summary_table` a
WHERE
dimension_config_name is not null
AND dimension is not null
AND is_used = true
GROUP BY
    case_study_id,
    dimension_config_id,
    nlp_type,
    nlp_pack,
    company_id, 
    source_id,
    dimension_type,
    modified_dimension,
    daily_date;


"""
