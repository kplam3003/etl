"""update_polarity_voc_6_4

Revision ID: 9ea287c4ae81
Revises: 8740611d4c8e
Create Date: 2021-08-02 09:06:43.185897

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
revision = '9ea287c4ae81'
down_revision = '8740611d4c8e'
branch_labels = None
depends_on = None

bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
fs = gcsfs.GCSFileSystem()

# Get GCS bucket
bucket_name = config.BUCKET_NAME

# Get Bigquery Database name:
project = config.GCP_PROJECT_ID
database_list = [config.STAGING, config.DWH, config.DATAMART, config.EXPORT, config.DATAMART_CS]
staging = config.STAGING
dwh = config.DWH
datamart = config.DATAMART
datamart_cs = config.DATAMART_CS


def upgrade():
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n Update view VOC_6_4_dimss successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 8740611d4c8e process is successfull!")

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

    sum(CASE WHEN polarity  IN ('N', 'N+', 'NEU', 'P', 'P+') THEN modified_polarity  ELSE null END) as sum_ss,
    count(CASE WHEN polarity  IN ('N', 'N+', 'NEU', 'P', 'P+') THEN review_id  ELSE null END) as sum_review_count

FROM `{project}.{datamart}.summary_table`  a
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
    daily_date
;
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

    sum(CASE WHEN polarity not in ('N','N+','NEU','P','P+') THEN null ELSE modified_polarity END) as sum_ss,
    count(CASE WHEN polarity not in ('N','N+','NEU','P','P+') THEN null ELSE review_id END) as sum_review_count

FROM `{project}.{datamart}.summary_table`  a
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
    daily_date
;
"""
