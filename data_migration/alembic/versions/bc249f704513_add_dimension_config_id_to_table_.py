"""add dimension_config_id to table dimension_config_statistic

Revision ID: bc249f704513
Revises: 72e4ee1d66c5
Create Date: 2021-01-28 10:35:44.491205

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
revision = 'bc249f704513'
down_revision = '72e4ee1d66c5'
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
    """add dimension config id to table dimension config statistic"""
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n Upgrade process is successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade process is successfull!")


query_string1 = f"""

CREATE
OR REPLACE VIEW `{project}.{datamart}.dimension_config_statistic` AS
SELECT
    case_study_id,
    case_study_name,
    dimension_config_id,
    dimension_config_name,
    nlp_type,
    nlp_pack,
    dimension,
    modified_dimension,
    label,
    modified_label,
    count(
        distinct CASE
            WHEN dimension is null THEN null
            ELSE review_id
        END
    ) as customer_review_processed,
    is_used as used_for_analysis,
    dimension_type
FROM
    `{project}.{dwh}.summary_table`
GROUP BY
    case_study_id,
    case_study_name,
    dimension_config_id,
    dimension_config_name,
    nlp_type,
    nlp_pack,
    dimension,
    modified_dimension,
    label,
    modified_label,
    is_used,
    dimension_type;

"""

query_string2 = f"""

CREATE
OR REPLACE VIEW `{project}.{datamart}.dimension_config_statistic` AS
SELECT
    case_study_id,
    case_study_name,    
    dimension_config_name,
    nlp_type,
    nlp_pack,
    dimension,
    modified_dimension,
    label,
    modified_label,
    count(
        distinct CASE
            WHEN dimension is null THEN null
            ELSE review_id
        END
    ) as customer_review_processed,
    is_used as used_for_analysis,
    dimension_type
FROM
    `{project}.{dwh}.summary_table`
GROUP BY
    case_study_id,
    case_study_name,    
    dimension_config_name,
    nlp_type,
    nlp_pack,
    dimension,
    modified_dimension,
    label,
    modified_label,
    is_used,
    dimension_type;

"""