"""add_keywords_is_user_defined_to_voe_CS_dimension_config

Revision ID: f529a27834df
Revises: bac855455e0e
Create Date: 2021-10-11 16:04:40.221421

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
revision = 'f529a27834df'
down_revision = 'bac855455e0e'
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


query_string1 =f"""
ALTER TABLE `{project}.{dwh}.voe_casestudy_dimension_config` 
ADD COLUMN keywords string,
ADD COLUMN is_user_defined  bool;
"""


query_string2 = f"""
CREATE OR REPLACE TABLE `{project}.{dwh}.voe_casestudy_dimension_config`
PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,1)) AS
SELECT * 
EXCEPT(keywords, is_user_defined)
FROM `{project}.{dwh}.voe_casestudy_dimension_config`;
"""	

def upgrade():
    query_job = bqclient.query(query_string1)
    query_job.result()
    print("\nAdd keywords and is_user_defined columns to voe_casestudy_dimension_config in DWH successful!")
    time.sleep(15)


def downgrade():
    query_job = bqclient.query(query_string2)
    query_job.result()
    print("\nDowngrade bac855455e0e process is successful!")
    time.sleep(15)
