"""add_column_file_name_to_review_country_mapping

Revision ID: acfdd7ad3b74
Revises: 31689f08d012
Create Date: 2021-10-21 16:48:19.853406

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
revision = 'acfdd7ad3b74'
down_revision = '31689f08d012'
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
ALTER TABLE `{project}.{staging}.review_country_mapping` 
ADD COLUMN `file_name` STRING;
"""


query_string2 = f"""
CREATE OR REPLACE TABLE `{project}.{staging}.review_country_mapping`
PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5)) AS
SELECT * 
EXCEPT(file_name)
FROM `{project}.{staging}.review_country_mapping`;
"""	

def upgrade():
    query_job = bqclient.query(query_string1)
    query_job.result()
    print("\nAdd `file_name` columns to review_country_mapping in STAGING successful!")
    time.sleep(5)


def downgrade():
    query_job = bqclient.query(query_string2)
    query_job.result()
    print("\nDowngrade 31689f08d012 process is successful!")
    time.sleep(5)