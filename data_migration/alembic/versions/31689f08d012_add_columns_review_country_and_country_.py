"""add_columns_review_country_and_country_code_to_voe

Revision ID: 31689f08d012
Revises: b6ee203050cb
Create Date: 2021-10-21 15:50:16.975151

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
revision = '31689f08d012'
down_revision = 'b6ee203050cb'
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
ALTER TABLE `{project}.{staging}.voe` 
ADD COLUMN `review_country` STRING,
ADD COLUMN `country_code`   STRING ;
"""


query_string2 = f"""
CREATE OR REPLACE TABLE `{project}.{staging}.voe`
PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5)) AS
SELECT * 
EXCEPT(review_country,country_code)
FROM `{project}.{staging}.voe`;
"""	

def upgrade():
    query_job = bqclient.query(query_string1)
    query_job.result()
    print("\nAdd `review_country` and `country_code` columns to voe in STAGING successful!")
    time.sleep(15)


def downgrade():
    query_job = bqclient.query(query_string2)
    query_job.result()
    print("\Removing columns `review_country` and `country_code` from voe is successful!")
    print("\nDowngrade b6ee203050cb process is successful!")
    time.sleep(5)
