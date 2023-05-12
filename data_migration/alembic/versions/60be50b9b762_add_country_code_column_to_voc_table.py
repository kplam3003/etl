"""add_country_code_column_to_voc_table

Revision ID: 60be50b9b762
Revises: 10ce6b531731
Create Date: 2021-10-21 13:38:14.380487

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
revision = '60be50b9b762'
down_revision = '10ce6b531731'
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
ALTER TABLE `{project}.{staging}.voc` 
ADD COLUMN `country_code` string;
"""


query_string2 = f"""
CREATE OR REPLACE TABLE `{project}.{staging}.voc`
PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5)) AS
SELECT * 
EXCEPT(country_code)
FROM `{project}.{staging}.voc`;
"""	

def upgrade():
    query_job = bqclient.query(query_string1)
    query_job.result()
    print("\nAdd `country_code` columns to voc in STAGING successful!")
    time.sleep(15)


def downgrade():
    query_job = bqclient.query(query_string2)
    query_job.result()
    print("\nDowngrade 10ce6b531731 process is successful!")
    time.sleep(15)
