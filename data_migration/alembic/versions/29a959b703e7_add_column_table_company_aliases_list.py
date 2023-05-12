"""add_column_table_company_aliases_list

Revision ID: 29a959b703e7
Revises: 2858c236e4fa
Create Date: 2021-06-16 07:59:01.929524

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
revision = '29a959b703e7'
down_revision = '5f70f57c4a70'
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
ALTER TABLE `{project}.{dwh}.company_aliases_list` 
ADD COLUMN original_aliases  string;

"""
query_string2 = f"""
CREATE OR REPLACE TABLE `{project}.{dwh}.company_aliases_list`
PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5)) AS
SELECT * 
EXCEPT(original_aliases)
FROM `{project}.{dwh}.company_aliases_list` ;
        """	
def upgrade():
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\nAdd original_aliases column to company_aliases_list in DWH successful!")
    time.sleep(15)



def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\nDowngrade 5f70f57c4a70 process is successful!")
    time.sleep(15)
