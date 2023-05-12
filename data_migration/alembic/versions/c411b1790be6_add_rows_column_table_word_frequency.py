"""add_rows_column_table_word_frequency

Revision ID: c411b1790be6
Revises: babfcb63d851
Create Date: 2021-04-07 03:44:29.469295

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
revision = 'c411b1790be6'
down_revision = 'babfcb63d851'
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


query_string1 =f"""
        ALTER TABLE `{project}.{datamart}.word_frequency` 
            ADD COLUMN `rows` int64;
           
        """
query_string2 = f"""
        CREATE OR REPLACE TABLE `{project}.{datamart}.word_frequency` 
            PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5)) AS
            SELECT * 
            EXCEPT(`rows`)
            FROM `{project}.{datamart}.word_frequency`;
        """	
def upgrade():
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\nAdd rows column to word_frequency in Datamart successful!")
    time.sleep(15)



def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\nDowngrade babfcb63d851 process is successful!")
    time.sleep(15)