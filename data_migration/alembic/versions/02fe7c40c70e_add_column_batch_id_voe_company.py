"""add_column_batch_id_voe_company

Revision ID: 9b7adddb6f54
Revises: 02fe7c40c70e
Create Date: 2021-04-13 07:16:47.211107

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
revision = '02fe7c40c70e'
down_revision = 'be144eb1d8fa'
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
        ALTER TABLE `{project}.{staging}.voe_company` 
            ADD COLUMN batch_id int64;
            
        """
query_string2 = f"""
        CREATE OR REPLACE TABLE `{project}.{staging}.voe_company` 
            PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5)) AS
            SELECT * 
            EXCEPT(batch_id)
            FROM `{project}.{staging}.voe_company`;
        """	
def upgrade():
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\nAdd batch_id column to voe_company in Staging successful!")
    time.sleep(15)



def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\nDowngrade 02fe7c40c70e process is successful!")
    time.sleep(15)