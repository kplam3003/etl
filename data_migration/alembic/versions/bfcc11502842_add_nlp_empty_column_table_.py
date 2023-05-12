"""add_nlp_empty_column_table_eltbatchreviewmonitor

Revision ID: bfcc11502842
Revises: 39face510f29
Create Date: 2021-03-31 10:48:44.224741

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
revision = 'bfcc11502842'
down_revision = '39face510f29'
branch_labels = None
depends_on = None

bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
fs = gcsfs.GCSFileSystem()


# Get GCS bucket
bucket_name = config.BUCKET_NAME

# Get Bigquery Database name:
project = config.GCP_PROJECT_ID
database_list = [config.DATA_QUALITY]
data_quality=config.DATA_QUALITY 

query_string1 =f"""
        ALTER TABLE `{project}.{data_quality}.etl_batch_review_monitor` 
            ADD COLUMN case_study_name string,
            ADD COLUMN request_status string,
            ADD COLUMN batch_status string,
            ADD COLUMN num_empty_nlp_response int64;
            
        """
query_string2 = f"""
        CREATE OR REPLACE TABLE `{project}.{data_quality}.etl_batch_review_monitor` 
            PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5)) AS
            SELECT * 
            EXCEPT(case_study_name,request_status,batch_status,num_empty_nlp_response)
            FROM `{project}.{data_quality}.etl_batch_review_monitor`;
        """	
def upgrade():
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\nAdd num_empty_nlp column to etl_batch_review_monitor in Data Quality successful!")
    time.sleep(15)



def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\nDowngrade 39face510f29 process is successful!")
    time.sleep(15)