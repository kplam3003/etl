"""create_table_voe

Revision ID: ff478d16da7a
Revises: 068d27ef9640
Create Date: 2021-03-31 10:45:30.720471

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
revision = 'ff478d16da7a'
down_revision = '068d27ef9640'
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

query_string1 = f"""
        CREATE OR REPLACE table `{project}.{staging}.voe`
        ( 
	    `id` int64,
        `created_at` timestamp,
        `review_id` string,
        `source_name` string,
        `company_name`  string,
        `nlp_pack` string,
        `nlp_type` string,
        `user_name` string,    
        `language` string,    
        `review` string,
        `trans_review` string,
        `trans_status` string,    
        `code`  string,
        `dimension`  string,
        `label`  string,
        `terms`  string,
        `relevance`  float64,
        `rel_relevance`  float64,   
        `polarity` string,
        `rating` float64,
        `batch_id` int64,
        `batch_name` string,
        `file_name` string,
        `review_date` timestamp,
        `company_id` int64,
        `source_id` int64,
        `step_id` int64,
        `request_id` int64,
        `case_study_id` int64
        )

        PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5)) ;
        """

def upgrade():
    ##########  CREATE VOE TABLE IN STAGING #################

    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\nCreating voe tables in Staging successfull!")


query_string2 = f"""
        DROP  TABLE `{project}.{staging}.voe` ;
        """ 
def downgrade():

    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade ff478d16da7a process is successfull!")