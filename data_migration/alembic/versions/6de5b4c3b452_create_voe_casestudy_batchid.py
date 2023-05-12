"""create_voe_casestudy_batchid

Revision ID: 6de5b4c3b452
Revises: f0e5c1e66a4f
Create Date: 2021-04-12 07:52:46.303243

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
revision = '6de5b4c3b452'
down_revision = 'f0e5c1e66a4f'
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
        CREATE OR REPLACE table `{project}.{staging}.voe_casestudy_batchid`
        ( 
        `created_at` timestamp,
        `case_study_id` int64,
	    `request_id` int64,
	    `batch_id` int64,
	    `batch_name` string,
        `source_id` int64,
        `company_id` int64,
        `run_id` string
        
        )

        PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,1)) ;
        """

def upgrade():
    ##########  CREATE voe_casestudy_batchid TABLE IN STAGING #################

    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\nCreating voe_casestudy_batchid tables in Staging successfull!")


query_string2 = f"""
        DROP  TABLE `{project}.{staging}.voe_casestudy_batchid` ;
        """ 
def downgrade():

    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade f0e5c1e66a4f process is successfull!")
