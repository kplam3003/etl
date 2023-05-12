"""create_voe_casestudy_runid

Revision ID: f0e5c1e66a4f
Revises: 2046036ed2a1
Create Date: 2021-04-12 07:51:50.708581

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
revision = 'f0e5c1e66a4f'
down_revision = '2046036ed2a1'
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
        CREATE OR REPLACE table `{project}.{staging}.voe_case_study_run_id`
        ( 
        `created_at` timestamp,
	    `case_study_id` int64,
        `run_id` string
        )

        PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,1)) ;
        """

def upgrade():
    ##########  CREATE voe_case_study_run_id TABLE IN STAGING #################

    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\nCreating voe_case_study_run_id tables in Staging successfull!")


query_string2 = f"""
        DROP  TABLE `{project}.{staging}.voe_case_study_run_id` ;
        """ 
def downgrade():

    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 2046036ed2a1 process is successfull!")