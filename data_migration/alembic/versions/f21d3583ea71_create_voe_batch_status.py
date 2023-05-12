"""create_voe_batch_status

Revision ID: f21d3583ea71
Revises: 6de5b4c3b452
Create Date: 2021-04-12 07:53:24.878706

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
revision = 'f21d3583ea71'
down_revision = '6de5b4c3b452'
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
        CREATE OR REPLACE table `{project}.{staging}.voe_batch_status`
        ( 
        `created_at` timestamp,
	`batch_id` int64,
	`batch_name` string,
        `source_id` int64,
	`source_name` string,
        `company_id` int64,
	`company_name`  string,
        `status` string,
        `version` int64
        )

        PARTITION BY RANGE_BUCKET(batch_id, GENERATE_ARRAY(1, 4000,10)) ;
        """

def upgrade():
    ##########  CREATE voe_batch_status TABLE IN STAGING #################

    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\nCreating voe_batch_status tables in Staging successfull!")


query_string2 = f"""
        DROP  TABLE `{project}.{staging}.voe_batch_status` ;
        """ 
def downgrade():

    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 6de5b4c3b452 process is successfull!")