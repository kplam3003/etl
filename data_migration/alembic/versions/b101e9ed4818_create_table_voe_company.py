"""create_table_voe_company

Revision ID: b101e9ed4818
Revises: ff478d16da7a
Create Date: 2021-03-31 10:46:47.973240

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
revision = 'b101e9ed4818'
down_revision = 'ff478d16da7a'
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
        CREATE OR REPLACE table `{project}.{staging}.voe_company`
        ( 
        `created_at` timestamp,
        `case_study_id` int64,
        `company_name`  string,
        `company_id` int64,
        `min_fte` int64,
        `max_fte` int64
        )

        PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,5)) ;
        """

def upgrade():
    ##########  CREATE VOE_COMPANY TABLE IN STAGING #################

    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\nCreating voe_company tables in Staging successfull!")


query_string2 = f"""
        DROP  TABLE `{project}.{staging}.voe_company` ;
        """ 
def downgrade():

    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade ff478d16da7a process is successfull!")