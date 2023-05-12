"""create_voe_dimension_keyword_list_table

Revision ID: cb762b6decec
Revises: 27479fef878c
Create Date: 2021-04-12 08:55:05.720983

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
revision = 'cb762b6decec'
down_revision = '27479fef878c'
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
		CREATE OR REPLACE table `{project}.{dwh}.voe_dimension_keyword_list` 

		(   
			`created_at` timestamp,
			`case_study_id` int64,
			`in_list` string,   
			`ex_list` string,
			`run_id` string

			)
		PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,1))


	 ;
        """

def upgrade():
    ##########  CREATE voe_dimension_keyword_list TABLE IN DWH#################

    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\nCreating voe_dimension_keyword_list tables in Dwh successfull!")


query_string2 = f"""
        DROP  TABLE `{project}.{dwh}.voe_dimension_keyword_list` ;
        """ 
def downgrade():

    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 27479fef878c process is successfull!")
