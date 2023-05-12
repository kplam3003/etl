"""create_voe_polarity_trans_table

Revision ID: 068ab9194659
Revises: 620af2db1335
Create Date: 2021-04-12 08:54:05.107615

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
revision = '068ab9194659'
down_revision = '620af2db1335'
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
		CREATE OR REPLACE table `{project}.{dwh}.voe_polarity_trans` 
		(   
			`created_at` timestamp,
			`case_study_id` int64,
			`case_study_name` string,   
			`nlp_polarity` string,
			`modified_polarity` float64,
			`run_id` string

			)
		PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,1))

	 ;
        """

def upgrade():
    ##########  CREATE voe_polarity_trans TABLE IN DWH#################

    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\nCreating voe_polarity_trans tables in Dwh successfull!")


query_string2 = f"""
        DROP  TABLE `{project}.{dwh}.voe_polarity_trans` ;
        """ 
def downgrade():

    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 620af2db1335 process is successfull!")