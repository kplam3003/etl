"""create_voe_casestudy_dimension_config_table

Revision ID: 620af2db1335
Revises: 0d1e6f66ccb3
Create Date: 2021-04-12 08:53:19.361112

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
revision = '620af2db1335'
down_revision = '0d1e6f66ccb3'
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
		CREATE OR REPLACE table `{project}.{dwh}.voe_casestudy_dimension_config` 

		(   
			`created_at` timestamp,
			`case_study_id` int64,
			`case_study_name` string,   
			`nlp_pack` string,
			`nlp_type` string,
			`dimension_config_id` int64,
			`dimension_config_name` string,
			`nlp_dimension` string,
			`modified_dimension` string,
			`nlp_label` string,
			`modified_label` string,
			`is_used` bool,
			`dimension_type` string,
			`run_id` string

			)
		PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,1))


	 ;
        """

def upgrade():
    ##########  CREATE voe_casestudy_dimension_config TABLE IN DWH#################

    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\nCreating voe_casestudy_dimension_config tables in Dwh successfull!")


query_string2 = f"""
        DROP  TABLE `{project}.{dwh}.voe_casestudy_dimension_config` ;
        """ 
def downgrade():

    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 0d1e6f66ccb3 process is successfull!")