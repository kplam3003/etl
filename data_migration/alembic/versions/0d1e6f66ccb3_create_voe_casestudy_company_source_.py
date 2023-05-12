"""create_voe_casestudy_company_source_table

Revision ID: 0d1e6f66ccb3
Revises: f21d3583ea71
Create Date: 2021-04-12 08:52:39.582329

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
revision = '0d1e6f66ccb3'
down_revision = 'f21d3583ea71'
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
     	CREATE OR REPLACE TABLE `{project}.{dwh}.voe_casestudy_company_source`
	(  
	    `created_at` timestamp,
    	`case_study_id` int64,
      	`case_study_name` string,
    	`dimension_config_id` int64,
	    `company_id` int64,
    	`company_name` string,
    	`source_name`  string,
    	`source_id` int64,
    	`nlp_pack` string,
    	`nlp_type` string,
    	`is_target` bool,
    	`run_at` timestamp,
   	    `run_id` string,
  	    `abs_relevance_inf` float64,
	    `rel_relevance_inf` float64
    	)
	PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,1))
	 ;
        """

def upgrade():
    ##########  CREATE voe_casestudy_company_source TABLE IN DWH #################

    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\nCreating voe_casestudy_company_source tables in Dwh successfull!")


query_string2 = f"""
        DROP  TABLE `{project}.{dwh}.voe_casestudy_company_source` ;
        """ 
def downgrade():

    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade f21d3583ea71 process is successfull!")