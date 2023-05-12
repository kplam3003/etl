"""create_voe_summary_table_origin_table

Revision ID: 27479fef878c
Revises: 068ab9194659
Create Date: 2021-04-12 08:54:38.382665

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
revision = '27479fef878c'
down_revision = '068ab9194659'
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
			CREATE OR REPLACE TABLE `{project}.{dwh}.voe_summary_table_origin`

			(
			`created_at`	timestamp	,
			`case_study_id`	int64	,
			`case_study_name`  string	,
			`source_id`	    int64	,
			`source_name`	string	,
			`company_id`	int64	,
			`company_name`	string	,
			`nlp_pack`	    string	,
			`nlp_type`	    string	,
			`user_name`	    string	,
			`dimension_config_id`	int64	,
			`dimension_config_name`  string	,
			`review_id`	  string	,
			`review`	string	,
			`trans_review`	  string	,
			`trans_status`	 string	,
			`review_date`	 date	,
			`rating`	 float64	,
			`language_code`	  string	,
			`language`	 string	,
			`code`	  string	,
			`dimension_type`	string	,
			`dimension`	   string	,
			`modified_dimension`  string	,
			`label`	   string	,
			`modified_label`	string	,
			`is_used`	 bool	,
			`terms`	   string	,
			`relevance`	  float64	,
			`rel_relevance`	  float64	,
			`polarity`	 string	,
			`modified_polarity`	   float64	,
			`batch_id`	 int64,
			`batch_name`	string,
			`run_id` string
			)

			PARTITION BY RANGE_BUCKET( case_study_id , GENERATE_ARRAY(1, 4000,1));
        """

def upgrade():
    ##########  CREATE voe_summary_table_origin TABLE IN DWH#################

    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\nCreating voe_summary_table_origin tables in Dwh successfull!")


query_string2 = f"""
        DROP  TABLE `{project}.{dwh}.voe_summary_table_origin` ;
        """ 
def downgrade():

    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 068ab9194659 process is successfull!")