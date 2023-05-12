"""create_voe_nlp_output_casestudy

Revision ID: ad5eb77e70de
Revises: cb762b6decec
Create Date: 2021-04-12 09:19:49.517104

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
revision = 'ad5eb77e70de'
down_revision = 'cb762b6decec'
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
		CREATE OR REPLACE TABLE `{project}.{dwh}.voe_nlp_output_case_study`
			(
			`created_at`	timestamp	,
			`review_id`	    string	,
			`source_name`	string	,
			`company_name`	string	,
			`nlp_pack`	    string	,
			`nlp_type`	    string	,
			`user_name`	    string	,
			`language`	    string	,
			`review`	     string	,
			`trans_review`	string	,
			`trans_status`	string	,
			`code`	        string	,
			`dimension`	    string	,
			`label`	        string	,
			`terms`	        string	,
			`relevance`	    float64	,
			`rel_relevance`	float64	,
			`polarity`	    string	,
			`rating`	      float64	,
			`batch_id`	    int64	,
			`batch_name`	    string	,
			`file_name`	    string	,
			`review_date`	    timestamp	,
			`company_id`	    int64	,
			`source_id`	    int64	,
			`step_id`	        int64	,
			`request_id`	    int64	,
			`is_target`	    bool	,
			`case_study_id`	int64	,
			`case_study_name`	string	,
			`dimension_config_id`	int64,
			`run_id` string
			)

PARTITION BY RANGE_BUCKET(case_study_id, GENERATE_ARRAY(1, 4000,1))
;

        """

def upgrade():
    ##########  CREATE voe_nlp_output_case_study TABLE IN DWH#################

    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\nCreating voe_nlp_output_case_study tables in Dwh successfull!")


query_string2 = f"""
        DROP  TABLE `{project}.{dwh}.voe_nlp_output_case_study` ;
        """ 
def downgrade():

    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade cb762b6decec process is successfull!")