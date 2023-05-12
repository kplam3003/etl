"""create wildcard voe_summary_table

Revision ID: aa537504eeee
Revises: 3f03a8e215c8
Create Date: 2021-04-14 10:07:07.227485

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
revision = 'aa537504eeee'
down_revision = '3f03a8e215c8'
branch_labels = None
depends_on = None

bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)
fs = gcsfs.GCSFileSystem()

# Get GCS bucket
bucket_name = config.BUCKET_NAME

# Get Bigquery Database name:
project = config.GCP_PROJECT_ID
database_list = [config.STAGING, config.DWH, config.DATAMART, config.EXPORT, config.DATAMART_CS]
staging = config.STAGING
dwh = config.DWH
datamart = config.DATAMART
datamart_cs = config.DATAMART_CS

def upgrade():
    """create first wildcard voe_summary_table in Datamart"""
    query_job =bqclient.query(query_string1)
    query_job.result()
    print(f" Create first wildcard voe_summary_table in Datamart successfully!") 

    """create wildcard voe_summary_table in Datamart"""
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Create wildcard voe_summary_table in Datamart successfull!")

    
    
def downgrade():
    query_job =bqclient.query(query_string3)
    query_job .result()
    print("\n Downgrade 3f03a8e215c8 process is successfull!")
    

query_string1 = f"""
        CREATE OR REPLACE TABLE `{project}.{datamart_cs}.voe_summary_table_tmp` 

        ( 
        created_at	timestamp	,
        case_study_id	int64	,
        case_study_name	string	,
        source_id	int64	,
        source_name	string	,
        company_id	int64	,
        company_name	string	,
        nlp_pack	string	,
        nlp_type	string	,
        user_name	string	,
        dimension_config_id	int64	,
        dimension_config_name	string	,
        review_id	string	,
        review	string	,
        trans_review	string	,
        trans_status	string	,
        review_date	date	,
        rating	float64	,
        language_code	string	,
        language	string	,
        code	string	,
        dimension_type	string	,
        dimension	string	,
        modified_dimension	string	,
        label	string	,
        modified_label	string	,
        is_used	bool	,
        terms	string	,
        relevance	float64	,
        rel_relevance	float64	,
        polarity	string	,
        modified_polarity	float64	,
        batch_id	int64	,
        batch_name	string	,
        run_id	string	,
        abs_relevance_inf	float64	,
        rel_relevance_inf	float64	

        )

        PARTITION BY RANGE_BUCKET(company_id, GENERATE_ARRAY(1, 4000,1))

        """
           
query_string2 = f"""
        CREATE OR REPLACE VIEW `{project}.{datamart}.voe_summary_table` AS
        SELECT * FROM `{project}.{datamart_cs}.voe_summary_table_*` ;
    """

query_string3 = f"""
        DROP VIEW `{project}.{datamart}.voe_summary_table` ;
        DROP TABLE `{project}.{datamart_cs}.voe_summary_table_tmp` ;
    """