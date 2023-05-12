"""add_columns_to_casestudy_batchid_bq

Revision ID: 13b8210289d3
Revises: ad945597de9c
Create Date: 2021-01-15 09:08:15.821379

"""
from alembic import op
import sqlalchemy as sa

from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage
import pandas as pd
import ast
import time
import config

# revision identifiers, used by Alembic.
revision = '13b8210289d3'
down_revision = 'ad945597de9c'
branch_labels = None
depends_on = None


bqclient = bigquery.Client(project=config.GCP_PROJECT_ID)



# Get GCS bucket
bucket_name = config.BUCKET_NAME

# Get Bigquery Database name:
project = config.GCP_PROJECT_ID
database_list = [config.STAGING, config.DWH, config.DATAMART, config.EXPORT]
staging = config.STAGING
dwh = config.DWH
datamart = config.DATAMART

def upgrade():
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n Add new columns to table in Staging is successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Drop columns in Staging is successfull!")    

     
query_string1 = f"""
CREATE OR REPLACE TABLE `{project}.{staging}.casestudy_batchid_bk`
as
SELECT * FROM `{project}.{staging}.casestudy_batchid`
;

DROP TABLE `{project}.{staging}.casestudy_batchid`;

CREATE OR REPLACE TABLE `{project}.{staging}.casestudy_batchid` 
PARTITION BY RANGE_BUCKET( case_study_id , GENERATE_ARRAY(1, 4000,5))
AS
SELECT *, 
CAST(null as string) batch_name,
CAST(null as int64) company_id,
CAST(null as int64) source_id
FROM `{project}.{staging}.casestudy_batchid_bk` ;

DROP TABLE `{project}.{staging}.casestudy_batchid_bk`
;

"""

query_string2 = f"""
CREATE OR REPLACE TABLE `{project}.{staging}.casestudy_batchid_bk`
as
SELECT * FROM `{project}.{staging}.casestudy_batchid`
;

DROP TABLE `{project}.{staging}.casestudy_batchid`;

CREATE OR REPLACE TABLE `{project}.{staging}.casestudy_batchid` 
as
SELECT * except(batch_name,company_id, source_id)
FROM `{project}.{staging}.casestudy_batchid_bk` ;

DROP TABLE `{project}.{staging}.casestudy_batchid_bk`
;"""

