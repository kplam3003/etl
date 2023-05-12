"""add_column_parent_review_in_voc_voe

Revision ID: af7d4cb9673e
Revises: c665494eea2b
Create Date: 2021-07-13 03:11:16.509339

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
revision = 'af7d4cb9673e'
down_revision = 'c665494eea2b'
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
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n Create table voc with two new columns in Staging is successfull!")
    time.sleep(15)
    
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Create table voe with two new columns in Staging is successfull!")
    time.sleep(15)

def downgrade():
    query_job =bqclient.query(query_string3)
    query_job .result()
    print("\n Downgrade to old version of table voc in Staging is successfull!")
    
    query_job =bqclient.query(query_string4)
    query_job .result()
    print("\n Downgrade c665494eea2b  process is successfull!")
	
query_string1 = f"""
DROP TABLE `{project}.{staging}.voc`;

CREATE OR REPLACE TABLE `{project}.{staging}.voc` 
PARTITION BY RANGE_BUCKET( case_study_id , GENERATE_ARRAY(1, 4000,5))
AS
SELECT
*,
review_id as parent_review_id,
'all' as technical_type

FROM `{project}.{staging}.voc_bk` ;

"""
query_string2 = f"""

DROP TABLE `{project}.{staging}.voe`;

CREATE OR REPLACE TABLE `{project}.{staging}.voe` 
PARTITION BY RANGE_BUCKET( case_study_id , GENERATE_ARRAY(1, 4000,5))
AS
SELECT
*,
review_id as parent_review_id,
'all' as technical_type

FROM `{project}.{staging}.voe_bk` ;

"""


query_string3 = f"""
DROP TABLE `{project}.{staging}.voc`;

CREATE OR REPLACE TABLE `{project}.{staging}.voc` 
PARTITION BY RANGE_BUCKET( case_study_id , GENERATE_ARRAY(1, 4000,5))
AS 
SELECT * 
FROM `{project}.{staging}.voc_bk` ;

"""

query_string4 = f"""

DROP TABLE `{project}.{staging}.voe`;

CREATE OR REPLACE TABLE `{project}.{staging}.voe` 
PARTITION BY RANGE_BUCKET( case_study_id , GENERATE_ARRAY(1, 4000,5))
AS 
SELECT *
FROM `{project}.{staging}.voe_bk` ;
"""