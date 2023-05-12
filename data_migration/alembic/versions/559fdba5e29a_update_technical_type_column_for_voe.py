"""update_technical_type_column_for_voe

Revision ID: 559fdba5e29a
Revises: 2d4c33aac3f0
Create Date: 2021-07-13 03:12:03.149161

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
revision = '559fdba5e29a'
down_revision = '2d4c33aac3f0'
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
    print("\n Update technical_type column name in table voe_parent_review_mapping is successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 2d4c33aac3f0 process is successfull!")    

     
query_string1 = f"""
CREATE OR REPLACE TABLE `{project}.{staging}.voe_parent_review_mapping_bk`
as
SELECT * FROM `{project}.{staging}.voe_parent_review_mapping`
;

DROP TABLE `{project}.{staging}.voe_parent_review_mapping`;

CREATE OR REPLACE TABLE `{project}.{staging}.voe_parent_review_mapping` 
PARTITION BY RANGE_BUCKET( case_study_id , GENERATE_ARRAY(1, 4000,5))
AS
SELECT * except (technique_type), 
CAST(null as string) technical_type
FROM `{project}.{staging}.voe_parent_review_mapping_bk` ;

DROP TABLE `{project}.{staging}.voe_parent_review_mapping_bk`
;

"""

query_string2 = f"""
CREATE OR REPLACE TABLE `{project}.{staging}.voe_parent_review_mapping_bk`
as
SELECT * FROM `{project}.{staging}.voe_parent_review_mapping`
;

DROP TABLE `{project}.{staging}.voe_parent_review_mapping`;

CREATE OR REPLACE TABLE `{project}.{staging}.voe_parent_review_mapping` 
PARTITION BY RANGE_BUCKET( case_study_id , GENERATE_ARRAY(1, 4000,5))
AS 
SELECT * except(technical_type),
CAST(null as string) technique_type
FROM `{project}.{staging}.voe_parent_review_mapping_bk` ;

DROP TABLE `{project}.{staging}.voe_parent_review_mapping_bk`;
"""


