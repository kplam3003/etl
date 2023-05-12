"""add_column_dimension_type_voc_5

Revision ID: 4fa5ccb9dd8f
Revises: 35002bc44d80
Create Date: 2021-05-19 06:52:35.696836

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
revision = '4fa5ccb9dd8f'
down_revision = '35002bc44d80'
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

query_string1 = f"""
CREATE OR REPLACE TABLE `{project}.{datamart}.VOC_5_heatmapdim_table_bk`
AS
SELECT * FROM `{project}.{datamart}.VOC_5_heatmapdim_table`
;

DROP TABLE `{project}.{datamart}.VOC_5_heatmapdim_table`;

CREATE OR REPLACE TABLE `{project}.{datamart}.VOC_5_heatmapdim_table` 
PARTITION BY RANGE_BUCKET( case_study_id , GENERATE_ARRAY(1, 4000,5))
AS
SELECT *, 
CAST(null as string) dimension_type
FROM `{project}.{datamart}.VOC_5_heatmapdim_table_bk` ;

DROP TABLE `{project}.{datamart}.VOC_5_heatmapdim_table_bk`
;

"""

query_string2 = f"""
CREATE OR REPLACE TABLE `{project}.{datamart}.VOC_5_heatmapdim_table_bk`
as
SELECT * FROM `{project}.{datamart}.VOC_5_heatmapdim_table`
;

DROP TABLE `{project}.{datamart}.VOC_5_heatmapdim_table`;

CREATE OR REPLACE TABLE `{project}.{datamart}.VOC_5_heatmapdim_table` 
as
SELECT * except(dimension_type)
FROM `{project}.{datamart}.VOC_5_heatmapdim_table_bk` ;

DROP TABLE `{project}.{datamart}.VOC_5_heatmapdim_table_bk`
;
"""


def upgrade():
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\nAdd dimension_type column to voc_5 successful!")
    time.sleep(15)



def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\nDowngrade 35002bc44d80 process is successful!")
    time.sleep(15)

