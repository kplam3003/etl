"""add_columns_source_name_voe_company

Revision ID: d21c25a9d298
Revises: 16d34ef640ee
Create Date: 2021-05-14 04:08:42.071280

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
revision = 'd21c25a9d298'
down_revision = '16d34ef640ee'
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
CREATE OR REPLACE TABLE `{project}.{staging}.voe_company_bk`
as
SELECT * FROM `{project}.{staging}.voe_company`
;

DROP TABLE `{project}.{staging}.voe_company`;

CREATE OR REPLACE TABLE `{project}.{staging}.voe_company` 
PARTITION BY RANGE_BUCKET( case_study_id , GENERATE_ARRAY(1, 4000,5))
AS
SELECT *, 
CAST(null as string) source_name,
CAST(null as int64) source_id
FROM `{project}.{staging}.voe_company_bk` ;

DROP TABLE `{project}.{staging}.voe_company_bk`
;

"""

query_string2 = f"""
CREATE OR REPLACE TABLE `{project}.{staging}.voe_company_bk`
as
SELECT * FROM `{project}.{staging}.voe_company`
;

DROP TABLE `{project}.{staging}.voe_company`;

CREATE OR REPLACE TABLE `{project}.{staging}.voe_company` 
as
SELECT * except(source_name, source_id)
FROM `{project}.{staging}.voe_company_bk` ;

DROP TABLE `{project}.{staging}.voe_company_bk`
;
"""


def upgrade():
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\nAdd source_name, source_id column to voe_company in Staging successful!")
    time.sleep(15)



def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\nDowngrade 16d34ef640ee process is successful!")
    time.sleep(15)
