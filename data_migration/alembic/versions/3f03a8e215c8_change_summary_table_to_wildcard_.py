"""change summary_table to wildcard summary_table_*

Revision ID: 3f03a8e215c8
Revises: 12ba5fe61b43
Create Date: 2021-04-13 11:28:34.536982

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
revision = '3f03a8e215c8'
down_revision = '12ba5fe61b43'
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
    """change summary_table to wildcard summary_table_*"""
    query_job =bqclient.query(query_string1)
    query_job .result()
  
    print("\n Upgrade 3f03a8e215c8 process is successfull!")

    
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n Downgrade 3f03a8e215c8 process is successfull!")
    
    
query_string1 = f"""
DROP TABLE `{project}.{datamart}.summary_table` ;

CREATE OR REPLACE VIEW `{project}.{datamart}.summary_table` AS
SELECT * FROM `{project}.{datamart_cs}.summary_table_*` ;

"""

query_string2 = f"""
DROP VIEW `{project}.{datamart}.summary_table` ;

CREATE OR REPLACE TABLE `{project}.{datamart}.summary_table` AS
SELECT * FROM `{project}.{datamart_cs}.summary_table_*` ;

"""
