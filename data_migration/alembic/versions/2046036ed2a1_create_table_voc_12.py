"""create table voc_12

Revision ID: 2046036ed2a1
Revises: c411b1790be6
Create Date: 2021-04-08 10:46:27.868798

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
revision = '2046036ed2a1'
down_revision = 'c411b1790be6'
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

def upgrade():
    """create table voc_12"""
    query_job =bqclient.query(query_string1)
    query_job .result()
    print("\n 2046036ed2a1 Upgrade process is successfull!")
    
def downgrade():
    query_job =bqclient.query(query_string2)
    query_job .result()
    print("\n 2046036ed2a1 Downgrade process is successfull!")

query_string1 = f"""
CREATE OR REPLACE VIEW `{project}.{datamart}.VOC_12_sscausalmining` 
AS 
SELECT * FROM `{project}.{datamart}.VOC_4_1_sstimecompany`;

"""

query_string2 = f"""
DROP VIEW `{project}.{datamart}.VOC_12_sscausalmining`; 
"""